/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Qwen TTS: qwen3-tts-flash-realtime over DashScope's realtime WebSocket.
 * https://help.aliyun.com/en/model-studio/qwen-tts-realtime-client-events
 *
 * The session opens as a turn starts (muse_tts_prepare), so its TLS and
 * WebSocket handshakes are done before the reply text arrives. Then each
 * reply is input_text_buffer.append + commit; the server answers with
 * response.audio.delta events (base64 16 kHz 16-bit mono PCM, as asked for in
 * session.update) and response.done. The session closes with the turn.
 */
#include "muse_tts.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#include "cJSON.h"
#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "esp_log.h"
#include "esp_random.h"
#include "esp_timer.h"
#include "esp_websocket_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/idf_additions.h"
#include "freertos/semphr.h"
#include "freertos/stream_buffer.h"
#include "freertos/task.h"
#include "mbedtls/base64.h"
#include "nvs.h"

static const char *TAG = "muse_tts";

#define NVS_NS "muse_tts"
#define MODEL "qwen3-tts-flash-realtime"
#define KEY_MAX 255
#define VOICE_MAX 31
#define TEXT_MAX 1024
#define PCM_BUF (MUSE_TTS_RATE * 2 * 10)   /* 10 s of speech ahead of the speaker */
#define FRAME_MAX (64 * 1024)              /* one server event: deltas are ~13 KB of base64 */
#define AUDIO_MAX (FRAME_MAX * 3 / 4)
#define FIRST_AUDIO_US (8 * 1000000)       /* no speech this long after a commit: give up */
#define STALL_US (8 * 1000000)             /* or this long between pieces of it */

#define OP_OPEN (1u << 0)
#define OP_CLOSE (1u << 1)

enum { ST_IDLE, ST_RUNNING, ST_DONE, ST_FAILED };            /* the reply being spoken */
enum { CONN_OFF, CONN_OPENING, CONN_READY };                  /* the session */

static char s_key[KEY_MAX + 1];
static char s_voice[VOICE_MAX + 1] = "Cherry";
static bool s_loaded;
static char *s_text;            /* waiting for the session to be ready, or "" */
static char *s_frame;
static size_t s_frame_len;
static uint8_t *s_audio;
static StreamBufferHandle_t s_pcm;
static TaskHandle_t s_task;
static SemaphoreHandle_t s_lock;
static esp_websocket_client_handle_t s_ws;
static atomic_int s_state = ST_IDLE;
static atomic_int s_conn = CONN_OFF;
static atomic_bool s_cancel;
static atomic_llong s_commit_us, s_audio_us;

static void load(void)
{
    if (s_loaded) {
        return;
    }
    s_loaded = true;
    nvs_handle_t h;
    if (nvs_open(NVS_NS, NVS_READONLY, &h) != ESP_OK) {
        return;
    }
    size_t n = sizeof(s_key);
    if (nvs_get_str(h, "key", s_key, &n) != ESP_OK) {
        s_key[0] = '\0';
    }
    n = sizeof(s_voice);
    if (nvs_get_str(h, "voice", s_voice, &n) != ESP_OK || !s_voice[0]) {
        strlcpy(s_voice, "Cherry", sizeof(s_voice));
    }
    nvs_close(h);
}

bool muse_tts_ready(void)
{
    load();
    return s_key[0] != '\0';
}

/* ---- Sending events (any task) ---- */

static void send_event(cJSON *event)
{
    char id[24];
    snprintf(id, sizeof(id), "e%08lx%08lx", (unsigned long)esp_random(), (unsigned long)esp_random());
    cJSON_AddStringToObject(event, "event_id", id);
    char *json = cJSON_PrintUnformatted(event);
    cJSON_Delete(event);
    if (!json) {
        return;
    }
    esp_websocket_client_handle_t ws = s_ws;
    if (!ws || esp_websocket_client_send_text(ws, json, strlen(json), pdMS_TO_TICKS(2000)) < 0) {
        ESP_LOGW(TAG, "couldn't send an event");
        int running = ST_RUNNING;
        atomic_compare_exchange_strong(&s_state, &running, ST_FAILED);
    }
    free(json);
}

static cJSON *event_of(const char *type)
{
    cJSON *event = cJSON_CreateObject();
    cJSON_AddStringToObject(event, "type", type);
    return event;
}

static void send_session_update(void)
{
    cJSON *event = event_of("session.update");
    cJSON *session = cJSON_AddObjectToObject(event, "session");
    cJSON_AddStringToObject(session, "voice", s_voice);
    cJSON_AddStringToObject(session, "mode", "commit");
    cJSON_AddStringToObject(session, "response_format", "pcm");
    cJSON_AddNumberToObject(session, "sample_rate", MUSE_TTS_RATE);
    cJSON_AddStringToObject(session, "language_type", "Auto");
    send_event(event);
}

/* Sends the waiting text, if any. Called with s_lock held. */
static void send_text_locked(void)
{
    if (!s_text[0] || atomic_load(&s_conn) != CONN_READY) {
        return;
    }
    cJSON *append = event_of("input_text_buffer.append");
    cJSON_AddStringToObject(append, "text", s_text);
    s_text[0] = '\0';
    send_event(append);
    send_event(event_of("input_text_buffer.commit"));
    atomic_store(&s_commit_us, esp_timer_get_time());
}

/* ---- Receiving (the WebSocket client's task) ---- */

static void push_pcm(const uint8_t *data, size_t len)
{
    len &= ~(size_t)1;
    while (len && !atomic_load(&s_cancel) && atomic_load(&s_state) == ST_RUNNING) {
        size_t n = xStreamBufferSend(s_pcm, data, len, pdMS_TO_TICKS(100));
        data += n;
        len -= n;
    }
}

static void on_event(const char *json)
{
    cJSON *root = cJSON_Parse(json);
    if (!root) {
        return;
    }
    const char *type = cJSON_GetStringValue(cJSON_GetObjectItem(root, "type"));
    if (!type) {
        type = "";
    }
    if (!strcmp(type, "response.audio.delta")) {
        const char *b64 = cJSON_GetStringValue(cJSON_GetObjectItem(root, "delta"));
        size_t olen = 0;
        if (b64 && !atomic_load(&s_cancel) && atomic_load(&s_state) == ST_RUNNING &&
            mbedtls_base64_decode(s_audio, AUDIO_MAX, &olen, (const unsigned char *)b64, strlen(b64)) == 0) {
            atomic_store(&s_audio_us, esp_timer_get_time());
            push_pcm(s_audio, olen);
        }
    } else if (!strcmp(type, "response.done")) {
        int running = ST_RUNNING;
        atomic_compare_exchange_strong(&s_state, &running, ST_DONE);
    } else if (!strcmp(type, "session.created")) {
        send_session_update();
    } else if (!strcmp(type, "session.updated")) {
        atomic_store(&s_conn, CONN_READY);
        ESP_LOGI(TAG, "Qwen TTS session ready");
        xSemaphoreTake(s_lock, portMAX_DELAY);
        send_text_locked();
        xSemaphoreGive(s_lock);
    } else if (!strcmp(type, "error")) {
        cJSON *error = cJSON_GetObjectItem(root, "error");
        const char *code = cJSON_GetStringValue(cJSON_GetObjectItem(error, "code"));
        const char *msg = cJSON_GetStringValue(cJSON_GetObjectItem(error, "message"));
        ESP_LOGE(TAG, "Qwen TTS error %s: %s", code ? code : "?", msg ? msg : "");
        int running = ST_RUNNING;
        atomic_compare_exchange_strong(&s_state, &running, ST_FAILED);
    }
    cJSON_Delete(root);
}

static void ws_handler(void *arg, esp_event_base_t base, int32_t id, void *event_data)
{
    (void)arg;
    (void)base;
    esp_websocket_event_data_t *d = event_data;
    switch (id) {
    case WEBSOCKET_EVENT_DATA:
        if (d->op_code != 0x1 && d->op_code != 0x0) {
            break;      /* pings, pongs, close frames */
        }
        if (d->payload_offset == 0) {
            s_frame_len = 0;
        }
        if (d->payload_offset + d->data_len < FRAME_MAX) {
            memcpy(s_frame + d->payload_offset, d->data_ptr, d->data_len);
            s_frame_len = d->payload_offset + d->data_len;
        }
        if (d->payload_offset + d->data_len >= d->payload_len && s_frame_len) {
            s_frame[s_frame_len] = '\0';
            on_event(s_frame);
            s_frame_len = 0;
        }
        break;
    case WEBSOCKET_EVENT_ERROR:
        ESP_LOGW(TAG, "Qwen TTS connection error (HTTP %d)", d ? d->error_handle.esp_ws_handshake_status_code : 0);
        /* fall through */
    case WEBSOCKET_EVENT_DISCONNECTED:
    case WEBSOCKET_EVENT_CLOSED: {
        atomic_store(&s_conn, CONN_OFF);
        int running = ST_RUNNING;
        atomic_compare_exchange_strong(&s_state, &running, ST_FAILED);
        break;
    }
    default:
        break;
    }
}

/* ---- Opening and closing (this module's own task, so callers never block) ---- */

static void close_ws(void)
{
    if (s_ws) {
        esp_websocket_client_close(s_ws, pdMS_TO_TICKS(1000));
        esp_websocket_client_destroy(s_ws);
        s_ws = NULL;
    }
    atomic_store(&s_conn, CONN_OFF);
}

static void open_ws(void)
{
    if (s_ws && atomic_load(&s_conn) != CONN_OFF) {
        return;
    }
    close_ws();
    char headers[KEY_MAX + 32];
    snprintf(headers, sizeof(headers), "Authorization: Bearer %s\r\n", s_key);
    const esp_websocket_client_config_t cfg = {
        .uri = "wss://" CONFIG_MUSE_TTS_QWEN_HOST "/api-ws/v1/realtime?model=" MODEL,
        .headers = headers,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = 4096,
        .task_stack = 6144,
        .task_name = "muse_tts_ws",
        .disable_auto_reconnect = true,
        .network_timeout_ms = 10000,
    };
    s_ws = esp_websocket_client_init(&cfg);
    memset(headers, 0, sizeof(headers));
    if (!s_ws) {
        ESP_LOGE(TAG, "no memory for the Qwen TTS connection");
        return;
    }
    esp_websocket_register_events(s_ws, WEBSOCKET_EVENT_ANY, ws_handler, NULL);
    atomic_store(&s_conn, CONN_OPENING);
    if (esp_websocket_client_start(s_ws) != ESP_OK) {
        ESP_LOGE(TAG, "couldn't start the Qwen TTS connection");
        close_ws();
    }
}

static void tts_task(void *arg)
{
    (void)arg;
    for (;;) {
        uint32_t ops = 0;
        xTaskNotifyWait(0, UINT32_MAX, &ops, portMAX_DELAY);
        if (ops & OP_CLOSE) {
            close_ws();
        }
        if (ops & OP_OPEN) {
            open_ws();
        }
    }
}

static bool start_task(void)
{
    if (s_task) {
        return true;
    }
    s_text = heap_caps_calloc(1, TEXT_MAX, MALLOC_CAP_SPIRAM);
    s_frame = heap_caps_malloc(FRAME_MAX, MALLOC_CAP_SPIRAM);
    s_audio = heap_caps_malloc(AUDIO_MAX, MALLOC_CAP_SPIRAM);
    s_pcm = xStreamBufferCreateWithCaps(PCM_BUF, 2, MALLOC_CAP_SPIRAM);
    s_lock = xSemaphoreCreateMutex();
    if (!s_text || !s_frame || !s_audio || !s_pcm || !s_lock ||
        xTaskCreatePinnedToCoreWithCaps(tts_task, "muse_tts", 4096, NULL, 4, &s_task, tskNO_AFFINITY,
                                        MALLOC_CAP_SPIRAM) != pdPASS) {
        ESP_LOGE(TAG, "no memory for TTS");
        s_task = NULL;
        return false;
    }
    return true;
}

/* ---- The chat task's side ---- */

void muse_tts_prepare(void)
{
    /* Always asked for: a close queued by the last turn runs first (tts_task
     * handles OP_CLOSE before OP_OPEN), and opening an open session is a no-op. */
    if (muse_tts_ready() && start_task()) {
        xTaskNotify(s_task, OP_OPEN, eSetBits);
    }
}

void muse_tts_close(void)
{
    if (s_task) {
        xTaskNotify(s_task, OP_CLOSE, eSetBits);
    }
}

bool muse_tts_speak(const char *text)
{
    if (!muse_tts_ready() || !text || !text[0] || atomic_load(&s_state) == ST_RUNNING || !start_task()) {
        return false;
    }
    xSemaphoreTake(s_lock, portMAX_DELAY);
    strlcpy(s_text, text, TEXT_MAX);
    xStreamBufferReset(s_pcm);
    atomic_store(&s_cancel, false);
    atomic_store(&s_audio_us, 0);
    atomic_store(&s_commit_us, esp_timer_get_time());   /* the first-audio clock starts now */
    atomic_store(&s_state, ST_RUNNING);
    send_text_locked();
    xSemaphoreGive(s_lock);
    if (atomic_load(&s_conn) == CONN_OFF) {
        xTaskNotify(s_task, OP_OPEN, eSetBits);     /* sent once the session is ready */
    }
    return true;
}

size_t muse_tts_read(int16_t *pcm, size_t max)
{
    if (!s_pcm || atomic_load(&s_cancel)) {
        return 0;
    }
    return xStreamBufferReceive(s_pcm, pcm, max * sizeof(int16_t), 0) / sizeof(int16_t);
}

bool muse_tts_finished(void)
{
    if (atomic_load(&s_state) == ST_RUNNING) {
        int64_t now = esp_timer_get_time(), heard = atomic_load(&s_audio_us);
        bool late = heard ? now - heard > STALL_US : now - atomic_load(&s_commit_us) > FIRST_AUDIO_US;
        if (!late) {
            return false;
        }
        ESP_LOGW(TAG, "Qwen TTS went quiet; showing the text instead");
        atomic_store(&s_state, ST_FAILED);
        muse_tts_close();
    }
    return !s_pcm || xStreamBufferIsEmpty(s_pcm);
}

bool muse_tts_failed(void)
{
    return atomic_load(&s_state) == ST_FAILED;
}

void muse_tts_cancel(void)
{
    atomic_store(&s_cancel, true);
    int running = ST_RUNNING;
    atomic_compare_exchange_strong(&s_state, &running, ST_IDLE);
}

/* ---- Serial setup ---- */

static void answer(const char *what, const char *result)
{
    printf("@%s %s\n", what, result);
    fflush(stdout);
}

static bool save(const char *name, const char *value)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NS, NVS_READWRITE, &h) != ESP_OK) {
        return false;
    }
    esp_err_t err = value[0] ? nvs_set_str(h, name, value) : nvs_erase_key(h, name);
    if (err == ESP_ERR_NVS_NOT_FOUND) {
        err = ESP_OK;
    }
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    return err == ESP_OK;
}

static bool valid(const char *s, size_t max)
{
    size_t n = strlen(s);
    if (n > max) {
        return false;
    }
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' ||
              c == '.')) {
            return false;
        }
    }
    return true;
}

bool muse_tts_console_command(const char *line)
{
    load();
    if (!strncmp(line, "tts_key=", 8)) {
        const char *key = line + 8;
        if (key[0] && (strncmp(key, "sk-", 3) || !valid(key, KEY_MAX))) {
            answer("tts_key", "error invalid");
        } else if (!save("key", key)) {
            answer("tts_key", "error storage");
        } else {
            strlcpy(s_key, key, sizeof(s_key));
            muse_tts_close();      /* the next session uses the new key */
            if (key[0]) {
                ESP_LOGI(TAG, "Qwen TTS key stored (%.7s...)", key);
            } else {
                ESP_LOGI(TAG, "Qwen TTS key removed");
            }
            answer("tts_key", key[0] ? "ok" : "cleared");
        }
        return true;
    }
    if (!strncmp(line, "tts_voice=", 10)) {
        const char *voice = line + 10;
        if (!valid(voice, VOICE_MAX)) {
            answer("tts_voice", "error invalid");
        } else if (!save("voice", voice)) {
            answer("tts_voice", "error storage");
        } else {
            strlcpy(s_voice, voice[0] ? voice : "Cherry", sizeof(s_voice));
            muse_tts_close();
            ESP_LOGI(TAG, "voice %s", s_voice);
            answer("tts_voice", "ok");
        }
        return true;
    }
    return false;
}
