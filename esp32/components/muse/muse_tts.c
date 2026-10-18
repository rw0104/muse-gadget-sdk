/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Qwen TTS: qwen3-tts-flash-realtime over DashScope's realtime WebSocket.
 * https://help.aliyun.com/en/model-studio/qwen-tts-realtime-client-events
 *
 * The session opens as a turn starts (muse_tts_prepare), so its TLS and
 * WebSocket handshakes are done before the reply text arrives. A reply goes
 * in segments of a sentence or more, each input_text_buffer.append + commit:
 * the server takes at most 2000 characters a commit, CJK counting double.
 * Each is answered with response.audio.delta events (base64 16-bit mono PCM
 * at the speaker's rate, as asked for in session.update) and response.done,
 * which sends the next. Where each segment's speech starts gives captions
 * their place in the text (muse_tts_text_at). The session closes with the turn.
 */
#include "muse_tts.h"
#include "muse_mem.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
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
/* Speaking rate in percent, sent as session.update's speech_rate (the server
 * takes 0.5 to 2.0). ">tts_rate=" with no value goes back to this. */
#define RATE_DEFAULT 100
#define RATE_MIN 50
#define RATE_MAX 200
#define TEXT_MAX 4096
#define APPEND_MAX 1024                    /* text per input_text_buffer.append */
#define SEG_MIN 60                         /* a segment ends at the first sentence end past this weight */
#define SEG_MAX 1200                       /* and is cut anyway here (the server's limit is 2000) */
#define SEGS_MAX 64
#define FRAMES_PER_WEIGHT (MUSE_TTS_RATE / 9)  /* speech per weight unit at rate 100, until a segment's been timed */
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
static uint16_t s_rate = RATE_DEFAULT;
static bool s_loaded;
static char *s_text;            /* the reply being spoken */
static size_t s_sent;           /* how much of it has gone to the server */
static bool s_awaiting;         /* a segment is out, its response.done not yet back */
static char *s_piece;           /* one append's worth of s_text */
/* Where each segment starts, in s_text and in the speech. */
static struct {
    uint32_t off, frame, weight;
} s_seg[SEGS_MAX];
static int s_nseg;
static uint32_t s_pushed;       /* frames of speech received for this reply */
static portMUX_TYPE s_seg_mux = portMUX_INITIALIZER_UNLOCKED;
static atomic_bool s_retried;
static char *s_frame;
static size_t s_frame_len;
static uint8_t *s_audio;
static StreamBufferHandle_t s_pcm;
static TaskHandle_t s_task;
static SemaphoreHandle_t s_lock;
static SemaphoreHandle_t s_ws_lock;     /* s_ws, against its teardown while sending */
static esp_websocket_client_handle_t s_ws;
static atomic_int s_state = ST_IDLE;
static atomic_int s_conn = CONN_OFF;
static atomic_bool s_cancel;
static atomic_bool s_open_failed;  /* the last open couldn't even start */
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
    if (nvs_get_u16(h, "rate", &s_rate) != ESP_OK || s_rate < RATE_MIN || s_rate > RATE_MAX) {
        s_rate = RATE_DEFAULT;
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
    xSemaphoreTake(s_ws_lock, portMAX_DELAY);
    bool sent = s_ws && esp_websocket_client_send_text(s_ws, json, strlen(json), pdMS_TO_TICKS(2000)) >= 0;
    xSemaphoreGive(s_ws_lock);
    if (!sent) {
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
    cJSON_AddNumberToObject(session, "speech_rate", s_rate / 100.0);
    send_event(event);
}

/* Length of the longest prefix of s, at most max bytes, that doesn't end
 * partway through a UTF-8 character: the server drops the connection over a
 * text frame that isn't valid UTF-8. */
static size_t utf8_cut(const char *s, size_t max)
{
    size_t n = strnlen(s, max);
    if (s[n] == '\0') {
        return n;
    }
    while (n && ((unsigned char)s[n] & 0xC0) == 0x80) {
        n--;
    }
    return n;
}

/* What a character costs against the server's limit: CJK counts double. */
static uint32_t char_weight(unsigned char lead)
{
    return lead >= 0xE0 ? 2 : 1;
}

static bool sentence_end(const char *p)
{
    static const char *const ends[] = { "\xE3\x80\x82", "\xEF\xBC\x81", "\xEF\xBC\x9F", "\xEF\xBC\x9B", "\xE2\x80\xA6" };
    if (*p == '\n' || ((*p == '.' || *p == '!' || *p == '?' || *p == ';') && (p[1] == ' ' || p[1] == '\n' || !p[1]))) {
        return true;
    }
    for (size_t i = 0; i < sizeof(ends) / sizeof(ends[0]); i++) {
        if (!strncmp(p, ends[i], strlen(ends[i]))) {
            return true;
        }
    }
    return false;
}

/* Bytes of p in the next segment, and its weight. */
static size_t segment_len(const char *p, uint32_t *weight)
{
    size_t n = 0, soft = 0;
    uint32_t w = 0, soft_w = 0;
    while (p[n]) {
        unsigned char c = (unsigned char)p[n];
        size_t len = c < 0x80 ? 1 : c < 0xE0 ? 2 : c < 0xF0 ? 3 : 4;
        if (w + char_weight(c) > SEG_MAX) {
            break;
        }
        bool end = sentence_end(p + n);
        w += char_weight(c);
        n += len;
        while (p[n] && ((unsigned char)p[n] & 0xC0) == 0x80) {
            n++;        /* a stray continuation byte stays with its character */
        }
        if (end || c == ',' || c == ' ' || !strncmp(p + n - len, "\xEF\xBC\x8C", 3)) {
            soft = n;   /* somewhere to cut a sentence that runs past SEG_MAX */
            soft_w = w;
        }
        if (end && w >= SEG_MIN) {
            *weight = w;
            return n;
        }
    }
    if (p[n] && soft) {
        *weight = soft_w;
        return soft;
    }
    *weight = w;
    return n;
}

/* Sends the reply's next segment, if it's due: in a few appends and one
 * commit. Called with s_lock held. */
static void send_next_locked(void)
{
    if (s_awaiting || !s_text[s_sent] || atomic_load(&s_conn) != CONN_READY ||
        atomic_load(&s_state) != ST_RUNNING || s_nseg >= SEGS_MAX) {
        return;
    }
    uint32_t weight;
    size_t len = segment_len(s_text + s_sent, &weight);
    taskENTER_CRITICAL(&s_seg_mux);
    s_seg[s_nseg].off = s_sent;
    s_seg[s_nseg].frame = s_pushed;
    s_seg[s_nseg].weight = weight;
    s_nseg++;
    taskEXIT_CRITICAL(&s_seg_mux);
    const char *p = s_text + s_sent, *end = p + len;
    while (p < end) {
        size_t n = utf8_cut(p, end - p < APPEND_MAX ? end - p : APPEND_MAX);
        if (!n) {
            break;
        }
        memcpy(s_piece, p, n);
        s_piece[n] = '\0';
        p += n;
        cJSON *append = event_of("input_text_buffer.append");
        cJSON_AddStringToObject(append, "text", s_piece);
        send_event(append);
    }
    s_sent += len;
    s_awaiting = true;
    send_event(event_of("input_text_buffer.commit"));
    if (!atomic_load(&s_audio_us)) {
        atomic_store(&s_commit_us, esp_timer_get_time());
    }
}

/* ---- Receiving (the WebSocket client's task) ---- */

static void push_pcm(const uint8_t *data, size_t len)
{
    len &= ~(size_t)1;
    while (len && !atomic_load(&s_cancel) && atomic_load(&s_state) == ST_RUNNING) {
        size_t n = xStreamBufferSend(s_pcm, data, len, pdMS_TO_TICKS(100));
        /* Waiting on a full buffer isn't the server going quiet. */
        atomic_store(&s_audio_us, esp_timer_get_time());
        taskENTER_CRITICAL(&s_seg_mux);
        s_pushed += n / 2;
        taskEXIT_CRITICAL(&s_seg_mux);
        data += n;
        len -= n;
    }
}

/* An audio delta, without parsing all of it as JSON: its base64 is most of
 * the event and has nothing to unescape. */
static bool on_audio_delta(char *json)
{
    if (!strstr(json, "\"response.audio.delta\"")) {
        return false;
    }
    char *b64 = strstr(json, "\"delta\":\"");
    char *end = b64 ? strchr(b64 + 9, '"') : NULL;
    size_t olen = 0;
    if (end && !atomic_load(&s_cancel) && atomic_load(&s_state) == ST_RUNNING &&
        mbedtls_base64_decode(s_audio, AUDIO_MAX, &olen, (const unsigned char *)b64 + 9, end - b64 - 9) == 0) {
        atomic_store(&s_audio_us, esp_timer_get_time());
        push_pcm(s_audio, olen);
    }
    return true;
}

static void on_event(char *json)
{
    if (on_audio_delta(json)) {
        return;
    }
    cJSON *root = cJSON_Parse(json);
    if (!root) {
        return;
    }
    const char *type = cJSON_GetStringValue(cJSON_GetObjectItem(root, "type"));
    if (!type) {
        type = "";
    }
    if (!strcmp(type, "response.done")) {
        xSemaphoreTake(s_lock, portMAX_DELAY);
        s_awaiting = false;
        if (s_text[s_sent] && s_nseg < SEGS_MAX) {
            send_next_locked();
        } else {
            int running = ST_RUNNING;
            atomic_compare_exchange_strong(&s_state, &running, ST_DONE);
        }
        xSemaphoreGive(s_lock);
    } else if (!strcmp(type, "session.created")) {
        send_session_update();
    } else if (!strcmp(type, "session.updated")) {
        atomic_store(&s_conn, CONN_READY);
        ESP_LOGI(TAG, "Qwen TTS session ready");
        xSemaphoreTake(s_lock, portMAX_DELAY);
        send_next_locked();
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
        break;      /* DISCONNECTED follows */
    case WEBSOCKET_EVENT_DISCONNECTED:
    case WEBSOCKET_EVENT_CLOSED: {
        /* With auto-reconnect off, the client's task ends after either of
         * these, so it frees the client itself: close_ws from another task
         * would race that exit (seen as a double free in esp-tls). */
        xSemaphoreTake(s_ws_lock, portMAX_DELAY);
        if (d && d->client == s_ws) {
            esp_websocket_client_destroy_on_exit(s_ws);
            s_ws = NULL;
        }
        xSemaphoreGive(s_ws_lock);
        atomic_store(&s_conn, CONN_OFF);
        if (atomic_load(&s_state) == ST_RUNNING && !atomic_load(&s_audio_us) && !atomic_exchange(&s_retried, true)) {
            /* Dropped before a word was heard: a new session, the same text. */
            ESP_LOGW(TAG, "Qwen TTS session dropped; retrying once");
            xSemaphoreTake(s_lock, portMAX_DELAY);
            s_sent = 0;
            s_awaiting = false;
            taskENTER_CRITICAL(&s_seg_mux);
            s_nseg = 0;
            taskEXIT_CRITICAL(&s_seg_mux);
            xSemaphoreGive(s_lock);
            atomic_store(&s_commit_us, esp_timer_get_time());
            xTaskNotify(s_task, OP_OPEN, eSetBits);
            break;
        }
        int running = ST_RUNNING;
        atomic_compare_exchange_strong(&s_state, &running, ST_FAILED);
        break;
    }
    default:
        break;
    }
}

/* ---- Opening and closing (this module's own task, so callers never block) ---- */

/*
 * esp_websocket_client starts its task with no core affinity, so it can run on
 * the audio core. TLS on a reply's audio events then holds that core for up to
 * 90 ms at a time, at Wi-Fi's priority when Wi-Fi waits on a lock the task
 * holds, and the speaker runs dry: heard as stutter. The link wraps task
 * creation (CMakeLists.txt) so that this one task goes on the other core.
 */
BaseType_t __real_xTaskCreatePinnedToCore(TaskFunction_t fn, const char *const name, const uint32_t stack, void *const arg,
                                          UBaseType_t prio, TaskHandle_t *const out, const BaseType_t core);
BaseType_t __wrap_xTaskCreatePinnedToCore(TaskFunction_t fn, const char *const name, const uint32_t stack, void *const arg,
                                          UBaseType_t prio, TaskHandle_t *const out, BaseType_t core)
{
    if (core == tskNO_AFFINITY && MUSE_AUDIO_CORE != 0 && name && !strcmp(name, "muse_tts_ws")) {
        core = 0;
    }
    return __real_xTaskCreatePinnedToCore(fn, name, stack, arg, prio, out, core);
}

static void close_ws(void)
{
    xSemaphoreTake(s_ws_lock, portMAX_DELAY);
    esp_websocket_client_handle_t ws = s_ws;
    s_ws = NULL;
    xSemaphoreGive(s_ws_lock);
    if (ws) {
        if (esp_websocket_client_close(ws, pdMS_TO_TICKS(1000)) == ESP_OK) {
            esp_websocket_client_destroy(ws);
        } else {
            esp_websocket_client_destroy_on_exit(ws);   /* its task is already ending on its own */
        }
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
        /* Over CONFIG_SPIRAM_MALLOC_ALWAYSINTERNAL, so its two buffers come
         * from PSRAM: internal RAM is short with the display and audio up. */
        .buffer_size = 8192,
        .task_stack = 5120,
        .task_name = "muse_tts_ws",
        /* Below the UI and chat tasks: a reply's audio arrives in a burst that
         * takes TLS, JSON and base64 work, which must not starve playback. */
        .task_prio = 4,
        .disable_auto_reconnect = true,
        .network_timeout_ms = 10000,
    };
    atomic_store(&s_open_failed, false);
    esp_websocket_client_handle_t ws = esp_websocket_client_init(&cfg);
    memset(headers, 0, sizeof(headers));
    if (!ws) {
        ESP_LOGE(TAG, "no memory for the Qwen TTS connection");
        atomic_store(&s_open_failed, true);
        return;
    }
    esp_websocket_register_events(ws, WEBSOCKET_EVENT_ANY, ws_handler, NULL);
    atomic_store(&s_conn, CONN_OPENING);
    xSemaphoreTake(s_ws_lock, portMAX_DELAY);
    s_ws = ws;
    xSemaphoreGive(s_ws_lock);
    if (esp_websocket_client_start(ws) != ESP_OK) {
        ESP_LOGE(TAG, "couldn't start the Qwen TTS connection (%u bytes internal RAM free)",
                 (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
        xSemaphoreTake(s_ws_lock, portMAX_DELAY);
        s_ws = NULL;
        xSemaphoreGive(s_ws_lock);
        esp_websocket_client_destroy(ws);     /* no task to race */
        atomic_store(&s_conn, CONN_OFF);
        atomic_store(&s_open_failed, true);
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
    s_piece = heap_caps_malloc(APPEND_MAX + 1, MALLOC_CAP_SPIRAM);
    s_frame = heap_caps_malloc(FRAME_MAX, MALLOC_CAP_SPIRAM);
    s_audio = heap_caps_malloc(AUDIO_MAX, MALLOC_CAP_SPIRAM);
    s_pcm = xStreamBufferCreateWithCaps(PCM_BUF, 2, MALLOC_CAP_SPIRAM);
    s_lock = xSemaphoreCreateMutex();
    s_ws_lock = xSemaphoreCreateMutex();
    if (!s_text || !s_piece || !s_frame || !s_audio || !s_pcm || !s_lock || !s_ws_lock ||
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
    if (!muse_tts_ready() || !start_task()) {
        return;
    }
    if (atomic_load(&s_conn) == CONN_OFF) {
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
    muse_tts_clean(s_text);
    s_sent = 0;
    s_awaiting = false;
    taskENTER_CRITICAL(&s_seg_mux);
    s_nseg = 0;
    s_pushed = 0;
    taskEXIT_CRITICAL(&s_seg_mux);
    atomic_store(&s_retried, false);
    xStreamBufferReset(s_pcm);
    atomic_store(&s_cancel, false);
    atomic_store(&s_audio_us, 0);
    atomic_store(&s_commit_us, esp_timer_get_time());   /* the first-audio clock starts now */
    atomic_store(&s_state, ST_RUNNING);
    send_next_locked();
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
    if (atomic_load(&s_state) == ST_RUNNING && atomic_load(&s_open_failed) && atomic_load(&s_conn) == CONN_OFF) {
        atomic_store(&s_state, ST_FAILED);   /* no session to speak with: text at once */
    }
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

size_t muse_tts_text_at(uint32_t frame)
{
    size_t at = 0;
    taskENTER_CRITICAL(&s_seg_mux);
    int i = s_nseg - 1;
    while (i > 0 && s_seg[i].frame > frame) {
        i--;
    }
    if (i >= 0) {
        bool last = i == s_nseg - 1;
        uint32_t start = s_seg[i].frame;
        uint32_t len = (last ? s_sent : s_seg[i + 1].off) - s_seg[i].off;
        /* A finished segment's length is known; the one being received is
         * guessed from the ones before it, or the default rate. */
        uint32_t frames;
        if (!last || atomic_load(&s_state) != ST_RUNNING) {
            frames = (last ? s_pushed : s_seg[i + 1].frame) - start;
        } else {
            uint32_t w = 0;
            for (int j = 0; j < i; j++) {
                w += s_seg[j].weight;
            }
            frames = w && start ? (uint32_t)((uint64_t)start * s_seg[i].weight / w)
                                : s_seg[i].weight * FRAMES_PER_WEIGHT * 100 / s_rate;
            frames = frames > s_pushed - start ? frames : s_pushed - start;
        }
        at = s_seg[i].off + (frames ? (size_t)((uint64_t)(frame - start) * len / frames) : 0);
        size_t end = s_seg[i].off + len;
        at = at < end ? at : (end ? end - 1 : 0);
    }
    taskEXIT_CRITICAL(&s_seg_mux);
    return at;
}

/* Markdown and the like out of the way of the voice: it would read out the
 * asterisks of **bold** and the hashes of a heading. Emoji go too. Cuts in
 * place; only ever shortens. */
void muse_tts_clean(char *text)
{
    char *out = text;
    bool line_start = true;
    for (const char *p = text; *p;) {
        unsigned char c = (unsigned char)*p;
        if (line_start) {
            const char *q = p;
            while (*q == ' ' || *q == '\t') {
                q++;
            }
            while (*q == '#' || *q == '>') {
                q++;
            }
            if ((*q == '-' || *q == '*' || *q == '+') && q[1] == ' ') {
                q++;    /* a list item's bullet */
            }
            while (*q == ' ' || *q == '\t') {
                q++;
            }
            p = q;
            line_start = false;
            continue;
        }
        if (c == '*' || c == '`' || c == '~' || c == '|' || (c == '_' && (p[1] == '_' || p == text || p[-1] == ' '))) {
            p++;
            continue;
        }
        if (c == ']' && p[1] == '(') {
            const char *close = strchr(p, ')');
            p = close ? close + 1 : p + 1;    /* [text](link): just the text */
            continue;
        }
        if (c == '[') {
            p++;
            continue;
        }
        if (!strncmp(p, "http://", 7) || !strncmp(p, "https://", 8)) {
            while (*p && *p != ' ' && *p != '\n' && (unsigned char)*p < 0x80) {
                p++;
            }
            continue;
        }
        if (c >= 0xF0) {
            p += 4;     /* emoji and other astral characters */
            while (*p && ((unsigned char)*p & 0xC0) == 0x80) {
                p++;
            }
            continue;
        }
        if (!strncmp(p, "\xEF\xB8\x8F", 3) || !strncmp(p, "\xE2\x80\x8D", 3)) {
            p += 3;     /* emoji variation selector, zero-width joiner */
            continue;
        }
        if (c == '\n') {
            line_start = true;
            while (out > text && out[-1] == ' ') {
                out--;
            }
            if (out > text && out[-1] == '\n') {
                p++;
                continue;   /* blank lines */
            }
        }
        if (c == ' ' && (out == text || out[-1] == ' ' || out[-1] == '\n')) {
            p++;
            continue;
        }
        *out++ = *p++;
    }
    *out = '\0';
}

size_t muse_tts_buffered(void)
{
    return s_pcm ? xStreamBufferBytesAvailable(s_pcm) / sizeof(int16_t) : 0;
}

bool muse_tts_fetched(void)
{
    return atomic_load(&s_state) != ST_RUNNING;
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

/* Stores the rate, or forgets it when it's the default. */
static bool save_rate(uint16_t rate)
{
    nvs_handle_t h;
    if (nvs_open(NVS_NS, NVS_READWRITE, &h) != ESP_OK) {
        return false;
    }
    esp_err_t err = rate != RATE_DEFAULT ? nvs_set_u16(h, "rate", rate) : nvs_erase_key(h, "rate");
    if (err == ESP_ERR_NVS_NOT_FOUND) {
        err = ESP_OK;
    }
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    return err == ESP_OK;
}

/* "1.1" or "110" to percent; 0 if it's neither or out of range. Empty is the default. */
static uint16_t parse_rate(const char *s)
{
    if (!s[0]) {
        return RATE_DEFAULT;
    }
    char *end;
    double v = strtod(s, &end);
    if (*end || v != v) {
        return 0;
    }
    if (v <= RATE_MAX / 100.0) {
        v *= 100;
    }
    return v >= RATE_MIN - 0.5 && v < RATE_MAX + 0.5 ? (uint16_t)(v + 0.5) : 0;
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
    if (!strncmp(line, "tts_rate=", 9)) {
        uint16_t rate = parse_rate(line + 9);
        if (!rate) {
            answer("tts_rate", "error invalid");
        } else if (!save_rate(rate)) {
            answer("tts_rate", "error storage");
        } else {
            s_rate = rate;
            muse_tts_close();      /* the next session speaks at it */
            ESP_LOGI(TAG, "speech rate %u.%02u", rate / 100, rate % 100);
            answer("tts_rate", "ok");
        }
        return true;
    }
    return false;
}
