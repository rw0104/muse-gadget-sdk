/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Qwen TTS: qwen3-tts-flash over DashScope's HTTP API, streamed as SSE.
 * Each "data:" event carries output.audio.data, base64 24 kHz 16-bit mono
 * PCM; the first one starts with a 44-byte WAV header. An event with "code"
 * is an error. https://help.aliyun.com/zh/model-studio/qwen-tts-api
 */
#include "muse_tts.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#include "cJSON.h"
#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "esp_http_client.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/idf_additions.h"
#include "freertos/stream_buffer.h"
#include "freertos/task.h"
#include "mbedtls/base64.h"
#include "nvs.h"

static const char *TAG = "muse_tts";

#define NVS_NS "muse_tts"
#define KEY_MAX 255
#define VOICE_MAX 31
#define TEXT_MAX 1024
#define PCM_BUF (MUSE_TTS_RATE * 2 * 6)   /* 6 s of speech ahead of the speaker */
#define SSE_LINE_MAX (48 * 1024)              /* one SSE event: ~20 KB of base64 */
#define AUDIO_MAX (SSE_LINE_MAX * 3 / 4)
#define READ_CHUNK 2048
#define WAV_HEADER 44

enum { ST_IDLE, ST_RUNNING, ST_DONE, ST_FAILED };

static char s_key[KEY_MAX + 1];
static char s_voice[VOICE_MAX + 1] = "Cherry";
static bool s_loaded;
static char *s_text;
static char *s_line;
static uint8_t *s_audio;
static StreamBufferHandle_t s_pcm;
static TaskHandle_t s_task;
static atomic_int s_state = ST_IDLE;
static atomic_bool s_cancel;

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
    if (nvs_get_str(h, "voice", s_voice, &n) != ESP_OK) {
        strlcpy(s_voice, "Cherry", sizeof(s_voice));
    }
    nvs_close(h);
}

bool muse_tts_ready(void)
{
    load();
    return s_key[0] != '\0';
}

/* Queues PCM for the speaker, waiting while the buffer is full. */
static bool push_pcm(const uint8_t *data, size_t len)
{
    len &= ~(size_t)1;
    while (len && !atomic_load(&s_cancel)) {
        size_t n = xStreamBufferSend(s_pcm, data, len, pdMS_TO_TICKS(100));
        data += n;
        len -= n;
    }
    return !atomic_load(&s_cancel);
}

/* One SSE line. False to stop: an error event, or cancelled. */
static bool on_line(char *line, bool *first)
{
    if (strncmp(line, "data:", 5) != 0) {
        return true;
    }
    cJSON *root = cJSON_Parse(line + 5);
    if (!root) {
        ESP_LOGW(TAG, "unreadable event (%u bytes)", (unsigned)strlen(line));
        return true;
    }
    bool ok = true;
    const char *code = cJSON_GetStringValue(cJSON_GetObjectItem(root, "code"));
    if (code && code[0]) {
        const char *msg = cJSON_GetStringValue(cJSON_GetObjectItem(root, "message"));
        ESP_LOGE(TAG, "Qwen TTS error %s: %s", code, msg ? msg : "");
        ok = false;
    } else {
        cJSON *audio = cJSON_GetObjectItem(cJSON_GetObjectItem(root, "output"), "audio");
        const char *b64 = cJSON_GetStringValue(cJSON_GetObjectItem(audio, "data"));
        size_t olen = 0;
        if (b64 && b64[0] &&
            mbedtls_base64_decode(s_audio, AUDIO_MAX, &olen, (const unsigned char *)b64, strlen(b64)) == 0) {
            const uint8_t *pcm = s_audio;
            if (*first && olen >= WAV_HEADER && !memcmp(pcm, "RIFF", 4)) {
                pcm += WAV_HEADER;
                olen -= WAV_HEADER;
            }
            *first = false;
            ok = push_pcm(pcm, olen);
        }
    }
    cJSON_Delete(root);
    return ok;
}

static char *request_body(void)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *input = cJSON_AddObjectToObject(root, "input");
    cJSON_AddStringToObject(root, "model", "qwen3-tts-flash");
    cJSON_AddStringToObject(input, "text", s_text);
    cJSON_AddStringToObject(input, "voice", s_voice);
    cJSON_AddStringToObject(input, "language_type", "Auto");
    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return body;
}

static bool fetch(void)
{
    char *body = request_body();
    if (!body) {
        return false;
    }
    char auth[KEY_MAX + 8];
    snprintf(auth, sizeof(auth), "Bearer %s", s_key);
    const esp_http_client_config_t cfg = {
        .url = "https://" CONFIG_MUSE_TTS_QWEN_HOST "/api/v1/services/aigc/multimodal-generation/generation",
        .method = HTTP_METHOD_POST,
        .timeout_ms = 15000,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = READ_CHUNK,
    };
    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    bool ok = false;
    if (!client) {
        free(body);
        return false;
    }
    esp_http_client_set_header(client, "Authorization", auth);
    esp_http_client_set_header(client, "Content-Type", "application/json");
    esp_http_client_set_header(client, "X-DashScope-SSE", "enable");
    memset(auth, 0, sizeof(auth));

    int len = strlen(body);
    esp_err_t err = esp_http_client_open(client, len);
    if (err == ESP_OK && esp_http_client_write(client, body, len) == len) {
        esp_http_client_fetch_headers(client);
        int status = esp_http_client_get_status_code(client);
        size_t line_len = 0;
        bool first = true, going = true;
        char buf[256];
        ok = status == 200;
        while (going && !atomic_load(&s_cancel)) {
            int n = esp_http_client_read(client, buf, sizeof(buf));
            if (n <= 0) {
                break;
            }
            for (int k = 0; k < n && going; k++) {
                char c = buf[k];
                if (c == '\n' || c == '\r') {
                    if (line_len) {
                        s_line[line_len] = '\0';
                        if (status != 200) {
                            ESP_LOGE(TAG, "HTTP %d: %.200s", status, s_line);
                        } else {
                            going = on_line(s_line, &first);
                            ok = going;
                        }
                    }
                    line_len = 0;
                } else if (line_len < SSE_LINE_MAX - 1) {
                    s_line[line_len++] = c;
                }
            }
        }
        if (status != 200 && line_len) {
            s_line[line_len] = '\0';
            ESP_LOGE(TAG, "HTTP %d: %.200s", status, s_line);
        }
    } else {
        ESP_LOGE(TAG, "request failed: %s", esp_err_to_name(err));
    }
    esp_http_client_cleanup(client);
    free(body);
    return ok && !atomic_load(&s_cancel);
}

static void tts_task(void *arg)
{
    (void)arg;
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        int64_t t0 = esp_timer_get_time();
        bool ok = fetch();
        ESP_LOGI(TAG, "%s in %.1f s", ok ? "spoke" : "stopped", (esp_timer_get_time() - t0) / 1e6);
        atomic_store(&s_state, ok ? ST_DONE : ST_FAILED);
    }
}

static bool start_task(void)
{
    if (s_task) {
        return true;
    }
    s_text = heap_caps_malloc(TEXT_MAX, MALLOC_CAP_SPIRAM);
    s_line = heap_caps_malloc(SSE_LINE_MAX, MALLOC_CAP_SPIRAM);
    s_audio = heap_caps_malloc(AUDIO_MAX, MALLOC_CAP_SPIRAM);
    s_pcm = xStreamBufferCreateWithCaps(PCM_BUF, 2, MALLOC_CAP_SPIRAM);
    if (!s_text || !s_line || !s_audio || !s_pcm ||
        xTaskCreatePinnedToCoreWithCaps(tts_task, "muse_tts", 8192, NULL, 4, &s_task, tskNO_AFFINITY,
                                        MALLOC_CAP_SPIRAM) != pdPASS) {
        ESP_LOGE(TAG, "no memory for TTS");
        s_task = NULL;
        return false;
    }
    return true;
}

bool muse_tts_speak(const char *text)
{
    if (!muse_tts_ready() || !text || !text[0] || atomic_load(&s_state) == ST_RUNNING || !start_task()) {
        return false;
    }
    strlcpy(s_text, text, TEXT_MAX);
    xStreamBufferReset(s_pcm);
    atomic_store(&s_cancel, false);
    atomic_store(&s_state, ST_RUNNING);
    xTaskNotifyGive(s_task);
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
    int st = atomic_load(&s_state);
    return st != ST_RUNNING && (!s_pcm || xStreamBufferIsEmpty(s_pcm));
}

bool muse_tts_failed(void)
{
    return atomic_load(&s_state) == ST_FAILED;
}

void muse_tts_cancel(void)
{
    atomic_store(&s_cancel, true);
}

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
            ESP_LOGI(TAG, "voice %s", s_voice);
            answer("tts_voice", "ok");
        }
        return true;
    }
    return false;
}
