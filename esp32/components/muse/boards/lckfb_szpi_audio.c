/* SPDX-License-Identifier: Apache-2.0 */
/*
 * LCKFB SZPI ESP32-S3 audio: ES8311 DAC at 0x18 and ES7210 ADC at 0x41
 * (AD0 high) on the shared SDA1/SCL2 bus, I2S MCLK38 BCLK14 WS13 DOUT45 DIN12.
 * The speaker amp has no S3 GPIO; it is PCA9557 P1. The ES8311 driver
 * switches it through s_pa_gpio as the speaker opens, closes and mutes.
 * Pins and addresses: 78/xiaozhi-esp32 main/boards/lckfb/szpi-esp32s3/config.h
 * and https://wiki.lckfb.com/zh-hans/szpi-esp32s3/beginner/audio-input.html
 *
 * Standard-mode stereo capture: slot 0 is physical MIC1. The vendor's MIC3
 * playback reference (for echo cancellation) is not used by Muse.
 */
#include "driver/i2s_std.h"
#include "esp_check.h"
#include "esp_codec_dev_defaults.h"
#include "esp_log.h"

#include "lckfb_szpi_audio.h"
#include "lckfb_szpi_board.h"
#include "muse_audio.h"

static const char *TAG = "szpi.audio";

#define ES8311_ADDR_7BIT 0x18
#define ES7210_ADDR_7BIT 0x41
#define PA_PIN 1 /* PCA9557 P1; only s_pa_gpio reads it */

static int pa_gpio_setup(int16_t gpio, audio_gpio_dir_t dir, audio_gpio_mode_t mode)
{
    (void)gpio;
    (void)dir;
    (void)mode;
    return ESP_CODEC_DEV_OK;
}

static int pa_gpio_set(int16_t gpio, bool high)
{
    (void)gpio;
    return lckfb_szpi_set_pa(high) == ESP_OK ? ESP_CODEC_DEV_OK : ESP_CODEC_DEV_WRITE_FAIL;
}

static bool pa_gpio_get(int16_t gpio)
{
    (void)gpio;
    return false;
}

static const audio_codec_gpio_if_t s_pa_gpio = {
    .setup = pa_gpio_setup,
    .set = pa_gpio_set,
    .get = pa_gpio_get,
};

esp_err_t lckfb_szpi_audio_init(esp_codec_dev_handle_t *spk, esp_codec_dev_handle_t *mic)
{
    i2c_master_bus_handle_t i2c = lckfb_szpi_i2c();
    ESP_RETURN_ON_FALSE(i2c, ESP_ERR_INVALID_STATE, TAG, "shared I2C not ready");
    ESP_RETURN_ON_ERROR(i2c_master_probe(i2c, ES8311_ADDR_7BIT, 100), TAG, "ES8311 probe");
    ESP_RETURN_ON_ERROR(i2c_master_probe(i2c, ES7210_ADDR_7BIT, 100), TAG, "ES7210 probe");

    i2s_chan_handle_t tx, rx;
    i2s_chan_config_t chan = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_0, I2S_ROLE_MASTER);
    chan.auto_clear = true;
    /* 6 x 360 frames: 90 ms at 24 kHz, what the default 6 x 240 gives at
     * 16 kHz. DMA buffers are internal RAM, which Wi-Fi and TLS need too. */
    chan.dma_desc_num = 6;
    chan.dma_frame_num = 360;
    ESP_RETURN_ON_ERROR(i2s_new_channel(&chan, &tx, &rx), TAG, "I2S channels");
    const i2s_std_config_t std = {
        .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(MUSE_AUDIO_RATE),
        .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_STEREO),
        .gpio_cfg = {
            .mclk = GPIO_NUM_38,
            .bclk = GPIO_NUM_14,
            .ws = GPIO_NUM_13,
            .dout = GPIO_NUM_45,
            .din = GPIO_NUM_12,
        },
    };
    ESP_RETURN_ON_ERROR(i2s_channel_init_std_mode(tx, &std), TAG, "I2S TX");
    ESP_RETURN_ON_ERROR(i2s_channel_init_std_mode(rx, &std), TAG, "I2S RX");
    ESP_RETURN_ON_ERROR(i2s_channel_enable(tx), TAG, "I2S TX enable");
    ESP_RETURN_ON_ERROR(i2s_channel_enable(rx), TAG, "I2S RX enable");

    audio_codec_i2s_cfg_t data_cfg = { .port = I2S_NUM_0, .rx_handle = rx, .tx_handle = tx };
    const audio_codec_data_if_t *data = audio_codec_new_i2s_data(&data_cfg);
    /* esp_codec_dev takes 8-bit (shifted) addresses. */
    audio_codec_i2c_cfg_t ctrl_cfg = { .port = I2C_NUM_0, .addr = ES8311_ADDR_7BIT << 1, .bus_handle = i2c };
    const audio_codec_ctrl_if_t *dac_ctrl = audio_codec_new_i2c_ctrl(&ctrl_cfg);
    ctrl_cfg.addr = ES7210_ADDR_7BIT << 1;
    const audio_codec_ctrl_if_t *adc_ctrl = audio_codec_new_i2c_ctrl(&ctrl_cfg);
    ESP_RETURN_ON_FALSE(data && dac_ctrl && adc_ctrl, ESP_ERR_NO_MEM, TAG, "codec interfaces");

    es8311_codec_cfg_t dac = {
        .ctrl_if = dac_ctrl,
        .gpio_if = &s_pa_gpio,
        .codec_mode = ESP_CODEC_DEV_WORK_MODE_DAC,
        .pa_pin = PA_PIN,
        .use_mclk = true,
        .hw_gain = { .pa_voltage = 5.0, .codec_dac_voltage = 3.3 },
    };
    es7210_codec_cfg_t adc = {
        .ctrl_if = adc_ctrl,
        .mic_selected = ES7210_SEL_MIC1 | ES7210_SEL_MIC2,
    };
    const audio_codec_if_t *out = es8311_codec_new(&dac);
    const audio_codec_if_t *in = es7210_codec_new(&adc);
    ESP_RETURN_ON_FALSE(out && in, ESP_FAIL, TAG, "codec initialization");

    esp_codec_dev_cfg_t out_cfg = { .dev_type = ESP_CODEC_DEV_TYPE_OUT, .codec_if = out, .data_if = data };
    esp_codec_dev_cfg_t in_cfg = { .dev_type = ESP_CODEC_DEV_TYPE_IN, .codec_if = in, .data_if = data };
    *spk = esp_codec_dev_new(&out_cfg);
    *mic = esp_codec_dev_new(&in_cfg);
    ESP_RETURN_ON_FALSE(*spk && *mic, ESP_FAIL, TAG, "codec handles");
    ESP_LOGI(TAG, "ES8311/ES7210 ready at %d Hz", MUSE_AUDIO_RATE);
    return ESP_OK;
}
