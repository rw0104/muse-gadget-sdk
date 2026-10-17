/* SPDX-License-Identifier: Apache-2.0 */
/*
 * LCKFB SZPI ESP32-S3 / 立创·实战派: ESP32-S3-WROOM-1-N16R8,
 * 16 MB flash, 8 MB octal PSRAM, 2.0" ST7789 320x240 landscape LCD,
 * FT6336 touch, ES8311 speaker / ES7210 microphones, CH340K UART0 console.
 *
 * Pin map, SPI mode 2 / 80 MHz, RGB565, inversion and landscape orientation:
 * https://wiki.lckfb.com/zh-hans/szpi-esp32s3/beginner/lcd-display.html
 * It also specifies GPIO42 active-low backlight and selecting PCA9557 P0
 * after panel_reset and before panel_init. Reset is tied to board reset.
 * FT6336 is polled through the FT5x06 driver, with native 240x320 geometry,
 * mirror-X followed by swap-XY, matching the vendor's LVGL example:
 * https://wiki.lckfb.com/zh-hans/szpi-esp32s3/beginner/lvgl.html
 * Shared I2C SDA1/SCL2, PCA9557@0x19: P0 LCD_CS, P1 PA_EN, P2 CAM_PWDN:
 * https://wiki.lckfb.com/zh-hans/szpi-esp32s3/beginner/audio-output.html
 * Cross-check: 78/xiaozhi-esp32 commit e0d2390a1ca415473508bbc0c66eaf1d8dc04e37,
 * main/boards/lckfb/szpi-esp32s3/{config.h,lichuang_dev_board.cc}:
 * https://github.com/78/xiaozhi-esp32/tree/e0d2390a1ca415473508bbc0c66eaf1d8dc04e37/main/boards/lckfb/szpi-esp32s3
 *
 * No battery measurement or software power latch is verified. This USB-power
 * profile reports no battery; it never guesses an ADC or shutdown GPIO.
 * Camera is held powered down and is outside this board port's UI/audio scope.
 */
#include "driver/ledc.h"
#include "driver/spi_master.h"
#include "esp_check.h"
#include "esp_lcd_panel_io.h"
#include "esp_lcd_panel_ops.h"
#include "esp_lcd_panel_vendor.h"
#include "esp_lcd_touch_ft5x06.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_lv_adapter.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "lckfb_szpi_audio.h"
#include "lckfb_szpi_board.h"
#include "muse_board.h"
#include "muse_mem.h"

static const char *TAG = "board.szpi";

#define LCD_H_RES 320
#define LCD_V_RES 240
#define LCD_HOST SPI3_HOST
#define LCD_SCLK GPIO_NUM_41
#define LCD_MOSI GPIO_NUM_40
#define LCD_DC GPIO_NUM_39
#define LCD_BL GPIO_NUM_42
/* Two 16-line internal DMA buffers use 20 KB, leaving room for Wi-Fi/BLE. */
#define DRAW_BUF_LINES 16
#define PCA9557_ADDRESS 0x19
#define PCA9557_REG_OUTPUT 0x01
#define PCA9557_REG_CONFIG 0x03
#define PCA9557_LCD_CS (1u << 0)
#define PCA9557_PA_EN (1u << 1)
#define PCA9557_CAMERA_PWDN (1u << 2)
/* BOOT is a small tact switch that slips under a thumb mid-sentence. A press
 * this soon after a release is the same talk: the release is held back until
 * the window passes, so the note isn't cut short and restarted. */
#define TALK_REGRIP_MS 300

static i2c_master_bus_handle_t s_i2c;
static i2c_master_dev_handle_t s_expander;
static SemaphoreHandle_t s_expander_lock;
static uint8_t s_expander_outputs = PCA9557_LCD_CS | PCA9557_CAMERA_PWDN;
static esp_lcd_panel_handle_t s_panel;
static esp_lcd_touch_handle_t s_touch;
static muse_gpio_button_t s_talk;
static int64_t s_release_at;   /* a held-back release, or 0 */

i2c_master_bus_handle_t lckfb_szpi_i2c(void)
{
    return s_i2c;
}

static esp_err_t expander_write(uint8_t reg, uint8_t value)
{
    const uint8_t payload[] = { reg, value };
    return i2c_master_transmit(s_expander, payload, sizeof(payload), 100);
}

static esp_err_t expander_set(uint8_t mask, bool enabled)
{
    if (!s_expander || !s_expander_lock) {
        return ESP_ERR_INVALID_STATE;
    }
    if (xSemaphoreTake(s_expander_lock, pdMS_TO_TICKS(200)) != pdTRUE) {
        return ESP_ERR_TIMEOUT;
    }
    uint8_t next = enabled ? s_expander_outputs | mask : s_expander_outputs & ~mask;
    esp_err_t err = ESP_OK;
    if (next != s_expander_outputs) {
        err = expander_write(PCA9557_REG_OUTPUT, next);
        if (err == ESP_OK) {
            s_expander_outputs = next;
        }
    }
    xSemaphoreGive(s_expander_lock);
    return err;
}

esp_err_t lckfb_szpi_set_pa(bool enabled)
{
    return expander_set(PCA9557_PA_EN, enabled);
}

static esp_err_t init(void)
{
    /* Keep the active-low backlight dark until the UI has drawn its first frame. */
    gpio_set_level(LCD_BL, 1);
    const gpio_config_t bl_gpio = {
        .pin_bit_mask = 1ULL << LCD_BL,
        .mode = GPIO_MODE_OUTPUT,
    };
    ESP_RETURN_ON_ERROR(gpio_config(&bl_gpio), TAG, "backlight pin");

    const i2c_master_bus_config_t i2c_cfg = {
        .i2c_port = I2C_NUM_0,
        .sda_io_num = GPIO_NUM_1,
        .scl_io_num = GPIO_NUM_2,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .glitch_ignore_cnt = 7,
        .flags.enable_internal_pullup = true,
    };
    ESP_RETURN_ON_ERROR(i2c_new_master_bus(&i2c_cfg, &s_i2c), TAG, "shared I2C");
    s_expander_lock = xSemaphoreCreateMutex();
    ESP_RETURN_ON_FALSE(s_expander_lock, ESP_ERR_NO_MEM, TAG, "expander mutex");
    const i2c_device_config_t pca_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = PCA9557_ADDRESS,
        .scl_speed_hz = 400000,
    };
    ESP_RETURN_ON_ERROR(i2c_master_bus_add_device(s_i2c, &pca_cfg, &s_expander), TAG, "PCA9557 device");
    /* Set output latches before their direction: CS high, amp off, camera off.
     * Bits 3..7 remain inputs, as in the vendor example (configuration 0xF8). */
    ESP_RETURN_ON_ERROR(expander_write(PCA9557_REG_OUTPUT, s_expander_outputs), TAG, "PCA9557 outputs");
    ESP_RETURN_ON_ERROR(expander_write(PCA9557_REG_CONFIG, 0xf8), TAG, "PCA9557 directions");
    ESP_RETURN_ON_ERROR(muse_gpio_button_init(&s_talk, GPIO_NUM_0), TAG, "BOOT button");
    s_talk.pressed = gpio_get_level(GPIO_NUM_0) == 0;
    ESP_LOGI(TAG, "I2C SDA1/SCL2 ready; PCA9557 PA off, camera off; BOOT=GPIO0");
    return ESP_OK;
}

static esp_err_t panel_init(esp_lcd_panel_io_handle_t *io)
{
    const ledc_timer_config_t bl_timer = {
        .speed_mode = LEDC_LOW_SPEED_MODE,
        .duty_resolution = LEDC_TIMER_10_BIT,
        .timer_num = LEDC_TIMER_0,
        .freq_hz = 20000,
        .clk_cfg = LEDC_AUTO_CLK,
    };
    const ledc_channel_config_t bl_ch = {
        .gpio_num = LCD_BL,
        .speed_mode = LEDC_LOW_SPEED_MODE,
        .channel = LEDC_CHANNEL_0,
        .timer_sel = LEDC_TIMER_0,
        .duty = 0,
        .flags.output_invert = true,
    };
    ESP_RETURN_ON_ERROR(ledc_timer_config(&bl_timer), TAG, "backlight timer");
    ESP_RETURN_ON_ERROR(ledc_channel_config(&bl_ch), TAG, "active-low backlight");
    const spi_bus_config_t bus = {
        .sclk_io_num = LCD_SCLK,
        .mosi_io_num = LCD_MOSI,
        .miso_io_num = GPIO_NUM_NC,
        .quadwp_io_num = GPIO_NUM_NC,
        .quadhd_io_num = GPIO_NUM_NC,
        .max_transfer_sz = LCD_H_RES * DRAW_BUF_LINES * 2,
    };
    ESP_RETURN_ON_ERROR(spi_bus_initialize(LCD_HOST, &bus, SPI_DMA_CH_AUTO), TAG, "LCD SPI3");
    const esp_lcd_panel_io_spi_config_t io_cfg = {
        .cs_gpio_num = GPIO_NUM_NC, /* selected through PCA9557, not an S3 GPIO */
        .dc_gpio_num = LCD_DC,
        .spi_mode = 2,
        .pclk_hz = 80 * 1000 * 1000,
        .trans_queue_depth = 10,
        .lcd_cmd_bits = 8,
        .lcd_param_bits = 8,
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_io_spi(LCD_HOST, &io_cfg, io), TAG, "LCD SPI IO");
    const esp_lcd_panel_dev_config_t panel_cfg = {
        .reset_gpio_num = GPIO_NUM_NC,
        .rgb_ele_order = LCD_RGB_ELEMENT_ORDER_RGB,
        .bits_per_pixel = 16,
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_st7789(*io, &panel_cfg, &s_panel), TAG, "ST7789");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_reset(s_panel), TAG, "LCD reset");
    ESP_RETURN_ON_ERROR(expander_set(PCA9557_LCD_CS, false), TAG, "LCD select");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_init(s_panel), TAG, "LCD init");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_invert_color(s_panel, true), TAG, "LCD inversion");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_swap_xy(s_panel, true), TAG, "LCD landscape");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_mirror(s_panel, true, false), TAG, "LCD mirror");
    return esp_lcd_panel_disp_on_off(s_panel, true);
}

static esp_err_t touch_init(void)
{
    /* FT6336 reset is shared with the LCD/board reset. No dedicated reset or
     * interrupt GPIO is allocated by the official example; poll over I2C. */
    const esp_lcd_touch_config_t cfg = {
        .x_max = LCD_V_RES,
        .y_max = LCD_H_RES,
        .rst_gpio_num = GPIO_NUM_NC,
        .int_gpio_num = GPIO_NUM_NC,
        .levels = { .reset = 0, .interrupt = 0 },
        .flags = { .swap_xy = true, .mirror_x = true, .mirror_y = false },
    };
    esp_lcd_panel_io_i2c_config_t io_cfg = ESP_LCD_TOUCH_IO_I2C_FT5x06_CONFIG();
    io_cfg.scl_speed_hz = 400000;
    esp_lcd_panel_io_handle_t io = NULL;
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_io_i2c(s_i2c, &io_cfg, &io), TAG, "FT6336 IO");
    esp_err_t err = esp_lcd_touch_new_i2c_ft5x06(io, &cfg, &s_touch);
    if (err != ESP_OK) {
        esp_lcd_panel_io_del(io);
        ESP_LOGE(TAG, "FT6336 init: %s", esp_err_to_name(err));
    }
    return err;
}

static void touch_read(lv_indev_t *indev, lv_indev_data_t *data)
{
    (void)indev;
    data->state = LV_INDEV_STATE_RELEASED;
    if (esp_lcd_touch_read_data(s_touch) != ESP_OK) {
        return;
    }
    esp_lcd_touch_point_data_t point;
    uint8_t count = 0;
    if (esp_lcd_touch_get_data(s_touch, &point, &count, 1) == ESP_OK && count) {
        uint16_t x = point.x, y = point.y;
        /* The vendor uses max=240/320 rather than max=239/319. Clip the
         * mirrored edge into LVGL's valid coordinates without changing axes. */
        data->point.x = x < LCD_H_RES ? x : LCD_H_RES - 1;
        data->point.y = y < LCD_V_RES ? y : LCD_V_RES - 1;
        data->state = LV_INDEV_STATE_PRESSED;
    }
}

static lv_display_t *display_start(lv_indev_t **touch)
{
    *touch = NULL;
    esp_lcd_panel_io_handle_t io = NULL;
    if (panel_init(&io) != ESP_OK || touch_init() != ESP_OK) {
        return NULL;
    }
    esp_lv_adapter_config_t adapter_cfg = ESP_LV_ADAPTER_DEFAULT_CONFIG();
    adapter_cfg.task_core_id = MUSE_UI_CORE;
    adapter_cfg.task_priority = MUSE_UI_PRIORITY;
    if (esp_lv_adapter_init(&adapter_cfg) != ESP_OK) {
        return NULL;
    }
    const esp_lv_adapter_display_config_t disp_cfg = {
        .panel = s_panel,
        .panel_io = io,
        .profile = {
            .interface = ESP_LV_ADAPTER_PANEL_IF_OTHER,
            .rotation = ESP_LV_ADAPTER_ROTATE_0,
            .hor_res = LCD_H_RES,
            .ver_res = LCD_V_RES,
            .buffer_height = DRAW_BUF_LINES,
            .use_psram = false,
            .require_double_buffer = true,
        },
        .tear_avoid_mode = ESP_LV_ADAPTER_TEAR_AVOID_MODE_NONE,
    };
    lv_display_t *disp = esp_lv_adapter_register_display(&disp_cfg);
    if (!disp) {
        return NULL;
    }
    lv_indev_t *indev = lv_indev_create();
    if (!indev) {
        return NULL;
    }
    lv_indev_set_type(indev, LV_INDEV_TYPE_POINTER);
    lv_indev_set_display(indev, disp);
    lv_indev_set_read_cb(indev, touch_read);
    if (esp_lv_adapter_start() != ESP_OK) {
        return NULL;
    }
    *touch = indev;
    ESP_LOGI(TAG, "ST7789 320x240 + FT6336 ready; backlight GPIO42 active-low");
    return disp;
}

static bool display_lock(int timeout_ms)
{
    return esp_lv_adapter_lock(timeout_ms) == ESP_OK;
}

static void set_brightness(int pct)
{
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    ledc_set_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, pct * 1023 / 100);
    ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);
}

static void panel_sleep(bool sleep)
{
    esp_lcd_panel_disp_sleep(s_panel, sleep);
}

static unsigned poll_buttons(void)
{
    unsigned ev = muse_gpio_button_poll(&s_talk);
    int64_t now = esp_timer_get_time();
    if (ev & MUSE_BTN_TALK_RELEASE) {
        ev &= ~MUSE_BTN_TALK_RELEASE;
        s_release_at = now;
    } else if ((ev & MUSE_BTN_TALK_PRESS) && s_release_at) {
        ev &= ~MUSE_BTN_TALK_PRESS;   /* regripped: still talking */
        s_release_at = 0;
    }
    if (s_release_at && now - s_release_at >= TALK_REGRIP_MS * 1000) {
        ev |= MUSE_BTN_TALK_RELEASE;
        s_release_at = 0;
    }
    return ev;
}

static void wait_buttons(int timeout_ms)
{
    if (s_release_at) {
        /* Wake in time to send the held-back release. */
        int left = TALK_REGRIP_MS - (int)((esp_timer_get_time() - s_release_at) / 1000);
        timeout_ms = left < 0 ? 0 : (timeout_ms >= 0 && timeout_ms < left ? timeout_ms : left);
    }
    muse_gpio_buttons_wait((muse_gpio_button_t *const[]){ &s_talk }, 1, timeout_ms);
}

static esp_err_t read_power(muse_power_t *out)
{
    /* This profile is for the stock USB-powered board, not a measured VBUS
     * signal. There is no verified battery sensor or charger-status input. */
    *out = (muse_power_t){ .battery_pct = -1, .battery_mv = 0, .charging = false, .usb = true };
    return ESP_OK;
}

static esp_err_t power_off(void)
{
    return ESP_ERR_NOT_SUPPORTED;
}

static const muse_board_t s_board = {
    .name = "LCKFB SZPI ESP32-S3",
    .width = LCD_H_RES,
    .height = LCD_V_RES,
    .round = false,
    .no_ring = true,
    .touch = true,
    .diagonal_in = 2.0f,
    .talk_button = "BOOT",
    .talk_hint = { LV_ALIGN_TOP_LEFT, 12, 24 },
    .frame_ms = 40,
    .init = init,
    .display_start = display_start,
    .display_lock = display_lock,
    .display_unlock = esp_lv_adapter_unlock,
    .set_brightness = set_brightness,
    .panel_sleep = panel_sleep,
    .audio_init = lckfb_szpi_audio_init,
    .mic_slot = 0, /* ES7210 MIC1, not the MIC3 playback reference */
    .poll_buttons = poll_buttons,
    .wait_buttons = wait_buttons,
    .read_power = read_power,
    .power_off = power_off,
};

const muse_board_t *muse_board_get(void)
{
    return &s_board;
}
