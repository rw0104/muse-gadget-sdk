/* SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include "esp_codec_dev.h"
#include "esp_err.h"

/* ES8311 speaker and ES7210 microphones on one duplex I2S bus. */
esp_err_t lckfb_szpi_audio_init(esp_codec_dev_handle_t *spk, esp_codec_dev_handle_t *mic);
