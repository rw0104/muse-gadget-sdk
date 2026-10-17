/* SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <stdbool.h>
#include "driver/i2c_master.h"
#include "esp_err.h"

/* Owned by the board's init(); display, touch and both codecs share this bus.
 * Callers must not delete or reconfigure it. */
i2c_master_bus_handle_t lckfb_szpi_i2c(void);

/* PCA9557 P1 is the active-high speaker amplifier enable. Thread-safe and
 * preserves LCD CS and camera PWDN. Only called from task context. */
esp_err_t lckfb_szpi_set_pa(bool enabled);
