/* SPDX-License-Identifier: Apache-2.0 */
#pragma once

/*
 * Speaking replies with Qwen TTS (CONFIG_MUSE_TTS_QWEN).
 *
 * One reply at a time: muse_tts_speak() hands the text to a background task,
 * which streams qwen3-tts-flash's 24 kHz mono PCM into a buffer that the chat
 * task drains with muse_tts_read(). The DashScope key lives in NVS, set over
 * serial; without one, muse_tts_ready() is false and replies stay text.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MUSE_TTS_RATE 24000

/* A key is stored. */
bool muse_tts_ready(void);

/* Starts speaking `text` (copied). False if no key, or still busy with the last one. */
bool muse_tts_speak(const char *text);

/* Up to `max` frames of speech that have arrived; 0 if none yet. */
size_t muse_tts_read(int16_t *pcm, size_t max);

/* The fetch has ended and every frame has been read. */
bool muse_tts_finished(void);

/* It ended in an error (check once finished). */
bool muse_tts_failed(void);

/* Stops the fetch and drops what's buffered. */
void muse_tts_cancel(void);

/* Console lines "tts_key=..." and "tts_voice=...", without their '>'. False if
 * the line is neither. Answers "@tts_key ok|cleared|error <why>". */
bool muse_tts_console_command(const char *line);

#ifdef __cplusplus
}
#endif
