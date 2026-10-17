/* SPDX-License-Identifier: Apache-2.0 */
#pragma once

/*
 * Speaking replies with Qwen TTS (CONFIG_MUSE_TTS_QWEN).
 *
 * qwen3-tts-flash-realtime over a WebSocket session opened as the turn starts
 * (muse_tts_prepare) and closed with it (muse_tts_close). One reply at a time:
 * muse_tts_speak() sends the text, and its mono PCM collects in a
 * buffer the chat task drains with muse_tts_read(). The DashScope key lives
 * in NVS, set over serial; without one, muse_tts_ready() is false and replies
 * stay text.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "sdkconfig.h"

/* The speaker's own rate, so nothing is resampled. */
#define MUSE_TTS_RATE CONFIG_MUSE_AUDIO_RATE

/* A key is stored. */
bool muse_tts_ready(void);

/* Opens the session ahead of the reply, so speaking starts sooner, and
 * replaces it before the server drops it for idling: call it while waiting.
 * No-op without a key or with a fresh one open. Never blocks. */
void muse_tts_prepare(void);

/* Closes the session, in the background. */
void muse_tts_close(void);

/* Starts speaking `text` (copied, then cleaned as muse_tts_clean does). False
 * if no key, or still busy with the last one. */
bool muse_tts_speak(const char *text);

/* Takes markdown, links and emoji out of `text`, in place, as the voice
 * would otherwise read them out. Speech positions are in the cleaned text. */
void muse_tts_clean(char *text);

/* Where in the (cleaned) text the speech is, `frame` frames into it. */
size_t muse_tts_text_at(uint32_t frame);

/* Up to `max` frames of speech that have arrived; 0 if none yet. */
size_t muse_tts_read(int16_t *pcm, size_t max);

/* Frames waiting to be read. */
size_t muse_tts_buffered(void);

/* The fetch has ended (more frames may still be waiting). */
bool muse_tts_fetched(void);

/* The fetch has ended and every frame has been read. */
bool muse_tts_finished(void);

/* It ended in an error (check once finished). */
bool muse_tts_failed(void);

/* Stops the fetch and drops what's buffered. */
void muse_tts_cancel(void);

/* Console lines "tts_key=...", "tts_voice=..." and "tts_rate=..." (1.1, or empty
 * for the default), without their '>'. False if the line is none of them.
 * Answers "@tts_key ok|cleared|error <why>". */
bool muse_tts_console_command(const char *line);

#ifdef __cplusplus
}
#endif
