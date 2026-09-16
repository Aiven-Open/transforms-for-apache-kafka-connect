/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.transforms.sandbox;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the guest transport directly, without going through a provider.
 *
 * <p>{@link StdioChannel} carries every record for the {@code sbx} runtime, and its failure modes
 * -- a guest that prints a start-up banner, answers twice, closes stderr, or stops answering --
 * belong to the transport rather than to any one sandbox. Driving it with a shell script keeps
 * these tests free of Docker, so they run everywhere.
 */
class StdioChannelTest {

    private static final String RESPONSE = "{\"value\":{\"ok\":true}}";

    @Test
    void aGuestThatAnswersTwiceNeverHasItsStaleLineServedToTheNextRecord(@TempDir final Path dir)
        throws IOException {
        // The dangerous case, and the reason the reader thread claims the outstanding request with
        // a compare-and-set rather than testing a flag and then queueing. A guest that emits two
        // lines per request has a spare line in flight; if it were delivered it would answer the
        // NEXT record -- valid JSON, plausibly correct content, wrong record. That is silent data
        // corruption, so the channel must refuse to continue instead.
        final String answersTwice = "while IFS= read -r line; do printf '%s\\n' '" + RESPONSE
            + "'; printf '%s\\n' '" + RESPONSE + "'; done\n";

        try (StdioChannel runtime =
                 new StdioChannel(guest(dir, "answers-twice", answersTwice), 5000)) {
            // The first call is answered legitimately by the first line.
            assertThat(runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8)))
                .asString().contains("ok");

            // The second line is unsolicited. Whether it is noticed before or after the next
            // request is written, the channel must end up abandoned rather than delivering it.
            awaitDead(runtime);
            assertThat(runtime.isAlive()).isFalse();
            assertThatThrownBy(() -> runtime.call("{\"v\":2}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class);
        }
    }

    private static List<String> guest(final Path dir, final String name, final String body)
        throws IOException {
        final Path script = dir.resolve(name + ".sh");
        Files.write(script, body.getBytes(StandardCharsets.UTF_8));
        return Arrays.asList("/bin/sh", script.toString());
    }

    private static final String SERVE =
        "while IFS= read -r line; do printf '%s\\n' '" + RESPONSE + "'; done\n";

    private static void awaitDead(final StdioChannel runtime) {
        final long deadline = System.currentTimeMillis() + 5000;
        while (runtime.isAlive() && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
    }

    @Test
    void answersOneLinePerRequest(@TempDir final Path dir) throws IOException {
        try (StdioChannel runtime =
                 new StdioChannel(guest(dir, "serve", SERVE), 5000)) {
            for (int i = 0; i < 100; i++) {
                assertThat(new String(runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8)),
                    StandardCharsets.UTF_8)).isEqualTo(RESPONSE);
            }
        }
    }

    @Test
    void abandonsTheChannelWhenTheGuestSpeaksUnprompted(@TempDir final Path dir) throws IOException {
        // A start-up banner is the classic case. Without request tracking the banner would be read
        // as the answer to record 1, record 1's real answer as record 2's, and so on -- every
        // record silently transformed by the wrong response.
        final String banner = "printf '%s\\n' 'listening on stdin'\n" + SERVE;
        try (StdioChannel runtime =
                 new StdioChannel(guest(dir, "banner", banner), 5000)) {
            awaitDead(runtime);
            assertThat(runtime.isAlive()).isFalse();
            assertThatThrownBy(() -> runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class);
        }
    }

    @Test
    void keepsServingAfterTheGuestClosesStderr(@TempDir final Path dir) throws IOException {
        // Closing stderr is ordinary behaviour for a process that has nothing to report. It must
        // not be mistaken for the conversation ending.
        final String quiet = "exec 2>&-\n" + SERVE;
        try (StdioChannel runtime =
                 new StdioChannel(guest(dir, "quiet", quiet), 5000)) {
            for (int i = 0; i < 20; i++) {
                assertThat(runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8))).isNotEmpty();
            }
            assertThat(runtime.isAlive()).isTrue();
        }
    }

    @Test
    void abandonsTheChannelOnTimeout(@TempDir final Path dir) throws IOException {
        final String silent = "while IFS= read -r line; do :; done\n";
        try (StdioChannel runtime =
                 new StdioChannel(guest(dir, "silent", silent), 250)) {
            assertThatThrownBy(() -> runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("did not answer");
            // A late reply must never be handed to a later record.
            assertThat(runtime.isAlive()).isFalse();
        }
    }

    @Test
    void rejectsAnEmptyCommand() {
        assertThatThrownBy(() -> new StdioChannel(java.util.Collections.emptyList(), 1000))
            .isInstanceOf(SandboxException.class)
            .hasMessageContaining("command is required");
    }
}
