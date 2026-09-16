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
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WasmSandboxTest {

    @Test
    void returnsTheGuestResponseAcrossTheAbi(@TempDir final Path dir) throws IOException {
        final String expected = "{\"value\":{\"redacted\":true}}";
        final Path module = WasmGuests.returning(dir, "redact", expected);

        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 64)) {
            final byte[] out = runtime.call("{\"topic\":\"t\"}".getBytes(StandardCharsets.UTF_8));
            assertThat(new String(out, StandardCharsets.UTF_8)).isEqualTo(expected);
            assertThat(runtime.isAlive()).isTrue();
        }
    }

    @Test
    void survivesManyCallsOnOneInstance(@TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.returning(dir, "ok", "{\"value\":1}");
        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 64)) {
            for (int i = 0; i < 10_000; i++) {
                assertThat(runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8))).isNotEmpty();
            }
        }
    }

    @Test
    void aGuestWithoutResetExhaustsItsMemoryAndIsStoppedAtTheLimit(@TempDir final Path dir)
        throws IOException {
        // Documents why the ABI has a reset export: a bump-allocating guest that never frees
        // grows until it hits its page limit. The limit holding is the isolation working.
        final Path module = WasmGuests.leaking(dir, "leaky", "{\"value\":1}");
        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 1)) {
            assertThatThrownBy(() -> {
                for (int i = 0; i < 100_000; i++) {
                    runtime.call("{\"v\":1}".getBytes(StandardCharsets.UTF_8));
                }
            }).isInstanceOf(SandboxException.class).hasMessageContaining("out of bounds");
        }
    }

    @Test
    void aTrappingGuestFailsTheCallButNotTheWorker(@TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.trapping(dir, "boom");
        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 64)) {
            assertThatThrownBy(() -> runtime.call("{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("trapped");
        }
    }

    @Test
    void rejectsAMissingModule() {
        assertThatThrownBy(() -> new WasmSandbox("/nope/absent.wasm", 64))
            .isInstanceOf(SandboxException.class)
            .hasMessageContaining("No wasm module");
    }

    @Test
    void rejectsAnEmptyModulePath() {
        assertThatThrownBy(() -> new WasmSandbox("", 64))
            .isInstanceOf(SandboxException.class)
            .hasMessageContaining("sandbox.module");
    }

    @Test
    void anOutOfRangeResponseIsRejectedRatherThanSizingAHostBuffer(@TempDir final Path dir)
        throws IOException {
        // A length close to Integer.MAX_VALUE would make the host allocate ~2GB on the guest's
        // say-so. The failure would be an OutOfMemoryError, which is an Error and not a
        // ChicoryException, so it would bypass the trap handling and SandboxException alike and
        // damage more than the task that caused it. Validate the pair against live memory first.
        final Path module =
            WasmGuests.returningOutOfRange(dir, "huge", 0, Integer.MAX_VALUE - 8);

        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 64)) {
            assertThatThrownBy(() -> runtime.call("{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("out-of-range");
        }
    }

    @Test
    void aPointerPastTheEndOfGuestMemoryIsRejected(@TempDir final Path dir) throws IOException {
        // One page is 64KiB; point just past it with a plausible length.
        final Path module = WasmGuests.returningOutOfRange(dir, "past-end", 65_536, 16);

        try (WasmSandbox runtime = new WasmSandbox(module.toString(), 64)) {
            assertThatThrownBy(() -> runtime.call("{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("out-of-range");
        }
    }
}
