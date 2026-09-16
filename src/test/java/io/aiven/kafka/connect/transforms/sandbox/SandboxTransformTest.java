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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SandboxTransformTest {

    private static SinkRecord record(final Object value) {
        return new SinkRecord("orders", 0, null, "k", null, value, 42L);
    }

    private static Map<String, Object> config(final Path module) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "wasm");
        configs.put("sandbox.module", module.toString());
        configs.put("schemas.enable", false);
        return configs;
    }

    @Test
    @SuppressWarnings("unchecked")
    void replacesTheValueWithWhatTheGuestReturned(@TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.returning(dir, "redact", "{\"value\":{\"card\":\"****\"}}");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));

            final Map<String, Object> input = new HashMap<>();
            input.put("card", "4111111111111111");
            final SinkRecord out = transform.apply(record(input));

            assertThat(out).isNotNull();
            assertThat((Map<String, Object>) out.value()).containsEntry("card", "****");
            assertThat(out.topic()).isEqualTo("orders");
            assertThat(out.key()).isEqualTo("k");
        }
    }

    @Test
    void aDropResponseFiltersTheRecordOut(@TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.returning(dir, "drop", "{\"drop\":true}");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));
            assertThat(transform.apply(record(new HashMap<>()))).isNull();
        }
    }

    @Test
    void anErrorResponseBecomesADataExceptionRatherThanFailingTheBoundary(@TempDir final Path dir)
        throws IOException {
        final Path module = WasmGuests.returning(dir, "bad", "{\"error\":\"unsupported schema\"}");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));
            assertThatThrownBy(() -> transform.apply(record(new HashMap<>())))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("unsupported schema");
        }
    }

    @Test
    void aGuestMayRerouteTheRecord(@TempDir final Path dir) throws IOException {
        final Path module =
            WasmGuests.returning(dir, "route", "{\"topic\":\"quarantine\",\"value\":{\"a\":1}}");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));
            assertThat(transform.apply(record(new HashMap<>())).topic()).isEqualTo("quarantine");
        }
    }

    @Test
    void tombstonesPassThroughUntouched(@TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.returning(dir, "any", "{\"value\":{\"a\":1}}");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));
            final SinkRecord tombstone = record(null);
            assertThat(transform.apply(tombstone)).isSameAs(tombstone);
        }
    }

    @Test
    void anUnknownRuntimeNameFailsWithTheAvailableOnesListed(@TempDir final Path dir)
        throws IOException {
        final Path module = WasmGuests.returning(dir, "any", "{\"value\":1}");
        final Map<String, Object> configs = config(module);
        configs.put("sandbox.provider", "firecracker");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            assertThatThrownBy(() -> transform.configure(configs))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("firecracker")
                .hasMessageContaining("wasm");
        }
    }

    @Test
    void aTrappingGuestFailsTheTaskRatherThanSilentlyPassingTheRecordThrough(
        @TempDir final Path dir) throws IOException {
        final Path module = WasmGuests.trapping(dir, "boom");
        try (SandboxTransform<SinkRecord> transform = new SandboxTransform<>()) {
            transform.configure(config(module));
            assertThatThrownBy(() -> transform.apply(record(new HashMap<>())))
                .isInstanceOf(SandboxException.class);
        }
    }
}
