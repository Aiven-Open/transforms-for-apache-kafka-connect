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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The abstraction is the point of this project, so it gets a test of its own.
 *
 * <p>Providers are found by {@link ServiceLoader}, which means a third party adds a boundary by
 * dropping a JAR next to this one -- no change here, no rebuild.
 */
class SandboxProviderDiscoveryTest {

    private static Map<String, SandboxProvider> discovered() {
        final Map<String, SandboxProvider> byName = new HashMap<>();
        for (final SandboxProvider provider
                 : ServiceLoader.load(SandboxProvider.class, SandboxProvider.class.getClassLoader())) {
            byName.put(provider.name(), provider);
        }
        return byName;
    }

    @Test
    void shipsWasmAndSbxAsTheTwoProviders() {
        assertThat(discovered()).containsKeys("wasm", "sbx");
    }

    @Test
    void theTwoProvidersDifferInKindNotJustInSpeed() {
        final Map<String, SandboxProvider> providers = discovered();
        final SandboxCapabilities wasm = providers.get("wasm").capabilities();
        final SandboxCapabilities sbx = providers.get("sbx").capabilities();

        assertThat(wasm.isolation()).isEqualTo(SandboxCapabilities.Isolation.IN_PROCESS);
        assertThat(sbx.isolation()).isEqualTo(SandboxCapabilities.Isolation.VIRTUAL_MACHINE);

        // Both are real boundaries: memory is bounded and the guest has no host syscalls.
        assertThat(wasm.boundsMemory()).isTrue();
        assertThat(wasm.blocksSyscalls()).isTrue();
        assertThat(sbx.boundsMemory()).isTrue();
        assertThat(sbx.blocksSyscalls()).isTrue();

        // They differ where it matters. Only a sandbox with its own scheduler can stop a guest
        // that loops forever; in-process WebAssembly cannot, because a Java thread cannot be
        // safely killed.
        assertThat(wasm.boundsCpu()).isFalse();
        assertThat(sbx.boundsCpu()).isTrue();
    }

    @Test
    void anUnknownProviderNameIsRejectedWithTheRealOnesListed() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("sandbox.provider", "firecracker");
        try (SandboxTransform<SinkRecord> transform =
                 new SandboxTransform<>()) {
            org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> transform.configure(configs))
                .isInstanceOf(SandboxException.class)
                .hasMessageContaining("wasm")
                .hasMessageContaining("sbx");
        }
    }
}
