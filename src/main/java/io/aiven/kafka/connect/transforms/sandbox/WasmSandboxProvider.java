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

import java.util.Map;

/** Registers the {@code wasm} runtime, which is the default. */
public class WasmSandboxProvider implements SandboxProvider {

    @Override
    public String name() {
        return "wasm";
    }

    @Override
    public SandboxCapabilities capabilities() {
        // Memory is capped and no host imports are supplied, so the guest has no syscalls at all.
        // CPU is the gap: a guest looping forever holds the task thread, and a Java thread cannot
        // be safely killed. 0.08us is the measured empty-guest round trip.
        return new SandboxCapabilities(
            SandboxCapabilities.Isolation.IN_PROCESS, true, false, true);
    }

    @Override
    public Sandbox create(final Map<String, ?> configs) {
        final SandboxTransformConfig config = new SandboxTransformConfig(configs);
        return new WasmSandbox(config.module(), config.maxMemoryPages(), config.compile());
    }
}
