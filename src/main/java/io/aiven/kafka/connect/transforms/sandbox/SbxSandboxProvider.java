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

/**
 * Runs the guest in a microVM with its own kernel, through Docker Sandboxes.
 *
 * <p>This is the provider that covers what the WebAssembly one cannot: a guest that loops forever
 * is bounded by the virtual machine's own CPU allocation instead of holding the Connect task
 * thread. It costs one to two orders of magnitude more per record, which is the trade being made.
 *
 * <p>It talks to a microVM-backed Docker engine. Docker Sandboxes exposes one at
 * {@code ~/.sbx/run/d/docker.sock}, which is the default when {@code sandbox.sbx.host} is unset.
 */
public class SbxSandboxProvider implements SandboxProvider {

    private static final String DEFAULT_HOST =
        "unix://" + System.getProperty("user.home") + "/.sbx/run/d/docker.sock";

    @Override
    public String name() {
        return "sbx";
    }

    @Override
    public SandboxCapabilities capabilities() {
        return new SandboxCapabilities(
            SandboxCapabilities.Isolation.VIRTUAL_MACHINE, true, true, true);
    }

    @Override
    public Sandbox create(final Map<String, ?> configs) {
        final SandboxTransformConfig config = new SandboxTransformConfig(configs);
        final String configured = config.sbxHost();
        return new SbxSandbox(
            configured.isEmpty() ? DEFAULT_HOST : configured,
            config.sbxName(),
            config.sbxImage(),
            config.sbxCpus(),
            config.sbxMemory(),
            config.command(),
            config.callTimeoutMs());
    }
}
