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

/**
 * What a provider's boundary actually is, and what it enforces.
 *
 * <p>Sandboxes differ in kind, not merely in speed, and the differences decide whether a provider
 * is appropriate for a given guest. Each field here is a structural fact that holds on any host --
 * deliberately not a latency figure, which depends on the machine and belongs in a benchmark
 * rather than frozen into an interface.
 */
public final class SandboxCapabilities {

    /** The kind of boundary between the guest and the Connect worker. */
    public enum Isolation {
        /** Same thread, same JVM. Memory-safe, but shares the worker's fate. */
        IN_PROCESS,
        /** An ordinary child process of the worker. Not a sandbox. */
        HOST_PROCESS,
        /** Namespaces and cgroups, sharing the host kernel. */
        CONTAINER,
        /** Its own kernel and its own scheduler. */
        VIRTUAL_MACHINE
    }

    private final Isolation isolation;
    private final boolean boundsMemory;
    private final boolean boundsCpu;
    private final boolean blocksSyscalls;

    public SandboxCapabilities(final Isolation isolation, final boolean boundsMemory,
                               final boolean boundsCpu, final boolean blocksSyscalls) {
        this.isolation = isolation;
        this.boundsMemory = boundsMemory;
        this.boundsCpu = boundsCpu;
        this.blocksSyscalls = blocksSyscalls;
    }

    public Isolation isolation() {
        return isolation;
    }

    /** Whether the guest can be held to a memory ceiling. */
    public boolean boundsMemory() {
        return boundsMemory;
    }

    /** Whether a guest that never returns can be stopped. */
    public boolean boundsCpu() {
        return boundsCpu;
    }

    /** Whether the guest is denied filesystem, network and other host syscalls. */
    public boolean blocksSyscalls() {
        return blocksSyscalls;
    }

    @Override
    public String toString() {
        return isolation + " (memory=" + boundsMemory + ", cpu=" + boundsCpu
            + ", syscalls-blocked=" + blocksSyscalls + ")";
    }
}
