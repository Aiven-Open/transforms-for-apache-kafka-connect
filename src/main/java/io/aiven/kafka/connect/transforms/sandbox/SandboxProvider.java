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
 * One kind of sandbox. Providers are discovered through {@link java.util.ServiceLoader}.
 *
 * <p>This is the extension point, and the point of the project. A sandbox is an interchangeable
 * thing: WebAssembly in the worker JVM and a microVM on the host are the same idea at wildly
 * different points on the isolation-versus-cost curve, and which one a pipeline wants is an
 * operational decision, not an architectural one. A third party ships a JAR with a new provider
 * and it becomes selectable by name with no change here.
 *
 * <p>Register implementations in
 * {@code META-INF/services/io.aiven.kafka.connect.transforms.sandbox.SandboxProvider}.
 */
public interface SandboxProvider {

    /** The value of {@code sandbox.provider} that selects this provider. Must be unique. */
    String name();

    /** What this provider's boundary actually enforces. Reported by the transform at start-up. */
    SandboxCapabilities capabilities();

    /**
     * Provision a sandbox and return it ready for use.
     *
     * <p>Whatever provisioning a provider needs happens here: loading and compiling a module,
     * starting a process, creating a microVM. An in-process provider does almost nothing; an
     * out-of-process one may take seconds. Either way the cost is paid once per Connect task, not
     * per record.
     *
     * @param configs the full, unparsed transform configuration
     * @return a started sandbox, ready to accept {@link Sandbox#call(byte[])}
     * @throws SandboxException if the sandbox cannot be started
     */
    Sandbox create(Map<String, ?> configs);
}
