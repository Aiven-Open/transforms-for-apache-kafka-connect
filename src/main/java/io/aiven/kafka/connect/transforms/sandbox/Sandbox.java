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

import java.io.Closeable;

/**
 * An isolated execution boundary that transforms one encoded record at a time.
 *
 * <p>This is the whole vendor-neutral contract. A runtime receives an opaque request payload,
 * executes untrusted user code against it inside whatever isolation boundary it provides, and
 * returns an opaque response payload. It knows nothing about Kafka Connect, and Connect knows
 * nothing about how the isolation is achieved.
 *
 * <p><b>Cost model.</b> {@link org.apache.kafka.connect.transforms.Transformation#apply} is
 * per-record and synchronous, so exactly one {@link #call(byte[])} happens per record. There is
 * no batching available at this layer. Implementations are therefore judged on round-trip
 * latency, not throughput: a runtime costing 50us per record caps a task at ~20k records/s.
 *
 * <p><b>Threading.</b> {@code Transformation.apply} must be thread-safe, but implementations of
 * this interface need not be: {@link SandboxTransform} serialises all calls. Implementations
 * must still tolerate {@link #close()} racing with an in-flight {@link #call(byte[])}.
 */
public interface Sandbox extends Closeable {

    /**
     * Execute the guest transform against one encoded record.
     *
     * @param request the encoded record; never null
     * @return the encoded response; never null
     * @throws SandboxException if the isolation boundary itself failed (guest crashed, timed out,
     *     or produced an unreadable response). Errors <em>inside</em> a healthy guest are reported
     *     in the response payload, not by throwing.
     */
    byte[] call(byte[] request);

    /** Whether this runtime is still usable. A false return makes the owning task fail fast. */
    boolean isAlive();

    /** Releases the boundary. Must be idempotent. */
    @Override
    void close();
}
