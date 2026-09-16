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

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import com.dylibso.chicory.compiler.MachineFactoryCompiler;
import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Memory;
import com.dylibso.chicory.wasm.ChicoryException;
import com.dylibso.chicory.wasm.Parser;
import com.dylibso.chicory.wasm.WasmModule;
import com.dylibso.chicory.wasm.types.MemoryLimits;

/**
 * Runs the guest as a WebAssembly module inside the worker JVM, using Chicory.
 *
 * <p>Chicory is a pure-Java Wasm runtime with no JNI and no native artifacts, which is the reason
 * it is the default here: the plugin stays an ordinary JAR that drops into {@code plugin.path} on
 * any Connect worker, including the containerised ones operated by Strimzi, Aiven and Confluent.
 * A runtime requiring {@code /var/run/docker.sock} or {@code /dev/kvm} cannot be installed there
 * at all, however good its isolation.
 *
 * <h2>What this boundary does and does not give you</h2>
 * <ul>
 *   <li><b>Memory safety.</b> The guest addresses only its own linear memory, capped by
 *       {@code sandbox.wasm.max.memory.pages}. It cannot reach the worker's heap, other
 *       connectors' configurations, or their credentials.</li>
 *   <li><b>No syscalls.</b> No WASI or host imports are supplied, so the guest has no filesystem,
 *       no network and no clock. It computes over the bytes it is handed and nothing else.</li>
 *   <li><b>No CPU bound.</b> This is the honest limitation. A guest that loops forever occupies
 *       the calling task thread, and a Java thread cannot be safely killed. Wasm gives memory and
 *       syscall isolation at microsecond cost but cannot bound CPU in-process; bounding CPU needs
 *       a process or VM boundary, which costs one to two orders of magnitude more per record.
 *       That trade-off is exactly why {@code Sandbox} is pluggable: use this for code you
 *       compiled, and an out-of-process runtime for code you did not.</li>
 * </ul>
 *
 * <h2>Guest ABI</h2>
 * The module must export a {@code memory} plus two functions:
 * <pre>
 * alloc(size: i32) -&gt; ptr: i32
 * transform(ptr: i32, len: i32) -&gt; packed: i64   // (out_ptr &lt;&lt; 32) | out_len
 * reset()                                        // optional, strongly recommended
 * </pre>
 * The host writes the request JSON at {@code alloc(len)}, calls {@code transform}, and reads the
 * response from the returned pointer and length.
 *
 * <p>{@code reset} exists because one instance serves every record for the life of the task. A
 * guest using a bump allocator and no {@code reset} will exhaust its linear memory after a few
 * thousand records. If the export is present the host calls it before each record; guests that
 * free their own allocations may omit it.
 */
final class WasmSandbox implements Sandbox {

    private static final String ALLOC_EXPORT = "alloc";
    private static final String TRANSFORM_EXPORT = "transform";
    private static final String RESET_EXPORT = "reset";

    private final Instance instance;
    private final ExportFunction alloc;
    private final ExportFunction transform;
    private final ExportFunction reset;
    private final Memory memory;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WasmSandbox(final String modulePath, final int maxMemoryPages) {
        this(modulePath, maxMemoryPages, true);
    }

    public WasmSandbox(final String modulePath, final int maxMemoryPages,
                              final boolean compile) {
        if (modulePath == null || modulePath.isEmpty()) {
            throw new SandboxException("The wasm runtime requires sandbox.module");
        }
        final File moduleFile = new File(modulePath);
        if (!moduleFile.isFile()) {
            throw new SandboxException("No wasm module at " + moduleFile.getAbsolutePath());
        }
        try {
            final WasmModule module = Parser.parse(moduleFile);
            // Honour the module's own initial memory and cap only the maximum. Forcing the
            // initial size instead would break any guest whose data segments sit above it --
            // a Rust cdylib, for instance, places its data after a 1MiB stack.
            final int declaredPages = module.memorySection()
                .filter(section -> section.memoryCount() > 0)
                .map(section -> section.getMemory(0).limits().initialPages())
                .orElse(1);
            if (declaredPages > maxMemoryPages) {
                throw new SandboxException("Wasm module needs " + declaredPages
                    + " memory pages but " + SandboxTransformConfig.MAX_MEMORY_PAGES_CONFIG
                    + " is " + maxMemoryPages);
            }
            final Instance.Builder builder = Instance.builder(module)
                .withMemoryLimits(new MemoryLimits(declaredPages, maxMemoryPages));
            if (compile) {
                builder.withMachineFactory(MachineFactoryCompiler.compile(module));
            }
            this.instance = builder.build();
            // Resolve the required exports separately from loading the module. Chicory throws
            // for a missing export, so doing this inside the catch below would report a guest
            // built against the wrong ABI as "could not load module" -- true, but it buries the
            // one detail the guest author needs, which is which exports were expected.
            this.alloc = requiredExport(instance, ALLOC_EXPORT);
            this.transform = requiredExport(instance, TRANSFORM_EXPORT);
            this.reset = optionalExport(instance, RESET_EXPORT);
            this.memory = instance.memory();
        } catch (final ChicoryException e) {
            throw new SandboxException("Could not load wasm module " + modulePath + ": "
                + e.getMessage(), e);
        }
        if (memory == null) {
            throw new SandboxException(abiMessage());
        }
    }

    @Override
    public byte[] call(final byte[] request) {
        if (closed.get()) {
            throw new SandboxException("Sandbox runtime is closed");
        }
        try {
            // One instance serves every record for the life of the task, so anything the guest
            // allocated for the previous record is dead. Without this the guest's allocator grows
            // monotonically and a long-running task eventually exhausts its linear memory --
            // observed at ~10k records with a 64KiB page. Reset first rather than last so the
            // memory of a failed call survives for inspection.
            if (reset != null) {
                reset.apply();
            }
            final int inPtr = (int) alloc.apply(request.length)[0];
            memory.write(inPtr, request, 0, request.length);

            final long packed = transform.apply(inPtr, request.length)[0];
            final int outPtr = (int) (packed >>> 32);
            final int outLen = (int) (packed & 0xFFFFFFFFL);

            // Validate the pair against the guest's live memory before reading it. Both halves
            // are entirely guest-controlled, and readBytes would otherwise size a host array from
            // them: a guest returning a length near 2^31 makes the worker attempt a 2GB
            // allocation. That fails with an OutOfMemoryError, which is an Error rather than a
            // ChicoryException, so it escapes the catch below, escapes SandboxException, and
            // takes down more than this task -- the one failure mode a sandbox must not have.
            final long memoryBytes = (long) memory.pages() * Memory.PAGE_SIZE;
            if (outPtr < 0 || outLen < 0 || (long) outPtr + outLen > memoryBytes) {
                throw new SandboxException("Wasm guest returned an out-of-range response: ptr="
                    + outPtr + " len=" + outLen + ", guest memory is " + memoryBytes + " bytes");
            }
            return memory.readBytes(outPtr, outLen);
        } catch (final ChicoryException e) {
            // A trap means the guest violated the Wasm contract. The boundary held: the worker is
            // unharmed. Fail the task rather than pretend the transform succeeded.
            throw new SandboxException("Wasm guest trapped: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isAlive() {
        return !closed.get();
    }

    @Override
    public void close() {
        // Nothing to release: the instance and its linear memory are ordinary Java objects that
        // become garbage once this runtime is unreachable. There is no process or VM to reap,
        // which is precisely the appeal of an in-process boundary.
        closed.set(true);
    }

    private static String abiMessage() {
        return "Wasm module must export \"memory\", \"" + ALLOC_EXPORT + "\" and \""
            + TRANSFORM_EXPORT + "\"";
    }

    /** Throws {@link SandboxException}, not {@link ChicoryException}, so the ABI message survives. */
    private static ExportFunction requiredExport(final Instance target, final String name) {
        final ExportFunction export;
        try {
            export = target.export(name);
        } catch (final ChicoryException e) {
            throw new SandboxException(abiMessage() + " (no \"" + name + "\" export found)", e);
        }
        if (export == null) {
            throw new SandboxException(abiMessage() + " (no \"" + name + "\" export found)");
        }
        return export;
    }

    private static ExportFunction optionalExport(final Instance target, final String name) {
        try {
            return target.export(name);
        } catch (final ChicoryException e) {
            return null;
        }
    }

}
