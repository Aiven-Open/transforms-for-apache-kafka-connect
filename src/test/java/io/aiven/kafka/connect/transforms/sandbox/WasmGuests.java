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

import com.dylibso.chicory.wabt.Wat2Wasm;

/**
 * Builds tiny WebAssembly guests for tests.
 *
 * <p>Each guest implements the host ABI and answers with a fixed response. Holding the guest's own
 * work at zero is deliberate: it isolates the cost and correctness of the boundary itself, which
 * is what these tests and the benchmark actually measure.
 */
final class WasmGuests {

    private WasmGuests() {
    }

    /** A guest that always answers with {@code json}. */
    static Path returning(final Path dir, final String name, final String json) throws IOException {
        final int length = json.getBytes(StandardCharsets.UTF_8).length;
        final String escaped = json.replace("\\", "\\\\").replace("\"", "\\\"");
        final String wat =
            "(module\n"
                + "  (memory (export \"memory\") 1)\n"
                + "  (global $bump (mut i32) (i32.const 1024))\n"
                + "  (data (i32.const 0) \"" + escaped + "\")\n"
                + "  (func (export \"alloc\") (param $size i32) (result i32)\n"
                + "    (local $p i32)\n"
                + "    (local.set $p (global.get $bump))\n"
                + "    (global.set $bump (i32.add (global.get $bump) (local.get $size)))\n"
                + "    (local.get $p))\n"
                + "  (func (export \"reset\")\n"
                + "    (global.set $bump (i32.const 1024)))\n"
                + "  (func (export \"transform\") (param $ptr i32) (param $len i32) (result i64)\n"
                + "    (i64.or (i64.shl (i64.const 0) (i64.const 32)) (i64.const "
                + length + "))))\n";
        return write(dir, name, wat);
    }

    /**
     * A guest with a bump allocator and no {@code reset}, to demonstrate the leak the reset export
     * exists to prevent.
     */
    static Path leaking(final Path dir, final String name, final String json) throws IOException {
        final int length = json.getBytes(StandardCharsets.UTF_8).length;
        final String escaped = json.replace("\\", "\\\\").replace("\"", "\\\"");
        final String wat =
            "(module\n"
                + "  (memory (export \"memory\") 1)\n"
                + "  (global $bump (mut i32) (i32.const 1024))\n"
                + "  (data (i32.const 0) \"" + escaped + "\")\n"
                + "  (func (export \"alloc\") (param $size i32) (result i32)\n"
                + "    (local $p i32)\n"
                + "    (local.set $p (global.get $bump))\n"
                + "    (global.set $bump (i32.add (global.get $bump) (local.get $size)))\n"
                + "    (local.get $p))\n"
                + "  (func (export \"transform\") (param $ptr i32) (param $len i32) (result i64)\n"
                + "    (i64.or (i64.shl (i64.const 0) (i64.const 32)) (i64.const "
                + length + "))))\n";
        return write(dir, name, wat);
    }

    /** A guest that traps, to prove a misbehaving module cannot take the worker with it. */
    static Path trapping(final Path dir, final String name) throws IOException {
        final String wat =
            "(module\n"
                + "  (memory (export \"memory\") 1)\n"
                + "  (func (export \"alloc\") (param $size i32) (result i32) (i32.const 1024))\n"
                + "  (func (export \"transform\") (param $ptr i32) (param $len i32) (result i64)\n"
                + "    (unreachable)))\n";
        return write(dir, name, wat);
    }

    /**
     * Returns a packed (ptr, len) pair the host must reject without acting on it.
     *
     * <p>Both halves of the return value are chosen by the guest, so the host cannot size a buffer
     * from them without checking first: a length near 2^31 would have the worker attempt a 2GB
     * allocation, and the resulting OutOfMemoryError is an Error rather than a ChicoryException,
     * so it escapes the trap handling entirely.
     */
    static Path returningOutOfRange(final Path dir, final String name,
                                    final int ptr, final int len) throws IOException {
        final long packed = ((long) ptr << 32) | (len & 0xFFFFFFFFL);
        final String wat =
            "(module\n"
                + "  (memory (export \"memory\") 1)\n"
                + "  (func (export \"alloc\") (param $size i32) (result i32) (i32.const 1024))\n"
                + "  (func (export \"transform\") (param $ptr i32) (param $len i32) (result i64)\n"
                + "    (i64.const " + packed + ")))\n";
        return write(dir, name, wat);
    }

    private static Path write(final Path dir, final String name, final String wat)
        throws IOException {
        final Path out = dir.resolve(name + ".wasm");
        Files.write(out, Wat2Wasm.parse(wat));
        return out;
    }
}
