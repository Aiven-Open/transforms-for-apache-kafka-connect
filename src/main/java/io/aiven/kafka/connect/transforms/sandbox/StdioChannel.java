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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One request per line in, one response per line out, over a child process's stdio.
 *
 * <p>Carries every record for the {@code sbx} runtime. Writing requests and owning the process
 * lifecycle live here; matching an answer to the request that is waiting for it lives in
 * {@link GuestResponses}, because that is the part with the sharp edge.
 */
final class StdioChannel implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StdioChannel.class);

    private final Process process;
    private final OutputStream toGuest;
    /** Owns the one-line-in, one-line-out correlation and the abandoned-channel state. */
    private final GuestResponses responses = new GuestResponses();
    private final long timeoutMs;

    StdioChannel(final List<String> command, final long timeoutMs) {
        this(command, timeoutMs, new String[0]);
    }

    StdioChannel(final List<String> command, final long timeoutMs, final String[] env) {
        if (command == null || command.isEmpty()) {
            throw new SandboxException("A command is required to start the guest");
        }
        this.timeoutMs = timeoutMs;
        try {
            final ProcessBuilder builder = new ProcessBuilder(command);
            for (int i = 0; i < env.length; i += 2) {
                builder.environment().put(env[i], env[i + 1]);
            }
            this.process = builder.start();
        } catch (final IOException e) {
            throw new SandboxException("Could not start guest " + command, e);
        }
        this.toGuest = new BufferedOutputStream(process.getOutputStream());
        responses.drain("sandbox-stdout", process.getInputStream(), true);
        responses.drain("sandbox-stderr", process.getErrorStream(), false);
    }

    byte[] call(final byte[] request) {
        if (responses.isPoisoned()) {
            throw new SandboxException("Sandbox is no longer usable");
        }
        responses.expectAnswer();
        try {
            toGuest.write(request);
            toGuest.write('\n');
            toGuest.flush();
        } catch (final IOException e) {
            responses.poison();
            throw new SandboxException("Could not write to the guest", e);
        }

        final String line;
        try {
            line = responses.awaitAnswer(timeoutMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            responses.poison();
            throw new SandboxException("Interrupted waiting for the guest", e);
        }
        // Deliberately no `awaitingResponse.set(false)` here. The reader thread clears the flag
        // when it claims the request, so clearing it again from this side would reopen the race
        // it exists to close. Every path that leaves the flag set also poisons the channel, so it
        // is never observed stale by a later call.

        if (line == null) {
            // A late reply would be mistaken for the answer to the *next* record, so the channel
            // can never be trusted again.
            responses.poison();
            throw new SandboxException(
                "Guest did not answer within " + timeoutMs + "ms; channel abandoned");
        }
        return line.getBytes(StandardCharsets.UTF_8);
    }

    boolean isAlive() {
        return !responses.isPoisoned() && process.isAlive();
    }

    @Override
    public void close() {
        responses.poison();
        try {
            toGuest.close();
        } catch (final IOException e) {
            log.debug("Ignoring error closing guest stdin", e);
        }
        try {
            if (!process.waitFor(2, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
