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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matches one line of guest output to the one request that is waiting for it.
 *
 * <p>Separate from {@link StdioChannel}, which owns the process and writes requests, because
 * correlating answers to requests is the part with the sharp edge. A guest that prints a start-up
 * banner, or answers a single request twice, puts a line on the stream that belongs to no
 * request. Delivering it would answer the <em>next</em> record with the previous record's data:
 * valid JSON, plausible content, wrong record. That is silent corruption rather than a visible
 * failure, so an unclaimed line abandons the channel instead.
 *
 * <p>Once abandoned the channel stays abandoned. A late answer can never be handed to a later
 * record.
 */
final class GuestResponses {

    private static final Logger log = LoggerFactory.getLogger(GuestResponses.class);

    /** Holds one line: the answer to the single outstanding request, and never more. */
    private final BlockingQueue<String> answers = new ArrayBlockingQueue<>(1);
    private final AtomicBoolean poisoned = new AtomicBoolean(false);
    private final AtomicBoolean awaiting = new AtomicBoolean(false);

    /** Declares that a request has been written and exactly one answer is now expected. */
    void expectAnswer() {
        awaiting.set(true);
    }

    /** Waits for the outstanding answer, or null if none arrives in time. */
    String awaitAnswer(final long timeoutMs) throws InterruptedException {
        return answers.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    void poison() {
        poisoned.set(true);
    }

    boolean isPoisoned() {
        return poisoned.get();
    }

    /**
     * Starts a daemon thread draining one of the guest's output streams.
     *
     * <p>Only stdout carries answers. A guest may close stderr and keep serving perfectly well, so
     * only stdout closing ends the conversation.
     */
    void drain(final String name, final InputStream stream, final boolean isStdout) {
        final Thread thread = new Thread(() -> readUntilClosed(name, stream, isStdout), name);
        thread.setDaemon(true);
        thread.start();
    }

    private void readUntilClosed(final String name, final InputStream stream,
                                 final boolean isStdout) {
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!isStdout) {
                    log.warn("[sandbox] {}", line);
                } else if (!claim(line)) {
                    return;
                }
            }
        } catch (final IOException e) {
            log.debug("Guest stream {} closed", name, e);
        } finally {
            if (isStdout) {
                poisoned.set(true);
            }
        }
    }

    /**
     * Claims the outstanding request and hands the line over as one atomic step.
     *
     * <p>Testing the flag and then queueing would be check-then-act: a guest that answers twice can
     * have its second line pass the test before the caller clears the flag, and that line is then
     * delivered as the answer to the next record. The queue holds one element, so a failed offer
     * means a line is already waiting, which is the same protocol violation seen from the other
     * side.
     *
     * @return false if the line was unclaimed, in which case the channel has been abandoned
     */
    private boolean claim(final String line) {
        if (awaiting.compareAndSet(true, false) && answers.offer(line)) {
            return true;
        }
        log.error("Guest wrote to stdout with no request outstanding; abandoning the channel. "
            + "Offending line: {}", line);
        poisoned.set(true);
        return false;
    }
}
