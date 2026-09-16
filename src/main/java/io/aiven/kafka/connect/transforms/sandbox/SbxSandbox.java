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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the guest inside a microVM, through a Docker Sandboxes engine.
 *
 * <p>Pointed at a microVM-backed engine -- Docker Sandboxes exposes one at
 * {@code ~/.sbx/run/d/docker.sock} -- the guest gets its own kernel and its own scheduler, and
 * {@code --cpus} becomes a real vCPU allocation rather than a share of host time. That is what
 * makes an enforced CPU bound possible, which is the one guarantee the WebAssembly runtime cannot
 * offer.
 *
 * <p>It shells out to the {@code docker} binary rather than speaking the Engine API over its unix
 * socket directly. That keeps the artifact at Java 11, so it still loads on Connect 3.x workers --
 * {@code SocketChannel} gained unix-domain support only in Java 16 -- and it adds no dependency to
 * a Connect plugin classloader. Nothing is bundled: whatever {@code docker} is on PATH is used.
 */
final class SbxSandbox implements Sandbox {

    private static final Logger log = LoggerFactory.getLogger(SbxSandbox.class);
    private static final String KEEPALIVE = "tail -f /dev/null";
    /** Budget for one docker lifecycle command; an image pull on a cold host is the slow case. */
    private static final long TIMEOUT_SECONDS = 180;

    private final String dockerHost;
    private final String name;
    private final StdioChannel channel;

    public SbxSandbox(final String dockerHost, final String name, final String image,
                         final String cpus, final String memory,
                         final List<String> guestCommand, final long timeoutMs) {
        if (guestCommand == null || guestCommand.isEmpty()) {
            throw new SandboxException("The \"sbx\" runtime requires sandbox.command");
        }
        if (image == null || image.isEmpty()) {
            throw new SandboxException("The \"sbx\" runtime requires sandbox.sbx.image");
        }
        this.dockerHost = dockerHost;
        this.name = name;

        // The container is kept alive by a no-op so the guest can be started as a long-lived exec
        // session. One sandbox serves every record for the life of the task.
        run(Arrays.asList("docker", "run", "-d", "--name", name,
            "--cpus", cpus, "--memory", memory, "--network", "none",
            image, "sh", "-c", KEEPALIVE), "start sandbox");

        final List<String> exec = new ArrayList<>(Arrays.asList("docker", "exec", "-i", name));
        exec.addAll(guestCommand);
        try {
            this.channel = new StdioChannel(exec, timeoutMs, environment());
        } catch (final RuntimeException e) {
            destroy();
            throw e;
        }
        log.info("Sandbox {} ready ({} cpus, {} memory)", name, cpus, memory);
    }

    @Override
    public byte[] call(final byte[] request) {
        return channel.call(request);
    }

    @Override
    public boolean isAlive() {
        return channel.isAlive();
    }

    @Override
    public void close() {
        channel.close();
        destroy();
    }

    private String[] environment() {
        return dockerHost == null || dockerHost.isEmpty()
            ? new String[0] : new String[] {"DOCKER_HOST", dockerHost};
    }

    private void destroy() {
        try {
            run(Arrays.asList("docker", "rm", "-f", name), "remove sandbox");
        } catch (final SandboxException e) {
            // Never let cleanup mask the original failure, but never leak a sandbox silently.
            log.warn("Could not remove sandbox {}; it may need removing by hand", name, e);
        }
    }

    private void run(final List<String> command, final String what) {
        try {
            final ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
            final String[] env = environment();
            for (int i = 0; i < env.length; i += 2) {
                builder.environment().put(env[i], env[i + 1]);
            }
            final Process process = builder.start();
            // Drain on another thread. readAll() returns only at EOF, which for a child process
            // means at exit, so reading inline here would make the timeout below unreachable:
            // a docker command that hangs -- a stale or unresponsive engine socket is the usual
            // cause -- would block the task thread for ever instead of failing after 180s.
            final StringBuilder collected = new StringBuilder();
            final Thread drain = new Thread(() -> {
                try {
                    collected.append(readAll(process));
                } catch (final IOException e) {
                    // The process was killed below; whatever it had already written is enough.
                }
            }, "sandbox-docker-drain");
            drain.setDaemon(true);
            drain.start();

            if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new SandboxException("Timed out after " + TIMEOUT_SECONDS
                    + "s trying to " + what);
            }
            // The child has exited, so EOF is imminent; bound the wait anyway rather than trade
            // one unbounded wait for another.
            drain.join(TimeUnit.SECONDS.toMillis(5));
            final String output = collected.toString();
            if (process.exitValue() != 0) {
                throw new SandboxException(
                    "Could not " + what + " (exit " + process.exitValue() + "): " + output.trim());
            }
        } catch (final IOException e) {
            throw new SandboxException("Could not run docker; is it installed and on PATH?", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SandboxException("Interrupted trying to " + what, e);
        }
    }

    private static String readAll(final Process process) throws IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final byte[] chunk = new byte[4096];
        int read;
        while ((read = process.getInputStream().read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return new String(buffer.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
