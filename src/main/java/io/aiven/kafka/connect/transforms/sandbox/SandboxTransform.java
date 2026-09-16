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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs user-supplied code against each record inside an isolated {@link Sandbox}.
 *
 * <p>A custom transform today means writing Java, building a JAR, getting it onto every worker's
 * {@code plugin.path}, and restarting the workers -- and on a managed Connect service it is not
 * possible at all. This transform takes the logic as <em>configuration</em> instead: a path to a
 * WebAssembly module, or an image to run, in the connector config like any other SMT setting. The
 * guest can be written in any language that compiles to one of those targets.
 *
 * <p>An isolation boundary is what makes that shape workable, not a security fix. Kafka's trust
 * position is unchanged and this plugin does not alter it: transforms execute in the worker JVM
 * with its privileges, and installing one is an act of trust. That applies to this plugin's own
 * JAR exactly as it does to any other. What changes is the guest: it runs behind
 * {@link Sandbox} rather than as more privileged Java on the class path, so taking it as
 * configuration is a reasonable thing to do rather than a way of handing the worker to whoever
 * wrote the config. The strength of that boundary is a property of the provider in use, which is
 * why {@link SandboxCapabilities} is declared per provider rather than claimed once here.
 *
 * <p>Configure it like any other SMT:
 * <pre>
 * transforms=redact
 * transforms.redact.type=io.aiven.kafka.connect.transforms.sandbox.SandboxTransform
 * transforms.redact.sandbox.provider=wasm
 * transforms.redact.sandbox.module=/opt/transforms/redact.wasm
 * </pre>
 *
 * <p>Errors are deliberately split in two so that Connect's existing machinery keeps working. A
 * record the guest rejects raises {@link DataException}; a broken boundary raises
 * {@link SandboxException}. What Connect then does with either depends on the connector's
 * {@code errors.tolerance}, and on whether it is a sink or a source:
 *
 * <ul>
 *   <li>With the default {@code errors.tolerance=none}, both fail the task.</li>
 *   <li>With {@code errors.tolerance=all}, both are tolerated and the record is skipped. Connect
 *       classifies tolerable exceptions by stage, not by type, so a {@link SandboxException} from
 *       a broken boundary is swallowed just as a {@link DataException} is. An operator who wants
 *       bad records skipped but a broken sandbox to stop the task cannot express that through
 *       {@code errors.tolerance} alone.</li>
 *   <li>The dead-letter queue applies to <b>sink connectors only</b>. {@code Worker} installs a
 *       {@code DeadLetterQueueReporter} for sink tasks and only a {@code LogReporter} for source
 *       tasks, and the {@code errors.deadletterqueue.*} properties are defined on
 *       {@code SinkConnectorConfig} alone. On a source connector a rejected record is logged and
 *       dropped, never captured.</li>
 * </ul>
 *
 * <p>Note also that the dead-letter queue records the message as it arrived, before this
 * transform ran. For a redaction transform that means the DLQ holds the unmasked original.
 *
 * <p>Tombstones pass through untouched: there is no value to transform, and dropping them would
 * break compaction semantics for downstream consumers.
 */
public class SandboxTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(SandboxTransform.class);

    private final ReentrantLock lock = new ReentrantLock();

    private SandboxTransformConfig config;
    private RecordEnvelopeCodec codec;
    private volatile Sandbox runtime;

    @Override
    public ConfigDef config() {
        return SandboxTransformConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new SandboxTransformConfig(configs);
        this.codec = new RecordEnvelopeCodec(config.schemasEnable());
        final SandboxProvider provider = resolveProvider(config.providerName());
        this.runtime = provider.create(configs);
        log.info("Sandbox transform ready using the \"{}\" provider: {}",
            provider.name(), provider.capabilities());
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null) {
            return record;
        }
        final Sandbox current = runtime;
        if (current == null) {
            throw new SandboxException("Sandbox transform used before configure() or after close()");
        }

        final byte[] request = codec.encode(record);

        // The Transformation contract requires apply() to be thread-safe, and a boundary is a
        // single conversation. Connect gives each task its own transform instance on its own
        // thread, so this lock is essentially uncontended in practice; it exists for correctness.
        final byte[] response;
        lock.lock();
        try {
            if (!current.isAlive()) {
                throw new SandboxException("Sandbox is no longer alive");
            }
            response = current.call(request);
        } finally {
            lock.unlock();
        }

        final GuestResponse guest = codec.decode(record.topic(), response);
        if (guest.isDrop()) {
            return null;
        }
        if (guest.isError()) {
            throw new DataException("Sandbox rejected the record: " + guest.error());
        }

        final SchemaAndValue value = guest.value();
        final String topic = guest.topic() != null ? guest.topic() : record.topic();
        return record.newRecord(
            topic,
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            value.schema(),
            value.value(),
            record.timestamp(),
            record.headers()
        );
    }

    @Override
    public void close() {
        final Sandbox current = runtime;
        runtime = null;
        if (current != null) {
            current.close();
        }
    }

    /**
     * Reports the plugin version.
     *
     * <p>Intentionally not annotated with {@code @Override}: Kafka 4.x added {@code version()} to
     * {@code Transformation} via {@code ConnectPlugin}, while 3.x has no such method. Declaring it
     * without the annotation overrides the newer default and remains a harmless extra method on
     * older workers, so one artifact serves both.
     */
    public String version() {
        final String implementation = getClass().getPackage().getImplementationVersion();
        return implementation != null ? implementation : "0.1.0-SNAPSHOT";
    }

    private static SandboxProvider resolveProvider(final String name) {
        // Load through this class's own classloader: under Connect that is the plugin
        // classloader, which is what actually contains the bundled runtimes.
        final ServiceLoader<SandboxProvider> loader =
            ServiceLoader.load(SandboxProvider.class, SandboxTransform.class.getClassLoader());
        final List<String> available = new ArrayList<>();
        for (final SandboxProvider factory : loader) {
            if (factory.name().equals(name)) {
                return factory;
            }
            available.add(factory.name());
        }
        throw new SandboxException(
            "No sandbox runtime named \"" + name + "\". Available runtimes: " + available);
    }
}
