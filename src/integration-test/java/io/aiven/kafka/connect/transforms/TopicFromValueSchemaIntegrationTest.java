package io.aiven.kafka.connect.transforms;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
@Testcontainers
public class TopicFromValueSchemaIntegrationTest {

        private static final Logger log = LoggerFactory.getLogger(TopicFromValueSchemaIntegrationTest.class);

        private final TopicPartition originalTopicPartition0 =
                new TopicPartition(TopicFromValueSchemaConnector.TOPIC, 0);

        private final TopicPartition newTopicPartition0 =
            new TopicPartition(TopicFromValueSchemaConnector.NAME, 0);

        private static File pluginsDir;

        @Container
        private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

        private AdminClient adminClient;
        private KafkaConsumer<byte[], byte[]> consumer;

        private ConnectRunner connectRunner;

        @BeforeAll
        static void setUpAll() throws IOException, InterruptedException {
            final File testDir = Files.createTempDirectory("transforms-for-apache-kafka-connect-test-").toFile();
            testDir.deleteOnExit();

            pluginsDir = new File(testDir, "plugins/");
            assert pluginsDir.mkdirs();

            // Unpack the library distribution.
            final File transformDir = new File(pluginsDir, "transforms-for-apache-kafka-connect/");
            assert transformDir.mkdirs();
            final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
            assert distFile.exists();
            final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
                    distFile.toString(), transformDir.toString());
            final Process p = Runtime.getRuntime().exec(cmd);
            assert p.waitFor() == 0;

            // Copy the test connector classes.
            final File testConnectorPluginDir = new File(pluginsDir, "test-connector/");
            assert testConnectorPluginDir.mkdirs();
            final File integrationTestClassesPath = new File(System.getProperty("integration-test.classes.path"));
            assert integrationTestClassesPath.exists();

            final Class<?>[] testConnectorClasses = new Class[]{
                    TopicFromValueSchemaConnector.class, TopicFromValueSchemaConnector.TopicFromValueSchemaConnectorTask.class
            };
            for (final Class<?> clazz : testConnectorClasses) {
                final String packageName = clazz.getPackage().getName();
                final String packagePrefix = packageName + ".";
                assert clazz.getCanonicalName().startsWith(packagePrefix);

                final String packageSubpath = packageName.replace('.', '/');
                final String classNameWithoutPackage = clazz.getCanonicalName().substring(packagePrefix.length());
                final String classFileName = classNameWithoutPackage.replace('.', '$') + ".class";
                final File classFileSrc = new File(
                        new File(integrationTestClassesPath, packageSubpath), classFileName
                );
                assert classFileSrc.exists();
                final File classFileDest = new File(testConnectorPluginDir, classFileName);
                Files.copy(classFileSrc.toPath(), classFileDest.toPath());
            }
        }

        @BeforeEach
        void setUp() throws ExecutionException, InterruptedException {
            final Properties adminClientConfig = new Properties();
            adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            adminClient = AdminClient.create(adminClientConfig);

            final Map<String, Object> consumerProps = new HashMap<>();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumer = new KafkaConsumer<>(consumerProps);

            final NewTopic originalTopic = new NewTopic(TopicFromValueSchemaConnector.TOPIC, 1, (short) 1);
            final NewTopic newTopic = new NewTopic(TopicFromValueSchemaConnector.NAME, 1, (short) 1);
            adminClient.createTopics(Arrays.asList(originalTopic, newTopic)).all().get();

            connectRunner = new ConnectRunner(pluginsDir, kafka.getBootstrapServers());
            connectRunner.start();
        }
        @AfterEach
        final void tearDown() {
            connectRunner.stop();
            adminClient.close();
            consumer.close();

            connectRunner.awaitStop();
        }

        @Test
        @Timeout(10)
        final void testExtractTopicFromValueSchemaName() throws ExecutionException, InterruptedException, IOException {
            final Map<String, String> connectorConfig = new HashMap<>();
            connectorConfig.put("name", "test-source-connector");
            connectorConfig.put("connector.class", TopicFromValueSchemaConnector.class.getName());
            connectorConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
            connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            connectorConfig.put("value.converter.value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
            connectorConfig.put("transforms",
                    "ExtractTopicFromValueSchema");
            connectorConfig.put("transforms.ExtractTopicFromValueSchema.type",
                    "io.aiven.kafka.connect.transforms.ExtractTopicFromValueSchema$Name");
            connectorConfig.put("tasks.max", "1");
            connectRunner.createConnector(connectorConfig);

            waitForCondition(
                    () -> consumer
                            .endOffsets(Arrays.asList(originalTopicPartition0, newTopicPartition0))
                            .values().stream().reduce(Long::sum).map(s -> s >= TestSourceConnector.MESSAGES_TO_PRODUCE)
                            .orElse(false),
                    5000, "Messages appear in any topic"
            );
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(
                    Arrays.asList(originalTopicPartition0, newTopicPartition0));
            // The original topic should be empty.
            assertEquals(0, endOffsets.get(originalTopicPartition0));
            // The new topic should be non-empty.
            assertEquals(TestSourceConnector.MESSAGES_TO_PRODUCE, endOffsets.get(newTopicPartition0));
        }

        private void waitForCondition(final Supplier<Boolean> conditionChecker,
                                      final long maxWaitMs,
                                      final String condition) throws InterruptedException {
            final long startTime = System.currentTimeMillis();

            boolean testConditionMet;
            while (!(testConditionMet = conditionChecker.get()) && ((System.currentTimeMillis() - startTime) < maxWaitMs)) {
                Thread.sleep(Math.min(maxWaitMs, 100L));
            }

            if (!testConditionMet) {
                throw new AssertionError("Condition not met within timeout " + maxWaitMs + ": " + condition);
            }
        }
}