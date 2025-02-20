/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.kafka.connect.transforms;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
final class IntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);

    private final TopicPartition originalTopicPartition0 =
        new TopicPartition(TestSourceConnector.ORIGINAL_TOPIC, 0);
    private final TopicPartition newTopicPartition0 =
        new TopicPartition(TestSourceConnector.NEW_TOPIC, 0);

    private final TopicPartition originalTopicValueFromSchema =
            new TopicPartition(TopicFromValueSchemaConnector.TOPIC, 0);

    private final TopicPartition newTopicValueFromSchema =
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
            TestSourceConnector.class, TestSourceConnector.TestSourceConnectorTask.class,
            TopicFromValueSchemaConnector.class,
            TopicFromValueSchemaConnector.TopicFromValueSchemaConnectorTask.class
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
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);

        final NewTopic originalTopic = new NewTopic(TestSourceConnector.ORIGINAL_TOPIC, 1, (short) 1);
        final NewTopic newTopic = new NewTopic(TestSourceConnector.NEW_TOPIC, 1, (short) 1);
        final NewTopic originalTopicForExtractTopicFromValue =
                new NewTopic(TopicFromValueSchemaConnector.TOPIC, 1, (short) 1);
        final NewTopic newTopicForExtractTopicFromValue  =
                new NewTopic(TopicFromValueSchemaConnector.NAME, 1, (short) 1);
        adminClient.createTopics(Arrays.asList(originalTopic, newTopic, originalTopicForExtractTopicFromValue,
                newTopicForExtractTopicFromValue)).all().get();

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
    final void testExtractTopic() throws ExecutionException, InterruptedException {
        final Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("name", "test-source-connector");
        connectorConfig.put("connector.class", TestSourceConnector.class.getName());
        connectorConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("transforms",
            "ExtractTopicFromValueField");
        connectorConfig.put("transforms.ExtractTopicFromValueField.type",
            "io.aiven.kafka.connect.transforms.ExtractTopic$Value");
        connectorConfig.put("transforms.ExtractTopicFromValueField.field.name",
            TestSourceConnector.ROUTING_FIELD);
        connectorConfig.put("tasks.max", "1");
        connectRunner.createConnector(connectorConfig);

        checkMessageTopics(originalTopicPartition0, newTopicPartition0);
    }

    @Test
    @Timeout(10)
    final void testExtractTopicFromValueSchemaName() throws ExecutionException, InterruptedException, IOException {
        final Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("name", "test-source-connector");
        connectorConfig.put("connector.class", TopicFromValueSchemaConnector.class.getName());
        connectorConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        connectorConfig.put("transforms",
                "ExtractTopicFromSchemaName");
        connectorConfig.put("transforms.ExtractTopicFromSchemaName.type",
                "io.aiven.kafka.connect.transforms.ExtractTopicFromSchemaName$Value");
        connectorConfig.put("tasks.max", "1");
        connectRunner.createConnector(connectorConfig);
        checkMessageTopics(originalTopicValueFromSchema, newTopicValueFromSchema);

    }

    @Test
    @Timeout(10)
    void testCaseTransform() throws ExecutionException, InterruptedException, IOException {
        adminClient.createTopics(Arrays.asList(new NewTopic(TestCaseTransformConnector.SOURCE_TOPIC, 1, (short) 1)))
                .all().get();
        adminClient.createTopics(Arrays.asList(new NewTopic(TestCaseTransformConnector.TARGET_TOPIC, 1, (short) 1)))
                .all().get();

        final Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("name", "test-source-connector");
        connectorConfig.put("connector.class", TestCaseTransformConnector.class.getName());
        connectorConfig.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        connectorConfig.put("tasks.max", "1");
        connectorConfig.put("transforms", "regexRouteToTargetTopic, caseTransform");
        connectorConfig.put("transforms.caseTransform.case", "upper");
        connectorConfig.put("transforms.caseTransform.field.names", TestCaseTransformConnector.TRANSFORM_FIELD);
        connectorConfig.put("transforms.caseTransform.type", "io.aiven.kafka.connect.transforms.CaseTransform$Value");
        connectorConfig.put("transforms.regexRouteToTargetTopic.type",
                "org.apache.kafka.connect.transforms.RegexRouter");
        connectorConfig.put("transforms.regexRouteToTargetTopic.regex", "(.*)-source-(.*)");
        connectorConfig.put("transforms.regexRouteToTargetTopic.replacement", String.format("$1-target-$2"));

        connectRunner.createConnector(connectorConfig);
        checkMessageTransformInTopic(
                new TopicPartition(TestCaseTransformConnector.TARGET_TOPIC, 0),
                TestCaseTransformConnector.MESSAGES_TO_PRODUCE
        );
    }

    final void checkMessageTransformInTopic(final TopicPartition topicPartition, final long expectedNumberOfMessages)
            throws InterruptedException, IOException {
        waitForCondition(
            () -> consumer.endOffsets(Arrays.asList(topicPartition))
                    .values().stream().reduce(Long::sum).map(s -> s == expectedNumberOfMessages)
                    .orElse(false), 5000, "Messages appear in target topic"
        );
        consumer.subscribe(Collections.singletonList(topicPartition.topic()));
        final ObjectMapper objectMapper = new ObjectMapper();
        final TypeReference<Map<String, Object>> tr = new TypeReference<>() {};
        for (final ConsumerRecord<byte[], byte[]> consumerRecord : consumer.poll(Duration.ofSeconds(1))) {
            final Map<String, Object> value = objectMapper.readValue(consumerRecord.value(), tr);
            final Map<String, String> payload = (Map<String, String>) value.get("payload");
            assertThat(payload.get("transform")).isEqualTo("LOWER-CASE-DATA-TRANSFORMS-TO-UPPERCASE");
        }
    }

    final void checkMessageTopics(final TopicPartition originalTopicPartition, final TopicPartition newTopicPartition)
            throws InterruptedException {
        waitForCondition(
            () -> consumer.endOffsets(Arrays.asList(originalTopicPartition, newTopicPartition))
                    .values().stream().reduce(Long::sum).map(s -> s >= TestSourceConnector.MESSAGES_TO_PRODUCE)
                    .orElse(false), 5000, "Messages appear in any topic"
        );
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(
            Arrays.asList(originalTopicPartition, newTopicPartition));
        // The original topic should be empty.
        assertThat(endOffsets.get(originalTopicPartition)).isZero();
        // The new topic should be non-empty.
        assertThat(endOffsets).containsEntry(newTopicPartition, TestSourceConnector.MESSAGES_TO_PRODUCE);
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
