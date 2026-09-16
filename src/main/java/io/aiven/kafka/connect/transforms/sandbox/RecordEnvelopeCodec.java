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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Translates between {@link ConnectRecord} and the bytes crossing the sandbox boundary.
 *
 * <p>Record values are encoded with Connect's own {@link JsonConverter} rather than a bespoke
 * mapping. That choice is deliberate: {@code JsonConverter} already defines the canonical
 * Connect-to-JSON translation, including {@code Struct}s, logical types and schema envelopes, so
 * guest authors work against a format that is already documented and that round-trips losslessly.
 *
 * <p>The envelope sent to the guest is a single JSON object:
 * <pre>
 * {"topic":"orders","partition":0,"timestamp":1736200000000,"value":&lt;connect-json&gt;}
 * </pre>
 * and the guest answers with exactly one of:
 * <pre>
 * {"value":&lt;connect-json&gt;}          transform (optionally with "topic" to reroute)
 * {"drop":true}                        filter the record out
 * {"error":"why"}                      reject the record
 * </pre>
 */
final class RecordEnvelopeCodec {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonConverter converter = new JsonConverter();

    public RecordEnvelopeCodec(final boolean schemasEnable) {
        final Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", schemasEnable);
        converterConfig.put("converter.type", ConverterType.VALUE.getName());
        converter.configure(converterConfig);
    }

    /** Encodes one record into the request payload handed to the guest. */
    public byte[] encode(final ConnectRecord<?> record) {
        final ObjectNode envelope = mapper.createObjectNode();
        envelope.put("topic", record.topic());
        if (record.kafkaPartition() != null) {
            envelope.put("partition", record.kafkaPartition());
        }
        if (record.timestamp() != null) {
            envelope.put("timestamp", record.timestamp());
        }
        final byte[] valueJson =
            converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        try {
            envelope.set("value", mapper.readTree(valueJson));
            return mapper.writeValueAsBytes(envelope);
        } catch (final IOException e) {
            throw new DataException("Could not encode record for the sandbox", e);
        }
    }

    /**
     * Decodes the guest's answer.
     *
     * @param topic the record's current topic, needed to reconstruct Connect data
     * @param payload the raw guest response
     * @return the decoded instruction
     * @throws SandboxException if the guest produced something that is not a valid response
     */
    public GuestResponse decode(final String topic, final byte[] payload) {
        final JsonNode response = parse(payload);

        // Each field is type-checked rather than coerced. A guest answering {"drop":"yes"} or
        // {"error":{...}} has violated the protocol, and coercing that into a dropped or
        // DLQ-routed record would destroy data on the strength of a malformed response.
        if (isDrop(response)) {
            return GuestResponse.drop();
        }
        if (response.hasNonNull("error")) {
            return GuestResponse.error(requireText(response, "error"));
        }
        if (!response.has("value")) {
            throw new SandboxException(
                "Sandbox response must contain one of \"value\", \"drop\" or \"error\"");
        }
        final String newTopic = response.hasNonNull("topic") ? requireText(response, "topic") : null;
        return GuestResponse.transformed(newTopic, toConnectValue(topic, response.get("value")));
    }

    private JsonNode parse(final byte[] payload) {
        final JsonNode response;
        try {
            response = mapper.readTree(payload);
        } catch (final IOException e) {
            throw new SandboxException("Sandbox returned a malformed response", e);
        }
        if (response == null || !response.isObject()) {
            throw new SandboxException("Sandbox response must be a JSON object");
        }
        return response;
    }

    private static boolean isDrop(final JsonNode response) {
        if (!response.has("drop")) {
            return false;
        }
        if (!response.get("drop").isBoolean()) {
            throw new SandboxException("Sandbox response field \"drop\" must be a boolean");
        }
        return response.get("drop").booleanValue();
    }

    private static String requireText(final JsonNode response, final String field) {
        if (!response.get(field).isTextual()) {
            throw new SandboxException(
                "Sandbox response field \"" + field + "\" must be a string");
        }
        return response.get(field).textValue();
    }

    private SchemaAndValue toConnectValue(final String topic, final JsonNode value) {
        try {
            return converter.toConnectData(topic, mapper.writeValueAsBytes(value));
        } catch (final IOException e) {
            throw new SandboxException("Could not re-serialise the sandbox response", e);
        } catch (final DataException e) {
            // JsonConverter rejecting the guest's output is a boundary failure, not a bad input
            // record. Letting DataException escape would route the ORIGINAL record to the DLQ and
            // carry on -- silently continuing past a guest that is emitting garbage.
            throw new SandboxException("Sandbox returned data Connect could not read: "
                + e.getMessage(), e);
        }
    }
}
