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

import org.apache.kafka.connect.data.SchemaAndValue;

/** What the guest said to do with one record. */
final class GuestResponse {

    private final boolean drop;
    private final String error;
    private final String topic;
    private final SchemaAndValue value;

    private GuestResponse(final boolean drop, final String error, final String topic,
                          final SchemaAndValue value) {
        this.drop = drop;
        this.error = error;
        this.topic = topic;
        this.value = value;
    }

    /** The record should be filtered out; {@code apply} returns null. */
    public static GuestResponse drop() {
        return new GuestResponse(true, null, null, null);
    }

    /**
     * The guest rejected this record; becomes a {@code DataException}.
     *
     * <p>What Connect does next depends on the connector. Under the default
     * {@code errors.tolerance=none} the task fails; under {@code all} the record is skipped. On a
     * sink task with a dead letter queue configured the record is captured there -- as it arrived,
     * before this transform ran. Source tasks have no dead letter queue at all.
     */
    public static GuestResponse error(final String message) {
        return new GuestResponse(false, message, null, null);
    }

    /** The guest produced a new value, and optionally a new destination topic. */
    public static GuestResponse transformed(final String topic, final SchemaAndValue value) {
        return new GuestResponse(false, null, topic, value);
    }

    public boolean isDrop() {
        return drop;
    }

    public boolean isError() {
        return error != null;
    }

    public String error() {
        return error;
    }

    /** New topic, or null to keep the record's current topic. */
    public String topic() {
        return topic;
    }

    public SchemaAndValue value() {
        return value;
    }
}
