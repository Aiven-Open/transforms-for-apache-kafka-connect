/*
 * Copyright 2021 Aiven Oy
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class ConcatFieldsKeyTest extends ConcatFieldsTest {

    @Override
    protected String dataPlace() {
        return "key";
    }

    @Override
    protected ConcatFields<SinkRecord> createTransformationObject() {
        return new ConcatFields.Key<>();
    }

    @Override
    protected SinkRecord record(final Schema schema, final Object data) {
        return record(schema, data, null, null);
    }

    @Override
    protected SinkRecord setNewValue(final SinkRecord record, final Schema newValueSchema, final Object newValue) {
        return record.newRecord(record.topic(),
            record.kafkaPartition(),
            newValueSchema,
            newValue,
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            record.headers()
        );
    }
}
