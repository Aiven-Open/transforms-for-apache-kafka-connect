/*
 * Copyright 2020 Aiven Oy
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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class DropValueIfHeaderSet<R extends ConnectRecord<R>> implements Transformation<R> {

    private DropValueIfHeaderSetConfig config;

    @Override
    public ConfigDef config() {
        return DropValueIfHeaderSetConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new DropValueIfHeaderSetConfig(configs);
    }

    @Override
    public R apply(final R record) {
        final var header = record.headers().lastWithName(config.headerKey());
        if (header != null && config.headerValue().equals(header.value())) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    null,
                    record.timestamp()
            );
        }

        return record;
    }

    @Override
    public void close() {

    }
}
