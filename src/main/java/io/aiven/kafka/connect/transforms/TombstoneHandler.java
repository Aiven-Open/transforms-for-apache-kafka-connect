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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TombstoneHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneHandler.class);

    private TombstoneHandlerConfig tombstoneHandlerConfig;

    @Override
    public ConfigDef config() {
        return TombstoneHandlerConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.tombstoneHandlerConfig = new TombstoneHandlerConfig(configs);
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null) {
            behaveOnNull(record);
            return null;
        } else {
            return record;
        }
    }

    private void behaveOnNull(final ConnectRecord<R> r) {

        switch (tombstoneHandlerConfig.getBehavior()) {
            case FAIL:
                throw new DataException(
                    String.format(
                        "Tombstone record encountered, failing due to configured '%s' behavior",
                        TombstoneHandlerConfig.Behavior.FAIL.name().toLowerCase()
                    )
                );
            case DROP_SILENT:
                LOGGER.debug(
                    "Tombstone record encountered, dropping due to configured '{}' behavior",
                    TombstoneHandlerConfig.Behavior.DROP_SILENT.name().toLowerCase()
                );
                break;
            case DROP_WARN:
                LOGGER.debug(
                    "Tombstone record encountered, dropping due to configured '{}' behavior",
                    TombstoneHandlerConfig.Behavior.DROP_WARN.name().toLowerCase()
                );
                break;
            default:
                throw new DataException(
                    String.format("Unknown behavior: %s", tombstoneHandlerConfig.getBehavior())
                );
        }
    }

    @Override
    public void close() {
    }

}
