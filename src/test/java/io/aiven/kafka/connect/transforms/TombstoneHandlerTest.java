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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.transforms.TombstoneHandlerConfig.Behavior;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TombstoneHandlerTest {

    @Test
    final void shouldDropTombstoneRecord() {
        assertNull(tombstoneHandler(Behavior.DROP_SILENT)
            .apply(record(null))
        );
    }

    @Test
    final void shouldThrowDataAccessExceptionOnTombstoneRecords() {
        final Throwable t = assertThrows(
            DataException.class,
            () -> tombstoneHandler(Behavior.FAIL)
                    .apply(record(null))
        );
        assertEquals(
            "Tombstone record encountered, failing due to configured 'fail' behavior",
            t.getMessage()
        );
    }

    private SinkRecord record(final Object value) {
        return new SinkRecord("some_topic", 0,
            null, null,
            null, value,
            123L,
            456L, TimestampType.CREATE_TIME);
    }

    private TombstoneHandler<SinkRecord> tombstoneHandler(final Behavior b) {
        final Map<String, String> props =
            ImmutableMap.of(
                TombstoneHandlerConfig.TOMBSTONE_HANDLER_BEHAVIOR,
                b.name()
            );
        final TombstoneHandler<SinkRecord> tombstoneHandler = new TombstoneHandler<>();
        tombstoneHandler.configure(props);
        return tombstoneHandler;
    }

}
