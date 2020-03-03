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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TombstoneHandlerConfigTest {

    @Test
    final void failOnUnknownBehaviorName() {
        final Throwable t =
            assertThrows(
                ConfigException.class,
                () -> new TombstoneHandlerConfig(newBehaviorProps("asdasdsadas"))
            );
        assertEquals(
            "Invalid value asdasdsadas for configuration behavior: "
                + "Unsupported behavior name: asdasdsadas. Supported are: drop_silent,drop_warn,fail",
            t.getMessage()
        );
    }

    @Test
    final void acceptCorrectBehaviorNames() {

        TombstoneHandlerConfig c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    TombstoneHandlerConfig.Behavior.DROP_SILENT.name()
                )
            );
        assertEquals(TombstoneHandlerConfig.Behavior.DROP_SILENT, c.getBehavior());

        c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    TombstoneHandlerConfig.Behavior.FAIL.name().toLowerCase()
                )
            );
        assertEquals(TombstoneHandlerConfig.Behavior.FAIL, c.getBehavior());

        c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    "Drop_WArn"
                )
            );
        assertEquals(TombstoneHandlerConfig.Behavior.DROP_WARN, c.getBehavior());
    }

    @Test
    final void failOnEmptyBehaviorName() {
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new TombstoneHandlerConfig(newBehaviorProps(""))
        );
        assertEquals("Invalid value  for configuration behavior: String must be non-empty", t.getMessage());
    }

    private Map<String, String> newBehaviorProps(final String bv) {
        return ImmutableMap.of(TombstoneHandlerConfig.TOMBSTONE_HANDLER_BEHAVIOR, bv);
    }

}
