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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TombstoneHandlerConfigTest {

    @Test
    final void failOnUnknownBehaviorName() {
        assertThatThrownBy(() -> new TombstoneHandlerConfig(newBehaviorProps("asdasdsadas")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value asdasdsadas for configuration behavior: "
                + "Unsupported behavior name: asdasdsadas. Supported are: drop_silent,drop_warn,fail");
    }

    @Test
    final void acceptCorrectBehaviorNames() {

        TombstoneHandlerConfig c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    TombstoneHandlerConfig.Behavior.DROP_SILENT.name()
                )
            );
        assertThat(c.getBehavior()).isEqualTo(TombstoneHandlerConfig.Behavior.DROP_SILENT);

        c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    TombstoneHandlerConfig.Behavior.FAIL.name().toLowerCase()
                )
            );
        assertThat(c.getBehavior()).isEqualTo(TombstoneHandlerConfig.Behavior.FAIL);

        c =
            new TombstoneHandlerConfig(
                newBehaviorProps(
                    "Drop_WArn"
                )
            );
        assertThat(c.getBehavior()).isEqualTo(TombstoneHandlerConfig.Behavior.DROP_WARN);
    }

    @Test
    final void failOnEmptyBehaviorName() {
        assertThatThrownBy(() -> new TombstoneHandlerConfig(newBehaviorProps("")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration behavior: String must be non-empty");
    }

    private Map<String, String> newBehaviorProps(final String bv) {
        return ImmutableMap.of(TombstoneHandlerConfig.TOMBSTONE_HANDLER_BEHAVIOR, bv);
    }

}
