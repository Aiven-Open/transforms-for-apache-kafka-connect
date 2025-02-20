/*
 * Copyright 2025 Aiven Oy
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestSourceConnector  extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTestSourceConnector.class);

    @Override
    public void start(final Map<String, String> props) {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.singletonList(Collections.emptyMap());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return null;
    }
}
