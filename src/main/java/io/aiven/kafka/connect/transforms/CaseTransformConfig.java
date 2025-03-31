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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Case transform configuration.
 *
 * <p>Configure the SMT to do case transform on configured fields.
 * Supported case transformations are transform to upper case and transform to lowercase.</p>
 */
public class CaseTransformConfig extends AbstractConfig {

    /**
     * A comma-separated list of fields to concatenate.
     */
    public static final String FIELD_NAMES_CONFIG = "field.names";
    private static final String FIELD_NAMES_DOC =
            "A comma-separated list of fields to concatenate.";
    /**
     * Set the case configuration, 'upper' or 'lower' are supported.
     */
    public static final String CASE_CONFIG = "case";
    private static final String CASE_DOC =
            "Set the case configuration, 'upper' or 'lower'.";

    CaseTransformConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(
                        FIELD_NAMES_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        FIELD_NAMES_DOC)
                .define(
                        CASE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        CASE_DOC);
    }

    final List<String> fieldNames() {
        return getList(FIELD_NAMES_CONFIG);
    }

    final Case transformCase() {
        return Objects.requireNonNull(Case.fromString(getString(CASE_CONFIG)));
    }

    /**
     * Case enumeration for supported transforms.
     */
    public enum Case {
        LOWER("lower"),
        UPPER("upper");

        private final String transformCase;

        Case(final String transformCase) {
            this.transformCase = transformCase;
        }

        /**
         * Return the case enumeration object resolved from the given parameter.
         * @param string The case enumeration to fetch.
         * @return
         */
        public static Case fromString(final String string) {
            for (final Case caseValue : values()) {
                if (caseValue.transformCase.equals(string)) {
                    return caseValue;
                }
            }
            throw new IllegalArgumentException(String.format("Unknown enum value %s", string));
        }
    }
}
