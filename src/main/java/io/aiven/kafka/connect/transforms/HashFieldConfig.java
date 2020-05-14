/*
 * Copyright 2019 Aiven Oy
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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;

class HashFieldConfig extends AbstractConfig {
    public static final String FIELD_NAME_CONFIG = "field.name";
    private static final String FIELD_NAME_DOC =
            "The name of the field which value should be hashed. If null or empty, "
                    + "the entire key or value is used (and assumed to be a string). By default is null.";
    public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
    private static final String SKIP_MISSING_OR_NULL_DOC =
            "In case the source of the new topic name is null or missing, "
                    + "should a record be silently passed without transformation.";
    public static final String FUNCTION_CONFIG = "function";
    private static final String FUNCTION_DOC =
            "The name of the hash function to use. The supported values are: md5, sha1, sha256.";
    private MessageDigest md = null;

    HashFieldConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(
                        FIELD_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        FIELD_NAME_DOC)
                .define(
                        SKIP_MISSING_OR_NULL_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        SKIP_MISSING_OR_NULL_DOC)
                .define(
                        FUNCTION_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.ValidString.in(
                                HashFunction.MD5.toString(),
                                HashFunction.SHA1.toString(),
                                HashFunction.SHA256.toString()),
                        ConfigDef.Importance.HIGH,
                        FUNCTION_DOC);
    }

    public enum HashFunction {
        MD5,
        SHA1 {
            public String toString() {
                return "SHA-1";
            }
        },
        SHA256 {
            public String toString() {
                return "SHA-256";
            }
        };
        static String stringValues = Stream.of(HashFunction.values())
                .map(Enum::toString)
                .collect(Collectors.joining("|", "[", "]"));
    }

    Optional<String> fieldName() {
        final String rawFieldName = getString(FIELD_NAME_CONFIG);
        if (null == rawFieldName || "".equals(rawFieldName)) {
            return Optional.empty();
        }
        return Optional.of(rawFieldName);
    }

    boolean skipMissingOrNull() {
        return getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
    }

    MessageDigest hashFunction() {
        final String hashAlg = getString(FUNCTION_CONFIG);
        try {
            if (md == null) {
                md = MessageDigest.getInstance(hashAlg);
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new DataException("Hash function " + hashAlg
                    + " must be "
                    + HashFieldConfig.HashFunction.stringValues);
        }
        return md;
    }
}

