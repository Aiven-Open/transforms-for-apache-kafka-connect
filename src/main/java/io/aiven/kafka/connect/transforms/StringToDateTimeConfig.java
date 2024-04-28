package io.aiven.kafka.connect.transforms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
public final class StringToDateTimeConfig extends AbstractConfig {

    public static final String STRING_TO_DATETIME_HANDLER_BEHAVIOR = "behavior";
    public StringToDateTimeConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }
}
