package io.aiven.kafka.connect.transforms.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;
import java.util.Optional;

public class CursorField {

    private final String cursor;

    public CursorField(String cursor) {
        if(cursor == null) {
            throw new IllegalArgumentException("provided field must not be null");
        }
        this.cursor = cursor;
    }

    public String getCursor() {
        return cursor;
    }

    public Optional<Object> read(Map<?, ?> document) {
        return read(MAP_NAVIGATION, document, cursor);
    }

    public Optional<String> readAsString(Map<?, ?> document) {
        return read(document).map(Object::toString);
    }

    public Field read(Schema schema) {
        return read(SCHEMA_NAVIGATION, schema, cursor).orElse(null);
    }

    public Optional<Object> read(Struct struct) {
        return read(STRUCT_NAVIGATION, struct, cursor);
    }

    public Optional<String> readAsString(Struct struct) {
        return read(struct).map(Object::toString);
    }

    private <T, V> Optional<V> read(Navigation<T, V> navAlg, T navigable, String field) {
        int firstDot = field.indexOf('.');

        if(firstDot > 0) {
            String head = field.substring(0, firstDot);
            String tail = field.substring(firstDot + 1);
            return navAlg.diveInto(navigable, head).flatMap(next -> read(navAlg, next, tail));
        } else {
            return navAlg.getValue(navigable, field);
        }
    }

    private interface Navigation<T, V> {
        Optional<T> diveInto(T navigable, String field);

        Optional<V> getValue(T navigable, String field);
    }

    private static final Navigation<Schema, Field> SCHEMA_NAVIGATION = new Navigation<>() {

        @Override
        public Optional<Schema> diveInto(Schema navigable, String field) {
            return getValue(navigable, field).map(Field::schema);
        }

        @Override
        public Optional<Field> getValue(Schema navigable, String field) {
            return Optional.ofNullable(navigable.field(field));
        }
    };

    private static final Navigation<Struct, Object> STRUCT_NAVIGATION = new Navigation<>() {

        @Override
        public Optional<Struct> diveInto(Struct navigable, String field) {
            return Optional.ofNullable(navigable.getStruct(field));
        }

        @Override
        public Optional<Object> getValue(Struct navigable, String field) {
            return Optional.ofNullable(navigable.get(field));
        }
    };

    private static final Navigation<Map<?, ?>, Object> MAP_NAVIGATION = new Navigation<>() {
        @Override
        public Optional<Map<?, ?>> diveInto(Map<?, ?> navigable, String field) {
            var value = navigable.get(field);
            return value instanceof Map ? Optional.of((Map<?, ?>) value) : Optional.empty();
        }

        @Override
        public Optional<Object> getValue(Map<?, ?> navigable, String field) {
            return Optional.ofNullable(navigable.get(field));
        }
    };
}
