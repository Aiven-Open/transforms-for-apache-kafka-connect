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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyToValueTest {
    private static final Struct ABC_STRUCT = createStruct("a", 1, "b", 2, "c", 3);
    private static final Struct XYZ_STRUCT = createStruct("x", 100, "y", 200, "z", 300);
    private static final SinkRecord ABC_XYZ_RECORD = new SinkRecord("", 0, ABC_STRUCT.schema(), ABC_STRUCT,
            XYZ_STRUCT.schema(), XYZ_STRUCT, 0);
    private final KeyToValue<SinkRecord> k2v = new KeyToValue<>();

    /**
     * Build a test {@link Struct} from a list of field names and values, inferring the schema from the values.
     *
     * @param kvs A list of alternating keys and values (e.g. "a", 1, "b", 2).
     * @return A structure created from the keys and values, where the values are either int32, string, or another
     *         struct.
     */
    private static Struct createStruct(final Object... kvs) {
        SchemaBuilder sb = SchemaBuilder.struct();
        for (int i = 0; i < kvs.length; i += 2) {
            Schema fieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
            if (kvs[i + 1] instanceof Integer) {
                fieldSchema = Schema.OPTIONAL_INT32_SCHEMA;
            } else if (kvs[i + 1] instanceof Struct) {
                fieldSchema = ((Struct) kvs[i + 1]).schema();
            }
            sb = sb.field(kvs[i].toString(), fieldSchema);
        }

        final Schema schema = sb.build();
        final Struct struct = new Struct(schema);
        for (int i = 0; i < kvs.length; i += 2) {
            struct.put(kvs[i].toString(), kvs[i + 1]);
        }

        return struct;
    }

    @AfterEach
    public void teardown() {
        k2v.close();
    }

    @Test
    public void schemalessMapToSchemalessMap() {
        final Map<String, Integer> key = Map.of("a", 1, "b", 2, "c", 3);
        final Map<String, Integer> value = Map.of("x", 100, "y", 200, "z", 300);
        final SinkRecord input = new SinkRecord("", 0, null, key, null, value, 0);

        final Map<String, Integer> expectedValue = Map.of("x", 100, "y", 200, "z", 300, "a", 1, "b", 2);

        k2v.configure(Collections.singletonMap("key.fields", "a,b"));
        final SinkRecord output = k2v.apply(input);

        assertNull(output.keySchema());
        assertEquals(key, output.key());
        assertNull(output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemaStructToSchemaStruct() {
        final SinkRecord input = ABC_XYZ_RECORD;

        final Struct expectedValue = createStruct("x", 100, "y", 200, "z", 300, "a", 1, "b", 2);

        k2v.configure(Collections.singletonMap("key.fields", "a,b"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemaIntToSchemaStructWholeKey() {
        final int key = 123;
        final SinkRecord input = new SinkRecord("", 0, Schema.OPTIONAL_INT32_SCHEMA, key, XYZ_STRUCT.schema(),
                XYZ_STRUCT, 0);

        final Struct expectedValue = createStruct("x", 100, "y", 200, "z", 300, "_key", 123);

        k2v.configure(Collections.singletonMap("key.fields", "*"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemaStructToSchemaStructWithRenaming() {
        final SinkRecord input = ABC_XYZ_RECORD;

        final Struct expectedValue = createStruct("x", 1, "y", 2, "z", 300, "m", 1, "n", 2, "a", 1);

        k2v.configure(Map.of("key.fields", "a,b,a,b,a", "value.fields", "m,n,x,y"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemaIntToSchemaStructWholeKeyWithRenaming() {
        final int key = 123;
        final SinkRecord input = new SinkRecord("", 0, Schema.OPTIONAL_INT32_SCHEMA, key, XYZ_STRUCT.schema(),
                XYZ_STRUCT, 0);

        final Struct expectedValue = createStruct("x", 123, "y", 123, "z", 300, "m", 123, "n", 123, "_key", 123);

        k2v.configure(Map.of("key.fields", "*,*,*,*,*", "value.fields", "m,n,x,y"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemalessIntToSchemaStructWholeKey() {
        final int key = 123;
        final SinkRecord input = new SinkRecord("", 0, null, key, XYZ_STRUCT.schema(), XYZ_STRUCT, 0);

        final Struct expectedValue = createStruct("x", 100, "y", 200, "z", 300, "_key", 123);

        k2v.configure(Collections.singletonMap("key.fields", "*"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }

    @Test
    public void schemaStructToSchemaStructWholeKey() {
        final SinkRecord input = ABC_XYZ_RECORD;

        final Struct expectedValue = createStruct("x", 100, "y", 200, "z", 300, "_key", ABC_STRUCT);

        k2v.configure(Collections.singletonMap("key.fields", "*"));
        final SinkRecord output = k2v.apply(input);

        assertEquals(input.keySchema(), output.keySchema());
        assertEquals(input.key(), output.key());
        assertEquals(expectedValue.schema(), output.valueSchema());
        assertEquals(expectedValue, output.value());
    }


    @Test
    public void errorMissingFieldKey() {
        k2v.configure(Collections.singletonMap("key.fields", "a,doesnotexist"));
        final DataException ex = assertThrows(DataException.class, () -> k2v.apply(ABC_XYZ_RECORD));
        assertEquals("Key field 'doesnotexist' not found in key schema", ex.getMessage());
    }

    /**
     * Test trying to extract fields from a key when the key value doesn't support fields.
     */
    @Test
    public void errorNonExtractableKeys() {
        final int key = 123;
        final Struct value = XYZ_STRUCT;
        final SinkRecord input = new SinkRecord("", 0, Schema.OPTIONAL_INT32_SCHEMA, key, value.schema(), value, 0);

        k2v.configure(Collections.singletonMap("key.fields", "a, b"));
        final DataException ex = assertThrows(DataException.class, () -> k2v.apply(input));
        assertEquals("Named key fields [a, b] cannot be copied from the key schema: INT32", ex.getMessage());

        final SinkRecord input2 = new SinkRecord("", 0, null, key, value.schema(), value, 0);

        final DataException ex2 = assertThrows(DataException.class, () -> k2v.apply(input2));
        assertEquals("Named key fields [a, b] cannot be copied from the key class: java.lang.Integer",
                ex2.getMessage());
    }

    /**
     * Test trying to extract a whole schemaless key to a value that requires a schema. This is not allowed.
     */
    @Test
    public void errorCantDeduceKeySchema() {
        final Map<String, Object> key = Map.of("a", 1, "b", 2, "c", 3);
        final Struct value = XYZ_STRUCT;
        final SinkRecord input = new SinkRecord("", 0, null, key, value.schema(), value, 0);

        k2v.configure(Collections.singletonMap("key.fields", "*"));
        final DataException ex = assertThrows(DataException.class, () -> k2v.apply(input));
        assertEquals("The value requires a schema, but the key class is a schemaless Map", ex.getMessage());

        k2v.configure(Collections.singletonMap("key.fields", "a,*"));
        final DataException ex2 = assertThrows(DataException.class, () -> k2v.apply(input));
        assertEquals("The value requires a schema, but the key class is a schemaless Map", ex2.getMessage());

        // An unknown or unsupported primitive.
        k2v.configure(Collections.singletonMap("key.fields", "*"));
        final SinkRecord input3 = new SinkRecord("", 0, null, new Object(), value.schema(), value, 0);
        final DataException ex3 = assertThrows(DataException.class, () -> k2v.apply(input3));
        assertEquals("Cannot infer schema for unsupported key class: java.lang.Object", ex3.getMessage());
    }

    /**
     * Test trying to copy to a value that can't be appended to.
     */
    @Test
    public void errorCantAppendToValue() {
        final Struct key = ABC_STRUCT;
        final int value = 123;
        final SinkRecord input = new SinkRecord("", 0, key.schema(), key, null, value, 0);

        k2v.configure(Collections.singletonMap("key.fields", "a,b"));
        final DataException ex = assertThrows(DataException.class, () -> k2v.apply(input));
        assertEquals("The value needs to be a Struct or Map in order to append fields", ex.getMessage());

        final SinkRecord input2 = new SinkRecord("", 0, key.schema(), key, null, value, 0);

        final DataException ex2 = assertThrows(DataException.class, () -> k2v.apply(input2));
        assertEquals("The value needs to be a Struct or Map in order to append fields", ex2.getMessage());
    }

    @Test
    public void errorDuplicateValueMapping() {
        final DataException ex = assertThrows(DataException.class,
                () -> k2v.configure(Map.of("key.fields", "a,a", "value.fields", "x,x")));
        assertEquals("More than one key value is copied to the value field name 'x'", ex.getMessage());

        final DataException ex2 = assertThrows(DataException.class,
                () -> k2v.configure(Map.of("key.fields", "*,*,*,*", "value.fields", "a,b")));
        assertEquals("More than one key value is copied to the value field name '_key'", ex2.getMessage());
    }
}
