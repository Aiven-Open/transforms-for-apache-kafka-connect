package io.aiven.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class StringToDateTimeTest {

    private StringToDateTime<SourceRecord> stringToDateTime;

    @Mock
    private SourceRecord mockRecord;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        stringToDateTime = new StringToDateTime<>();
    }

    @Test
    public void testApply() {
        Map<String, Object> props = new HashMap<>();
        props.put("field", "datetimeField");
        stringToDateTime.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("datetimeField", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema);
        String dateString = "2024-04-28T13:36:00";
        struct.put("datetimeField", dateString);

        when(mockRecord.valueSchema()).thenReturn(schema);
        when(mockRecord.value()).thenReturn(struct);

        SourceRecord transformedRecord = stringToDateTime.apply(mockRecord);

        Struct transformedStruct = (Struct) transformedRecord.value();
        assertEquals(transformedStruct.schema().field("datetimeField").schema(), org.apache.kafka.connect.data.Timestamp.SCHEMA);

        java.util.Date dateValue = (java.util.Date) transformedStruct.get("datetimeField");
        assertEquals(dateValue.toString(), "2024-04-28T13:36:00");
    }
}