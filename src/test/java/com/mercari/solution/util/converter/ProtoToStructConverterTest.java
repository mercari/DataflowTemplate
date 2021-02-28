package com.mercari.solution.util.converter;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.ProtoUtil;
import com.mercari.solution.util.ResourceUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class ProtoToStructConverterTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testToSchema() {
        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final Map<String, Descriptors.Descriptor> descriptors = ProtoUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);

        assertSchemaFields(type);

        final Type childType = getField(type, "child").getType();
        assertSchemaFields(childType);

        final Type grandchildType = getField(childType, "grandchild").getType();
        assertSchemaFields(grandchildType);
    }

    @Test
    public void testToStruct() throws Exception {
        testToStruct("data/test.pb");
    }

    @Test
    public void testToStructNull() throws Exception {
        testToStruct("data/test_null.pb");
    }

    private void testToStruct(final String protoPath) throws Exception {

        final byte[] descBytes = ResourceUtil.getResourceFileAsBytes("schema/test.desc");
        final byte[] protoBytes = ResourceUtil.getResourceFileAsBytes(protoPath);

        final Map<String, Descriptors.Descriptor> descriptors = ProtoUtil.getDescriptors(descBytes);
        final Descriptors.Descriptor descriptor = descriptors.get("com.mercari.solution.entity.TestMessage");
        final Type type = ProtoToStructConverter.convertSchema(descriptor);
        final DynamicMessage message = ProtoUtil.convert(descriptor, protoBytes);

        final JsonFormat.Printer printer = ProtoUtil.createJsonPrinter(descriptors);

        final Struct struct = ProtoToStructConverter.convert(type, descriptor, protoBytes, printer);
        assertStructValues(message, struct, printer);

        if(ProtoUtil.hasField(message, "child")) {
            final Struct child = (Struct) SpannerUtil.getValue(struct, "child");
            final DynamicMessage childMessage = (DynamicMessage) ProtoUtil.getFieldValue(message, "child");
            assertStructValues(childMessage, child, printer);

            final List<Struct> grandchildren = child.getStructList("grandchildren");
            final List<DynamicMessage> grandchildrenMessages = (List<DynamicMessage>)ProtoUtil.getFieldValue(childMessage, "grandchildren");
            int i = 0;
            for(final Struct s : grandchildren) {
                assertStructValues(grandchildrenMessages.get(i), s, printer);
                i++;
            }

            if(ProtoUtil.hasField(childMessage, "grandchild")) {
                final Struct grandchild = (Struct)SpannerUtil.getValue(child, "grandchild");
                final DynamicMessage grandchildMessage = (DynamicMessage) ProtoUtil.getFieldValue(childMessage, "grandchild");
                assertStructValues(grandchildMessage, grandchild, printer);
            }
        }

        final List<Struct> children = struct.getStructList("children");
        final List<DynamicMessage> childrenMessages = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "children");
        int i = 0;
        for(final Struct s : children) {
            assertStructValues(childrenMessages.get(i), s, printer);
            i++;
        }

    }

    private void assertSchemaFields(final Type type) {

        // Build-in types
        Assert.assertEquals(Type.Code.BOOL, getField(type, "boolValue").getType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "stringValue").getType().getCode());
        Assert.assertEquals(Type.Code.BYTES, getField(type, "bytesValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "intValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "longValue").getType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "floatValue").getType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "doubleValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "uintValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "ulongValue").getType().getCode());

        // Google provided types
        Assert.assertEquals(Type.Code.DATE, getField(type, "dateValue").getType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "timeValue").getType().getCode());
        Assert.assertEquals(Type.Code.TIMESTAMP, getField(type, "datetimeValue").getType().getCode());
        Assert.assertEquals(Type.Code.TIMESTAMP, getField(type, "timestampValue").getType().getCode());

        // Google provided types wrappedValues
        Assert.assertEquals(Type.Code.BOOL, getField(type, "wrappedBoolValue").getType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "wrappedStringValue").getType().getCode());
        Assert.assertEquals(Type.Code.BYTES, getField(type, "wrappedBytesValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedInt32Value").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedInt64Value").getType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedFloatValue").getType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedDoubleValue").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt32Value").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt64Value").getType().getCode());

        // Any
        Assert.assertEquals(Type.Code.STRING, getField(type, "anyValue").getType().getCode());

        // Enum
        Assert.assertEquals(Type.Code.STRING, getField(type, "enumValue").getType().getCode());

        // OneOf
        Assert.assertEquals(Type.Code.STRING, getField(type, "entityName").getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "entityAge").getType().getCode());

        // Map
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "strIntMapValue").getType().getCode());
        Assert.assertEquals(Type.Code.STRUCT, getField(type, "strIntMapValue").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "strIntMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "strIntMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        Assert.assertEquals(Type.Code.ARRAY, getField(type, "longDoubleMapValue").getType().getCode());
        Assert.assertEquals(Type.Code.STRUCT, getField(type, "longDoubleMapValue").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "longDoubleMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "longDoubleMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        //Assert.assertEquals(Type.Code.ARRAY, getField(type, "intChildMapValue").getType().getCode());
        //Assert.assertEquals(Type.Code.STRUCT, getField(type, "intChildMapValue").getType().getArrayElementType().getCode());
        //Assert.assertEquals(Type.Code.INT64, getField(type, "intChildMapValue").getType().getArrayElementType().getStructFields().get(0).getType().getCode());
        //Assert.assertEquals(Type.Code.STRUCT, getField(type, "intChildMapValue").getType().getArrayElementType().getStructFields().get(1).getType().getCode());

        // Repeated
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "boolValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "stringValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "bytesValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "intValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "longValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "floatValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "doubleValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "uintValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "ulongValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "dateValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "timeValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "datetimeValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "timestampValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedBoolValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedStringValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedBytesValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedInt32Values").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedInt64Values").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedUInt32Values").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedUInt64Values").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedFloatValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "wrappedDoubleValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "anyValues").getType().getCode());
        Assert.assertEquals(Type.Code.ARRAY, getField(type, "enumValues").getType().getCode());

        Assert.assertEquals(Type.Code.BOOL, getField(type, "boolValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "stringValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.BYTES, getField(type, "bytesValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "intValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "longValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "floatValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "doubleValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "uintValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "ulongValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.DATE, getField(type, "dateValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "timeValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.TIMESTAMP, getField(type, "datetimeValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.BOOL, getField(type, "wrappedBoolValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "wrappedStringValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.BYTES, getField(type, "wrappedBytesValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedInt32Values").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedInt64Values").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt32Values").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.INT64, getField(type, "wrappedUInt64Values").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedFloatValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.FLOAT64, getField(type, "wrappedDoubleValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "anyValues").getType().getArrayElementType().getCode());
        Assert.assertEquals(Type.Code.STRING, getField(type, "enumValues").getType().getArrayElementType().getCode());
    }

    private void assertStructValues(final DynamicMessage message, final Struct struct, final JsonFormat.Printer printer) throws InvalidProtocolBufferException {

        // Build-in type
        Assert.assertEquals(ProtoUtil.getValue(message, "boolValue", printer), struct.getBoolean("boolValue"));
        Assert.assertEquals(ProtoUtil.getValue(message, "stringValue", printer), struct.getString("stringValue"));
        Assert.assertEquals(
                new String((byte[])ProtoUtil.getValue(message, "bytesValue", printer), StandardCharsets.UTF_8),
                new String(struct.getBytes("bytesValue").toByteArray(), StandardCharsets.UTF_8));
        Assert.assertEquals(((Integer)ProtoUtil.getValue(message, "intValue", printer)).intValue(), struct.getLong("intValue"));
        Assert.assertEquals(ProtoUtil.getValue(message, "longValue", printer), struct.getLong("longValue"));
        Assert.assertEquals(((Float)ProtoUtil.getValue(message, "floatValue", printer)).doubleValue(), struct.getDouble("floatValue"), DELTA);
        Assert.assertEquals(ProtoUtil.getValue(message, "doubleValue", printer), struct.getDouble("doubleValue"));
        Assert.assertEquals(((Integer)ProtoUtil.getValue(message, "uintValue", printer)).longValue(), struct.getLong("uintValue"));
        Assert.assertEquals(ProtoUtil.getValue(message, "ulongValue", printer), struct.getLong("ulongValue"));

        // Google-provided type
        if(ProtoUtil.hasField(message, "dateValue")) {
            Assert.assertEquals(
                    ProtoUtil.getEpochDay(
                            (com.google.type.Date)(ProtoUtil.convertBuildInValue("google.type.Date",
                                    (DynamicMessage)ProtoUtil.getFieldValue(message, "dateValue")))),
                    SpannerUtil.getEpochDay(struct.getDate("dateValue")));
        } else {
            Assert.assertEquals(Date.fromYearMonthDay(1,1,1), struct.getDate("dateValue"));
        }

        if(ProtoUtil.hasField(message, "timeValue")) {
            var timeOfDay = ((com.google.type.TimeOfDay) (ProtoUtil.convertBuildInValue("google.type.TimeOfDay",
                    (DynamicMessage) ProtoUtil.getFieldValue(message, "timeValue"))));
            Assert.assertEquals(
                    String.format("%02d:%02d:%02d", timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds()),
                    (struct.getString("timeValue")));
        } else {
            Assert.assertEquals("00:00:00", struct.getString("timeValue"));
        }

        if(ProtoUtil.hasField(message, "datetimeValue")) {
            Assert.assertEquals(
                    ProtoUtil.getEpochMillis((com.google.type.DateTime) (ProtoUtil.convertBuildInValue("google.type.DateTime",
                            (DynamicMessage) ProtoUtil.getFieldValue(message, "datetimeValue")))),
                    (Timestamps.toMillis(struct.getTimestamp("datetimeValue").toProto())));
        } else {
            Assert.assertEquals(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"), struct.getTimestamp("datetimeValue"));
        }

        if(ProtoUtil.hasField(message, "timestampValue")) {
            Assert.assertEquals(
                    Timestamps.toMillis((com.google.protobuf.Timestamp) (ProtoUtil.convertBuildInValue("google.protobuf.Timestamp",
                            (DynamicMessage) ProtoUtil.getFieldValue(message, "timestampValue")))),
                    (Timestamps.toMillis(struct.getTimestamp("timestampValue").toProto())));
        } else {
            Assert.assertEquals(Timestamp.parseTimestamp("0001-01-01T00:00:00Z"), struct.getTimestamp("timestampValue"));
        }

        Assert.assertEquals(
                ProtoUtil.getValue(message, "wrappedBoolValue", printer),
                struct.getBoolean("wrappedBoolValue"));
        Assert.assertEquals(
                ProtoUtil.getValue(message, "wrappedStringValue", printer),
                struct.getString("wrappedStringValue"));
        Assert.assertEquals(
                new String((byte[])ProtoUtil.getValue(message, "wrappedBytesValue", printer), StandardCharsets.UTF_8),
                new String(struct.getBytes("wrappedBytesValue").toByteArray(), StandardCharsets.UTF_8));
        Assert.assertEquals(
                ((Integer)ProtoUtil.getValue(message, "wrappedInt32Value", printer)).longValue(),
                struct.getLong("wrappedInt32Value"));
        Assert.assertEquals(
                ProtoUtil.getValue(message, "wrappedInt64Value", printer),
                struct.getLong("wrappedInt64Value"));
        Assert.assertEquals(
                ((Float)ProtoUtil.getValue(message, "wrappedFloatValue", printer)).doubleValue(),
                struct.getDouble("wrappedFloatValue"), DELTA);
        Assert.assertEquals(
                ProtoUtil.getValue(message, "wrappedDoubleValue", printer),
                struct.getDouble("wrappedDoubleValue"));
        Assert.assertEquals(
                ((Integer)ProtoUtil.getValue(message, "wrappedUInt32Value", printer)).longValue(),
                struct.getLong("wrappedUInt32Value"));
        Assert.assertEquals(
                ProtoUtil.getValue(message, "wrappedUInt64Value", printer),
                struct.getLong("wrappedUInt64Value"));

        // Any
        if(ProtoUtil.hasField(message, "anyValue")) {
            Assert.assertEquals(printer.print((DynamicMessage)ProtoUtil.getFieldValue(message, "anyValue")), struct.getString("anyValue"));
        } else {
            Assert.assertEquals("", struct.getString("anyValue"));
        }

        // Enum
        if(ProtoUtil.hasField(message, "enumValue")) {
            Assert.assertEquals(((Descriptors.EnumValueDescriptor) ProtoUtil.getFieldValue(message, "enumValue")).getName(), struct.getString("enumValue"));
        } else {
            Assert.assertEquals(ProtoUtil.getField(message, "enumValue").getEnumType().getValues().get(0).getName(), struct.getString("enumValue"));
        }

        // OneOf
        final Object entityName = ProtoUtil.getFieldValue(message, "entityName");
        final Object entityAge  = ProtoUtil.getFieldValue(message, "entityAge");
        Assert.assertEquals(entityName == null ? "" : entityName, struct.getString("entityName"));
        Assert.assertEquals(entityAge == null ? 0L : ((Integer)ProtoUtil.getFieldValue(message, "entityAge")).longValue(), struct.getLong("entityAge"));

        // Map
        List<DynamicMessage> mapMessages = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "strIntMapValue");
        List<Struct> mapStructs = struct.getStructList("strIntMapValue");
        if(ProtoUtil.hasField(message, "strIntMapValue")) {
            Assert.assertEquals(mapMessages.size(), mapStructs.size());
            for(int i=0; i<mapMessages.size(); i++) {
                Assert.assertEquals(
                        ProtoUtil.getValue(mapMessages.get(i), "key", printer),
                        SpannerUtil.getValue(mapStructs.get(i), "key"));
                Assert.assertEquals(
                        ((Integer)ProtoUtil.getValue(mapMessages.get(i), "value", printer)).longValue(),
                        SpannerUtil.getValue(mapStructs.get(i), "value"));
            }
        } else {
            Assert.assertEquals(0, mapStructs.size());
        }

        mapStructs = struct.getStructList("longDoubleMapValue");
        if(ProtoUtil.hasField(message, "longDoubleMapValue")) {
            mapMessages = (List<DynamicMessage>) ProtoUtil.getFieldValue(message, "longDoubleMapValue");
            Assert.assertEquals(mapMessages.size(), mapStructs.size());
            for (int i = 0; i < mapMessages.size(); i++) {
                Assert.assertEquals(
                        ProtoUtil.getValue(mapMessages.get(i), "key", printer),
                        SpannerUtil.getValue(mapStructs.get(i), "key"));
                Assert.assertEquals(
                        ProtoUtil.getValue(mapMessages.get(i), "value", printer),
                        SpannerUtil.getValue(mapStructs.get(i), "value"));
            }
        } else {
            Assert.assertEquals(0, mapStructs.size());
        }

        // Repeated
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "boolValues")).orElse(new ArrayList<>()), struct.getBooleanList("boolValues"));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "stringValues")).orElse(new ArrayList<>()), struct.getStringList("stringValues"));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "intValues")).orElse(new ArrayList<>()), struct.getLongList("intValues").stream()
                .map(Long::intValue).collect(Collectors.toList()));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "longValues")).orElse(new ArrayList<>()), struct.getLongList("longValues"));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "floatValues")).orElse(new ArrayList<>()), struct.getDoubleList("floatValues").stream()
                .map(Double::floatValue).collect(Collectors.toList()));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "doubleValues")).orElse(new ArrayList<>()), struct.getDoubleList("doubleValues"));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "uintValues")).orElse(new ArrayList<>()), struct.getLongList("uintValues").stream()
                .map(Long::intValue).collect(Collectors.toList()));
        Assert.assertEquals(Optional.ofNullable(ProtoUtil.getFieldValue(message, "ulongValues")).orElse(new ArrayList<>()), struct.getLongList("ulongValues"));

        List<DynamicMessage> list = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "dateValues");
        int i = 0;
        if(ProtoUtil.hasField(message, "dateValues")) {
            Assert.assertEquals(list.size(), struct.getDateList("dateValues").size());

            for (var date : struct.getDateList("dateValues")) {
                Assert.assertEquals(
                        ProtoUtil.getEpochDay(
                                (com.google.type.Date) (ProtoUtil.convertBuildInValue("google.type.Date", list.get(i)))),
                        SpannerUtil.getEpochDay(date));
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getDateList("dateValues").size());
        }

        if(ProtoUtil.hasField(message, "timeValues")) {
            list = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "timeValues");
            Assert.assertEquals(list.size(), struct.getStringList("timeValues").size());
            i = 0;
            for (var localTime : struct.getStringList("timeValues")) {
                Assert.assertEquals(
                        ProtoUtil.getSecondOfDay(
                                (com.google.type.TimeOfDay) (ProtoUtil.convertBuildInValue("google.type.TimeOfDay", list.get(i)))),
                        LocalTime.parse(localTime).toSecondOfDay());
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getStringList("timeValues").size());
        }

        if(ProtoUtil.hasField(message, "datetimeValues")) {
            list = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "datetimeValues");
            Assert.assertEquals(list.size(), struct.getTimestampList("datetimeValues").size());
            i = 0;
            for (var instant : struct.getTimestampList("datetimeValues")) {
                Assert.assertEquals(
                        ProtoUtil.getEpochMillis(
                                (com.google.type.DateTime) (ProtoUtil.convertBuildInValue("google.type.DateTime", list.get(i)))),
                        Timestamps.toMillis(instant.toProto()));
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getTimestampList("datetimeValues").size());
        }

        if(ProtoUtil.hasField(message, "timestampValues")) {
            list = (List<DynamicMessage>)ProtoUtil.getFieldValue(message, "timestampValues");
            Assert.assertEquals(list.size(), struct.getTimestampList("timestampValues").size());
            i = 0;
            for (var instant : struct.getTimestampList("timestampValues")) {
                Assert.assertEquals(
                        Timestamps.toMillis(
                                (com.google.protobuf.Timestamp) (ProtoUtil.convertBuildInValue("google.protobuf.Timestamp", list.get(i)))),
                        Timestamps.toMillis(instant.toProto()));
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getTimestampList("timestampValues").size());
        }

        if(ProtoUtil.hasField(message, "anyValues")) {
            list = (List<DynamicMessage>) ProtoUtil.getFieldValue(message, "anyValues");
            Assert.assertEquals(list.size(), struct.getStringList("anyValues").size());
            i = 0;
            for (var json : struct.getStringList("anyValues")) {
                Assert.assertEquals(
                        printer.print(list.get(i)),
                        (json));
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getStringList("anyValues").size());
        }

        if(ProtoUtil.hasField(message, "enumValues")) {
            List<Descriptors.EnumValueDescriptor> enums = (List<Descriptors.EnumValueDescriptor>)ProtoUtil.getFieldValue(message, "enumValues");
            Assert.assertEquals(enums.size(), struct.getStringList("enumValues").size());
            i = 0;
            for(var e : struct.getStringList("enumValues")) {
                Assert.assertEquals(
                        enums.get(i).getName(),
                        e);
                i++;
            }
        } else {
            Assert.assertEquals(0, struct.getStringList("enumValues").size());
        }

    }

    private Type.StructField getField(Type type, final String field) {
        return type.getStructFields().stream()
                .filter(f -> f.getName().equals(field))
                .findAny()
                .orElse(null);
    }

}
