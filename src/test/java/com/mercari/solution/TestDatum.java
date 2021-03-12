package com.mercari.solution;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class TestDatum {

    public static Boolean getBooleanFieldValue() {
        return true;
    }

    public static String getStringFieldValue() {
        return "stringValue";
    }

    public static String getBytesFieldValue() {
        return "bytesValue";
    }

    public static Integer getIntFieldValue() {
        return 1;
    }

    public static Long getLongFieldValue() {
        return 1000000000L;
    }

    public static Float getFloatFieldValue() {
        return 0.125F;
    }

    public static Double getDoubleFieldValue() {
        return -10021.12345D;
    }

    public static LocalDate getDateFieldValue() {
        return LocalDate.of(2013, 2, 1);
    }

    public static LocalTime getTimeFieldValue() {
        return LocalTime.of(10,10,0);
    }

    public static Instant getTimestampFieldValue() {
        return Instant.parse("2013-02-01T00:00:00Z");
    }

    public static BigDecimal getDecimalFieldValue() {
        final int scale = AvroSchemaUtil.getLogicalTypeDecimal(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE).getScale();
        return BigDecimal.valueOf(1020125, scale);
    }

    public static List<Boolean> getBooleanArrayFieldValues() {
        return Arrays.asList(true, false, true);
    }

    public static List<String> getStringArrayFieldValues() {
        return Arrays.asList("stringValue1", "stringValue2", "stringValue3");
    }

    public static List<byte[]> getBytesArrayFieldValues() {
        return Arrays.asList(
                ByteArray.copyFrom("bytesValue1").toByteArray(),
                ByteArray.copyFrom("bytesValue2").toByteArray(),
                ByteArray.copyFrom("bytesValue3").toByteArray());
    }

    public static List<Integer> getIntArrayFieldValues() {
        return Arrays.asList(1, 2, 3);
    }

    public static List<Long> getLongArrayFieldValues() {
        return Arrays.asList(1000000L, 2000000L, 3000000L);
    }

    public static List<Float> getFloatArrayFieldValues() {
        return Arrays.asList(0.125F, 0.250F, -0.125F);
    }

    public static List<Double> getDoubleArrayFieldValues() {
        return Arrays.asList(-10021.12345D, -20021.12345D, -30021.12345D);
    }

    public static List<LocalDate> getDateArrayFieldValues() {
        return Arrays.asList(
                LocalDate.of(2013, 2, 1),
                LocalDate.of(2012, 1, 31),
                LocalDate.of(2010, 12, 15));
    }

    public static List<LocalTime> getTimeArrayFieldValues() {
        return Arrays.asList(
                LocalTime.of(0,0,0),
                LocalTime.of(12,9,9),
                LocalTime.of(23,59,59));
    }

    public static List<Instant> getTimestampArrayFieldValues() {
        return Arrays.asList(
                Instant.parse("2020-01-01T01:02:03.000Z"),
                Instant.parse("2012-06-15T10:10:10.100Z"),
                Instant.parse("1990-12-31T23:59:59.999Z"));
    }

    public static List<BigDecimal> getDecimalArrayFieldValues() {
        final int scale = AvroSchemaUtil.getLogicalTypeDecimal(AvroSchemaUtil.REQUIRED_LOGICAL_DECIMAL_TYPE).getScale();
        return Arrays.asList(
                BigDecimal.valueOf(1020125, scale),
                BigDecimal.valueOf(2020125, scale),
                BigDecimal.valueOf(3020125, scale));
    }

    //
    public static GenericRecord generateRecord() {
        final Schema schemaFlat = generateFlatFields("flatRecord").endRecord();
        final Schema schemaNest1 = generateFlatFields("nestRecord1")
                .name("recordField").type(Schema.createUnion(
                        schemaFlat, Schema.create(Schema.Type.NULL))).noDefault()
                .name("recordArrayField").type(Schema.createUnion(
                        Schema.createArray(schemaFlat), Schema.create(Schema.Type.NULL))).noDefault()
                .endRecord();
        final Schema schemaNest2 = generateFlatFields("nestRecord2")
                .name("recordField").type(Schema.createUnion(
                        schemaNest1, Schema.create(Schema.Type.NULL))).noDefault()
                .name("recordArrayField").type(Schema.createUnion(
                        Schema.createArray(schemaNest1), Schema.create(Schema.Type.NULL))).noDefault()
                .endRecord();

        final GenericRecord recordFlat = generateFlatRecord(schemaFlat).build();
        final GenericRecord recordNest1 = generateFlatRecord(schemaNest1)
                .set("recordField", recordFlat)
                .set("recordArrayField", Arrays.asList(recordFlat, recordFlat))
                .build();
        final GenericRecord recordNest2 = generateFlatRecord(schemaNest2)
                .set("recordField", recordNest1)
                .set("recordArrayField", Arrays.asList(recordNest1, recordNest1))
                .build();

        return recordNest2;
    }

    public static GenericRecord generateRecordNull() {
        final Schema schemaFlat = generateFlatFields("flatRecord").endRecord();
        final Schema schemaNest1 = generateFlatFields("nestRecord1")
                .name("recordField").type(Schema.createUnion(
                        schemaFlat, Schema.create(Schema.Type.NULL))).noDefault()
                .name("recordArrayField").type(Schema.createUnion(
                        Schema.createArray(schemaFlat), Schema.create(Schema.Type.NULL))).noDefault()
                .endRecord();
        final Schema schemaNest2 = generateFlatFields("nestRecord2")
                .name("recordField").type(Schema.createUnion(
                        schemaNest1, Schema.create(Schema.Type.NULL))).noDefault()
                .name("recordArrayField").type(Schema.createUnion(
                        Schema.createArray(schemaNest1), Schema.create(Schema.Type.NULL))).noDefault()
                .endRecord();

        final GenericRecord recordFlat = generateFlatRecordNull(schemaFlat).build();
        final GenericRecord recordNest1 = generateFlatRecordNull(schemaNest1)
                .set("recordField", recordFlat)
                .set("recordArrayField", Arrays.asList(recordFlat, recordFlat))
                .build();
        final GenericRecord recordNest2 = generateFlatRecordNull(schemaNest2)
                .set("recordField", recordNest1)
                .set("recordArrayField", Arrays.asList(recordNest1, recordNest1))
                .build();

        return recordNest2;
    }

    public static Row generateRow() {
        final org.apache.beam.sdk.schemas.Schema schemaFlat = generateFlatSchemaBuilder().build();
        final org.apache.beam.sdk.schemas.Schema schemaNest1 = generateFlatSchemaBuilder()
                .addField("recordField", org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaFlat))
                .addField("recordArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaFlat)))
                .build();
        final org.apache.beam.sdk.schemas.Schema schemaNest2 = generateFlatSchemaBuilder()
                .addField("recordField", org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaNest1))
                .addField("recordArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaNest1)))
                .build();

        final Row rowFlat = generateFlatRow(schemaFlat).build();
        final Row rowNest1 = generateFlatRow(schemaNest1)
                .withFieldValue("recordField", rowFlat)
                .withFieldValue("recordArrayField", Arrays.asList(rowFlat, rowFlat))
                .build();
        final Row rowNest2 = generateFlatRow(schemaNest2)
                .withFieldValue("recordField", rowNest1)
                .withFieldValue("recordArrayField", Arrays.asList(rowNest1, rowNest1))
                .build();

        return rowNest2;
    }

    public static Row generateRowNull() {
        final org.apache.beam.sdk.schemas.Schema schemaFlat = generateFlatSchemaBuilder().build();
        final org.apache.beam.sdk.schemas.Schema schemaNest1 = generateFlatSchemaBuilder()
                .addField("recordField", org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaFlat))
                .addField("recordArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaFlat)))
                .build();
        final org.apache.beam.sdk.schemas.Schema schemaNest2 = generateFlatSchemaBuilder()
                .addField("recordField", org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaNest1))
                .addField("recordArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.row(schemaNest1)))
                .build();

        final Row rowFlat = generateFlatRowNull(schemaFlat).build();
        final Row rowNest1 = generateFlatRowNull(schemaNest1)
                .withFieldValue("recordField", rowFlat)
                .withFieldValue("recordArrayField", Arrays.asList(rowFlat, rowFlat))
                .build();
        final Row rowNest2 = generateFlatRowNull(schemaNest2)
                .withFieldValue("recordField", rowNest1)
                .withFieldValue("recordArrayField", Arrays.asList(rowNest1, rowNest1))
                .build();

        return rowNest2;
    }

    public static Struct generateStruct() {
        final Struct structFlat = generateFlatStruct().build();
        final Struct structNest1 = generateFlatStruct()
                .set("recordField").to(structFlat)
                .set("recordArrayField").toStructArray(structFlat.getType(), Arrays.asList(structFlat, structFlat))
                .build();
        final Struct structNest2 = generateFlatStruct()
                .set("recordField").to(structNest1)
                .set("recordArrayField").toStructArray(structNest1.getType(), Arrays.asList(structNest1, structNest1))
                .build();

        return structNest2;
    }

    public static Struct generateStructNull() {
        final Struct structFlat = generateFlatStructNull().build();
        final Struct structNest1 = generateFlatStructNull()
                .set("recordField").to(structFlat)
                .set("recordArrayField").toStructArray(structFlat.getType(), Arrays.asList(structFlat, structFlat))
                .build();
        final Struct structNest2 = generateFlatStructNull()
                .set("recordField").to(structNest1)
                .set("recordArrayField").toStructArray(structNest1.getType(), Arrays.asList(structNest1, structNest1))
                .build();

        return structNest2;
    }

    public static Entity generateEntity() {
        final Entity entityFlat = generateFlatEntity().build();
        final Entity entityNest1 = generateFlatEntity()
                .putProperties("recordField", Value.newBuilder().setEntityValue(entityFlat).build())
                .putProperties("recordArrayField", Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder().addAllValues(
                                Arrays.asList(
                                        Value.newBuilder().setEntityValue(entityFlat).build(),
                                        Value.newBuilder().setEntityValue(entityFlat).build()))
                                .build())
                        .build())
                .build();
        final Entity entityNest2 = generateFlatEntity()
                .putProperties("recordField", Value.newBuilder().setEntityValue(entityNest1).build())
                .putProperties("recordArrayField", Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder().addAllValues(
                                Arrays.asList(
                                        Value.newBuilder().setEntityValue(entityNest1).build(),
                                        Value.newBuilder().setEntityValue(entityNest1).build()))
                                .build())
                        .build())
                .build();
        return entityNest2;
    }

    public static Entity generateEntityNull() {
        final Entity entityFlat = generateFlatEntityNull().build();
        final Entity entityNest1 = generateFlatEntityNull()
                .putProperties("recordField", Value.newBuilder().setEntityValue(entityFlat).build())
                .putProperties("recordArrayField", Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder().addAllValues(
                                Arrays.asList(
                                        Value.newBuilder().setEntityValue(entityFlat).build(),
                                        Value.newBuilder().setEntityValue(entityFlat).build()))
                                .build())
                        .build())
                .build();
        final Entity entityNest2 = generateFlatEntityNull()
                .putProperties("recordField", Value.newBuilder().setEntityValue(entityNest1).build())
                .putProperties("recordArrayField", Value.newBuilder()
                        .setArrayValue(ArrayValue.newBuilder().addAllValues(
                                Arrays.asList(
                                        Value.newBuilder().setEntityValue(entityNest1).build(),
                                        Value.newBuilder().setEntityValue(entityNest1).build()))
                                .build())
                        .build())
                .build();
        return entityNest2;
    }

    private static SchemaBuilder.FieldAssembler<Schema> generateFlatFields(final String name) {
        return SchemaBuilder.record(name).fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("intField").type(AvroSchemaUtil.NULLABLE_INT).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("floatField").type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timeField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MILLI_TYPE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("decimalField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE).noDefault()
                // Array
                .name("stringArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_STRING)).noDefault()
                .name("bytesArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_BYTES)).noDefault()
                .name("booleanArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_BOOLEAN)).noDefault()
                .name("intArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_INT)).noDefault()
                .name("longArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_LONG)).noDefault()
                .name("floatArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_FLOAT)).noDefault()
                .name("doubleArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_DOUBLE)).noDefault()
                .name("dateArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE)).noDefault()
                .name("timeArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MILLI_TYPE)).noDefault()
                .name("timestampArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)).noDefault()
                .name("decimalArrayField").type(Schema.createArray(AvroSchemaUtil.NULLABLE_LOGICAL_DECIMAL_TYPE)).noDefault();
    }

    private static org.apache.beam.sdk.schemas.Schema.Builder generateFlatSchemaBuilder() {
        return org.apache.beam.sdk.schemas.Schema.builder()
                .addField("stringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                .addField("bytesField", org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true))
                .addField("booleanField", org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("intField", org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(true))
                .addField("longField", org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true))
                .addField("floatField", org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT.withNullable(true))
                .addField("doubleField", org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE.withNullable(true))
                .addField("dateField", CalciteUtils.NULLABLE_DATE)
                .addField("timeField", CalciteUtils.NULLABLE_TIME)
                .addField("timestampField", org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                .addField("decimalField", org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL.withNullable(true))
                //
                .addField("stringArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true)))
                .addField("bytesArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.BYTES.withNullable(true)))
                .addField("booleanArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN.withNullable(true)))
                .addField("intArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.INT32.withNullable(true)))
                .addField("longArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.INT64.withNullable(true)))
                .addField("floatArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT.withNullable(true)))
                .addField("doubleArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE.withNullable(true)))
                .addField("dateArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        CalciteUtils.NULLABLE_DATE))
                .addField("timeArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        CalciteUtils.NULLABLE_TIME))
                .addField("timestampArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true)))
                .addField("decimalArrayField", org.apache.beam.sdk.schemas.Schema.FieldType.array(
                        org.apache.beam.sdk.schemas.Schema.FieldType.DECIMAL.withNullable(true)));
    }

    private static GenericRecordBuilder generateFlatRecord(final Schema schema) {
        return new GenericRecordBuilder(schema)
                .set("stringField", getStringFieldValue())
                .set("bytesField", ByteBuffer.wrap(ByteArray.copyFrom(getBytesFieldValue()).toByteArray()))
                .set("booleanField", getBooleanFieldValue())
                .set("intField", getIntFieldValue())
                .set("longField", getLongFieldValue())
                .set("floatField", getFloatFieldValue())
                .set("doubleField", getDoubleFieldValue())
                .set("dateField", (int)getDateFieldValue().toEpochDay())
                .set("timeField", getTimeFieldValue().toSecondOfDay() * 1000)
                .set("timestampField", getTimestampFieldValue().getMillis() * 1000)
                .set("decimalField", ByteBuffer.wrap(getDecimalFieldValue().unscaledValue().toByteArray()))
                // array
                .set("stringArrayField",getStringArrayFieldValues())
                .set("bytesArrayField", getBytesArrayFieldValues().stream()
                        .map(ByteBuffer::wrap)
                        .collect(Collectors.toList()))
                .set("booleanArrayField", getBooleanArrayFieldValues())
                .set("intArrayField", getIntArrayFieldValues())
                .set("longArrayField", getLongArrayFieldValues())
                .set("floatArrayField", getFloatArrayFieldValues())
                .set("doubleArrayField", getDoubleArrayFieldValues())
                .set("dateArrayField", getDateArrayFieldValues().stream()
                        .map(LocalDate::toEpochDay)
                        .map(Long::intValue)
                        .collect(Collectors.toList()))
                .set("timeArrayField", getTimeArrayFieldValues().stream()
                        .map(LocalTime::toSecondOfDay)
                        .map(v -> 1000 * v)
                        .collect(Collectors.toList()))
                .set("timestampArrayField", getTimestampArrayFieldValues().stream()
                        .map(Instant::getMillis)
                        .map(v -> 1000 * v)
                        .collect(Collectors.toList()))
                .set("decimalArrayField", getDecimalArrayFieldValues().stream()
                        .map(BigDecimal::unscaledValue)
                        .map(BigInteger::toByteArray)
                        .map(ByteBuffer::wrap)
                        .collect(Collectors.toList()));
    }

    private static GenericRecordBuilder generateFlatRecordNull(final Schema schema) {
        return new GenericRecordBuilder(schema)
                .set("stringField", null)
                .set("bytesField", null)
                .set("booleanField", null)
                .set("intField", null)
                .set("longField", null)
                .set("floatField", null)
                .set("doubleField", null)
                .set("dateField", null)
                .set("timeField", null)
                .set("timestampField", null)
                .set("decimalField", null)
                // array
                .set("stringArrayField", new ArrayList<>())
                .set("bytesArrayField", new ArrayList<>())
                .set("booleanArrayField", new ArrayList<>())
                .set("intArrayField", new ArrayList<>())
                .set("longArrayField", new ArrayList<>())
                .set("floatArrayField", new ArrayList<>())
                .set("doubleArrayField", new ArrayList<>())
                .set("dateArrayField", new ArrayList<>())
                .set("timeArrayField", new ArrayList<>())
                .set("timestampArrayField", new ArrayList<>())
                .set("decimalArrayField", new ArrayList<>());
    }

    private static Row.FieldValueBuilder generateFlatRow(final org.apache.beam.sdk.schemas.Schema schema) {
        return Row.withSchema(schema)
                .withFieldValue("stringField", getStringFieldValue())
                .withFieldValue("bytesField", ByteArray.copyFrom(getBytesFieldValue()).toByteArray())
                .withFieldValue("booleanField", getBooleanFieldValue())
                .withFieldValue("intField", getIntFieldValue())
                .withFieldValue("longField", getLongFieldValue())
                .withFieldValue("floatField", getFloatFieldValue())
                .withFieldValue("doubleField", getDoubleFieldValue())
                .withFieldValue("dateField", getDateFieldValue())
                .withFieldValue("timeField", getTimeFieldValue())
                .withFieldValue("timestampField", getTimestampFieldValue())
                .withFieldValue("decimalField", getDecimalFieldValue())
                // array
                .withFieldValue("stringArrayField",getStringArrayFieldValues())
                .withFieldValue("bytesArrayField", getBytesArrayFieldValues())
                .withFieldValue("booleanArrayField", getBooleanArrayFieldValues())
                .withFieldValue("intArrayField", getIntArrayFieldValues())
                .withFieldValue("longArrayField", getLongArrayFieldValues())
                .withFieldValue("floatArrayField", getFloatArrayFieldValues())
                .withFieldValue("doubleArrayField", getDoubleArrayFieldValues())
                .withFieldValue("dateArrayField", getDateArrayFieldValues())
                .withFieldValue("timeArrayField", getTimeArrayFieldValues())
                .withFieldValue("timestampArrayField", getTimestampArrayFieldValues())
                .withFieldValue("decimalArrayField", getDecimalArrayFieldValues());
    }

    private static Row.FieldValueBuilder generateFlatRowNull(final org.apache.beam.sdk.schemas.Schema schema) {
        return Row.withSchema(schema)
                .withFieldValue("stringField", null)
                .withFieldValue("bytesField", null)
                .withFieldValue("booleanField", null)
                .withFieldValue("intField", null)
                .withFieldValue("longField", null)
                .withFieldValue("floatField", null)
                .withFieldValue("doubleField", null)
                .withFieldValue("dateField", null)
                .withFieldValue("timeField", null)
                .withFieldValue("timestampField", null)
                .withFieldValue("decimalField", null)
                // array
                .withFieldValue("stringArrayField", new ArrayList<>())
                .withFieldValue("bytesArrayField", new ArrayList<>())
                .withFieldValue("booleanArrayField", new ArrayList<>())
                .withFieldValue("intArrayField", new ArrayList<>())
                .withFieldValue("longArrayField", new ArrayList<>())
                .withFieldValue("floatArrayField", new ArrayList<>())
                .withFieldValue("doubleArrayField", new ArrayList<>())
                .withFieldValue("dateArrayField", new ArrayList<>())
                .withFieldValue("timeArrayField", new ArrayList<>())
                .withFieldValue("timestampArrayField", new ArrayList<>())
                .withFieldValue("decimalArrayField", new ArrayList<>());
    }

    private static Struct.Builder generateFlatStruct() {
        return Struct.newBuilder()
                .set("stringField").to(getStringFieldValue())
                .set("bytesField").to(ByteArray.copyFrom(getBytesFieldValue()))
                .set("booleanField").to(getBooleanFieldValue())
                .set("intField").to(getIntFieldValue())
                .set("longField").to(getLongFieldValue())
                .set("floatField").to(getFloatFieldValue())
                .set("doubleField").to(getDoubleFieldValue())
                .set("dateField").to(Date.parseDate(getDateFieldValue().toString()))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(getTimestampFieldValue().getMillis() * 1000))
                .set("decimalField").to(getDecimalFieldValue())
                // array
                .set("stringArrayField").toStringArray(getStringArrayFieldValues())
                .set("bytesArrayField").toBytesArray(getBytesArrayFieldValues().stream()
                        .map(ByteArray::copyFrom)
                        .collect(Collectors.toList()))
                .set("booleanArrayField").toBoolArray(getBooleanArrayFieldValues())
                .set("longArrayField").toInt64Array(getLongArrayFieldValues())
                .set("doubleArrayField").toFloat64Array(getDoubleArrayFieldValues())
                .set("dateArrayField").toDateArray(getDateArrayFieldValues().stream()
                        .map(d -> Date.parseDate(d.toString()))
                        .collect(Collectors.toList()))
                .set("timestampArrayField").toTimestampArray(getTimestampArrayFieldValues().stream()
                        .map(t -> Timestamp.ofTimeMicroseconds(t.getMillis() * 1000))
                        .collect(Collectors.toList()))
                .set("decimalArrayField").toNumericArray(getDecimalArrayFieldValues());
    }

    private static Struct.Builder generateFlatStructNull() {
        return Struct.newBuilder()
                .set("stringField").to((String)null)
                .set("bytesField").to((ByteArray)null)
                .set("booleanField").to((Boolean)null)
                .set("longField").to((Long)null)
                .set("doubleField").to((Double)null)
                .set("dateField").to((Date)null)
                .set("timestampField").to((Timestamp)null)
                .set("decimalField").to((BigDecimal)null)
                // array
                .set("stringArrayField").toStringArray(null)
                .set("bytesArrayField").toBytesArray(null)
                .set("booleanArrayField").toBoolArray((List<Boolean>)null)
                .set("longArrayField").toInt64Array((List<Long>)null)
                .set("doubleArrayField").toFloat64Array((List<Double>)null)
                .set("dateArrayField").toDateArray(null)
                .set("timestampArrayField").toTimestampArray(null)
                .set("decimalArrayField").toNumericArray(null);
    }

    private static Entity.Builder generateFlatEntity() {
        return Entity.newBuilder()
                .putProperties("stringField", Value.newBuilder().setStringValue(getStringFieldValue()).build())
                .putProperties("bytesField", Value.newBuilder().setBlobValue(ByteString.copyFrom(ByteArray.copyFrom(getBytesFieldValue()).toByteArray())).build())
                .putProperties("booleanField", Value.newBuilder().setBooleanValue(getBooleanFieldValue()).build())
                .putProperties("longField", Value.newBuilder().setIntegerValue(getLongFieldValue()).build())
                .putProperties("doubleField", Value.newBuilder().setDoubleValue(getDoubleFieldValue()).build())
                .putProperties("dateField", Value.newBuilder().setStringValue(getDateFieldValue().toString()).build())
                .putProperties("timeField", Value.newBuilder().setStringValue(getTimeFieldValue().toString()).build())
                .putProperties("timestampField", Value.newBuilder().setTimestampValue(Timestamps.fromMillis(
                        getTimestampFieldValue().getMillis())).build())
                .putProperties("decimalField", Value.newBuilder().setStringValue(getDecimalFieldValue().toString()).build())
                // array
                .putProperties("stringArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getStringArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setStringValue(s).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("bytesArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getBytesArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setBlobValue(ByteString.copyFrom(s)).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("booleanArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getBooleanArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setBooleanValue(s).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("longArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getLongArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setIntegerValue(s).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("doubleArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getDoubleArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setDoubleValue(s).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("dateArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getDateArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("timeArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getTimeArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("timestampArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getTimestampArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setTimestampValue(Timestamps.fromMillis(
                                        getTimestampFieldValue().getMillis())).build())
                                .collect(Collectors.toList()))).build())
                .putProperties("decimalArrayField", Value.newBuilder().setArrayValue(ArrayValue.newBuilder()
                        .addAllValues(getDecimalArrayFieldValues().stream()
                                .map(s -> Value.newBuilder().setStringValue(s.toString()).build())
                                .collect(Collectors.toList()))).build());
    }

    private static Entity.Builder generateFlatEntityNull() {
        return Entity.newBuilder()
                .putProperties("stringField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("bytesField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("booleanField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("longField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("doubleField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("dateField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("timeField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("timestampField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("decimalField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                // array
                .putProperties("stringArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("bytesArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("booleanArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("longArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("doubleArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("dateArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("timeArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("timestampArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("decimalArrayField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());
    }

}
