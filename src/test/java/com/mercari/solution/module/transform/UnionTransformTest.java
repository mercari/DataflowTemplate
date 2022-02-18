package com.mercari.solution.module.transform;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;

public class UnionTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    // Row Test

    @Test
    public void testUnionSameSchemaRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final Schema schema1 = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Schema schema2 = Schema.builder()
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Schema schema3 = Schema.builder()
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema1)
                .withFieldValue("stringField", "a")
                .withFieldValue("longField", 1L)
                .withFieldValue("doubleField", 1.1)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[1])
                .withFieldValue("dateField", LocalDate.of(2021,1,1))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row2 = Row.withSchema(schema2)
                .withFieldValue("stringField", "b")
                .withFieldValue("longField", 2L)
                .withFieldValue("doubleField", 2.2)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[2])
                .withFieldValue("dateField", LocalDate.of(2022,2,2))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row3 = Row.withSchema(schema3)
                .withFieldValue("stringField", "c")
                .withFieldValue("longField", 3L)
                .withFieldValue("doubleField", 3.3)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[3])
                .withFieldValue("dateField", LocalDate.of(2023,3,3))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final PCollection<Row> input1 = pipeline
                .apply("CreateDummy1", Create.of(row1, row1))
                .setRowSchema(schema1);
        final PCollection<Row> input2 = pipeline
                .apply("CreateDummy2", Create.of(row2, row2))
                .setRowSchema(schema2);
        final PCollection<Row> input3 = pipeline
                .apply("CreateDummy3", Create.of(row3, row3))
                .setRowSchema(schema3);

        final FCollection<Row> fCollection1 = FCollection.of("input1", input1, DataType.ROW, schema1);
        final FCollection<Row> fCollection2 = FCollection.of("input2", input2, DataType.ROW, schema2);
        final FCollection<Row> fCollection3 = FCollection.of("input3", input3, DataType.ROW, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(1.1D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Row)row).getBoolean("booleanField")));
                    Assert.assertEquals(LocalDate.of(2021, 1, 1), Objects.requireNonNull(((Row)row).getLogicalTypeValue("dateField", LocalDate.class)));
                } else if("b".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Row)row).getBoolean("booleanField")));
                    Assert.assertEquals(LocalDate.of(2022, 2, 2), Objects.requireNonNull(((Row)row).getLogicalTypeValue("dateField", LocalDate.class)));
                } else {
                    Assert.assertEquals(3L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Row)row).getBoolean("booleanField")));
                    Assert.assertEquals(LocalDate.of(2023, 3, 3), Objects.requireNonNull(((Row)row).getLogicalTypeValue("dateField", LocalDate.class)));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final Schema schema1 = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .build();

        final Schema schema2 = Schema.builder()
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Schema schema3 = Schema.builder()
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema1)
                .withFieldValue("stringField", "a")
                .withFieldValue("longField", 1L)
                .withFieldValue("booleanField", true)
                .withFieldValue("dateField", LocalDate.of(2021,1,1))
                .build();

        final Row row2 = Row.withSchema(schema2)
                .withFieldValue("stringField", "b")
                .withFieldValue("doubleField", 2.2)
                .withFieldValue("dateField", LocalDate.of(2022,2,2))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row3 = Row.withSchema(schema3)
                .withFieldValue("stringField", "c")
                .withFieldValue("doubleField", 3.3)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[3])
                .build();

        final PCollection<Row> input1 = pipeline
                .apply("CreateDummy1", Create.of(row1, row1))
                .setRowSchema(schema1);
        final PCollection<Row> input2 = pipeline
                .apply("CreateDummy2", Create.of(row2, row2))
                .setRowSchema(schema2);
        final PCollection<Row> input3 = pipeline
                .apply("CreateDummy3", Create.of(row3, row3))
                .setRowSchema(schema3);

        final FCollection<Row> fCollection1 = FCollection.of("input1", input1, DataType.ROW, schema1);
        final FCollection<Row> fCollection2 = FCollection.of("input2", input2, DataType.ROW, schema2);
        final FCollection<Row> fCollection3 = FCollection.of("input3", input3, DataType.ROW, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Row)row).getString("stringField"))) {
                    Assert.assertNull(((Row)row).getValue("doubleField"));
                    Assert.assertNull(((Row)row).getValue("timestampField"));
                    Assert.assertEquals(1L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(true, ((Row)row).getBoolean("booleanField"));
                    Assert.assertEquals(LocalDate.of(2021,1,1), ((Row)row).getLogicalTypeValue("dateField", LocalDate.class));
                } else if("b".equals(((Row)row).getString("stringField"))) {
                    Assert.assertNull(((Row)row).getValue("longField"));
                    Assert.assertNull(((Row)row).getValue("booleanField"));
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(LocalDate.of(2022,2,2), ((Row)row).getLogicalTypeValue("dateField", LocalDate.class));
                } else {
                    Assert.assertNull(((Row)row).getValue("longField"));
                    Assert.assertNull(((Row)row).getValue("dateField"));
                    Assert.assertNull(((Row)row).getValue("timestampField"));
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, ((Row)row).getBoolean("booleanField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithBaseInputRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        config.setParameters(parameters);

        final Schema schema1 = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Schema schema2 = Schema.builder()
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Schema schema3 = Schema.builder()
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema1)
                .withFieldValue("stringField", "a")
                .withFieldValue("longField", 1L)
                .withFieldValue("doubleField", 1.1)
                .withFieldValue("booleanField", true)
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row2 = Row.withSchema(schema2)
                .withFieldValue("stringField", "b")
                .withFieldValue("longField", 2L)
                .withFieldValue("doubleField", 2.2)
                .withFieldValue("dateField", LocalDate.of(2021,3,3))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row3 = Row.withSchema(schema3)
                .withFieldValue("stringField", "c")
                .withFieldValue("doubleField", 3.3)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[3])
                .build();

        final PCollection<Row> input1 = pipeline
                .apply("CreateDummy1", Create.of(row1, row1))
                .setRowSchema(schema1);
        final PCollection<Row> input2 = pipeline
                .apply("CreateDummy2", Create.of(row2, row2))
                .setRowSchema(schema2);
        final PCollection<Row> input3 = pipeline
                .apply("CreateDummy3", Create.of(row3, row3))
                .setRowSchema(schema3);

        final FCollection<Row> fCollection1 = FCollection.of("input1", input1, DataType.ROW, schema1);
        final FCollection<Row> fCollection2 = FCollection.of("input2", input2, DataType.ROW, schema2);
        final FCollection<Row> fCollection3 = FCollection.of("input3", input3, DataType.ROW, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(5, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(1.1D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else if("b".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else {
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithMappingRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonArray mappings = new JsonArray();

        // For longField
        final JsonObject mapping1 = new JsonObject();
        mapping1.addProperty("outputField", "longField");
        final JsonArray mappingInputs1 = new JsonArray();

        final JsonObject mappingInput1 = new JsonObject();
        mappingInput1.addProperty("input", "input2");
        mappingInput1.addProperty("field", "longFieldB");
        mappingInputs1.add(mappingInput1);
        final JsonObject mappingInput2 = new JsonObject();
        mappingInput2.addProperty("input", "input3");
        mappingInput2.addProperty("field", "longFieldC");
        mappingInputs1.add(mappingInput2);

        mapping1.add("inputs", mappingInputs1);
        mappings.add(mapping1);

        // For doubleField
        final JsonObject mapping2 = new JsonObject();
        mapping2.addProperty("outputField", "doubleField");
        final JsonArray mappingInputs2 = new JsonArray();

        final JsonObject mappingInput3 = new JsonObject();
        mappingInput3.addProperty("input", "input2");
        mappingInput3.addProperty("field", "doubleFieldB");
        mappingInputs2.add(mappingInput3);
        final JsonObject mappingInput4 = new JsonObject();
        mappingInput4.addProperty("input", "input3");
        mappingInput4.addProperty("field", "doubleFieldC");
        mappingInputs2.add(mappingInput4);

        mapping2.add("inputs", mappingInputs2);
        mappings.add(mapping2);


        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        parameters.add("mappings", mappings);
        config.setParameters(parameters);

        final Schema schema1 = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("longField", Schema.FieldType.INT64.withNullable(true))
                .addField("doubleField", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Schema schema2 = Schema.builder()
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("longFieldB", Schema.FieldType.INT64.withNullable(true))
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("doubleFieldB", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .build();

        final Schema schema3 = Schema.builder()
                .addField("bytesField", Schema.FieldType.BYTES.withNullable(true))
                .addField("longFieldC", Schema.FieldType.INT64.withNullable(true))
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addField("doubleFieldC", Schema.FieldType.DOUBLE.withNullable(true))
                .addField("dateField", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(true))
                .addField("timestampField", Schema.FieldType.DATETIME.withNullable(true))
                .addField("booleanField", Schema.FieldType.BOOLEAN.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema1)
                .withFieldValue("stringField", "a")
                .withFieldValue("longField", 1L)
                .withFieldValue("doubleField", 1.1)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[1])
                .withFieldValue("dateField", LocalDate.of(2021,1,1))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row2 = Row.withSchema(schema2)
                .withFieldValue("stringField", "b")
                .withFieldValue("longFieldB", 2L)
                .withFieldValue("doubleFieldB", 2.2)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[2])
                .withFieldValue("dateField", LocalDate.of(2021,3,3))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final Row row3 = Row.withSchema(schema3)
                .withFieldValue("stringField", "c")
                .withFieldValue("longFieldC", 3L)
                .withFieldValue("doubleFieldC", 3.3)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", new byte[3])
                .withFieldValue("dateField", LocalDate.of(2021,3,3))
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();

        final PCollection<Row> input1 = pipeline
                .apply("CreateDummy1", Create.of(row1, row1))
                .setRowSchema(schema1);
        final PCollection<Row> input2 = pipeline
                .apply("CreateDummy2", Create.of(row2, row2))
                .setRowSchema(schema2);
        final PCollection<Row> input3 = pipeline
                .apply("CreateDummy3", Create.of(row3, row3))
                .setRowSchema(schema3);

        final FCollection<Row> fCollection1 = FCollection.of("input1", input1, DataType.ROW, schema1);
        final FCollection<Row> fCollection2 = FCollection.of("input2", input2, DataType.ROW, schema2);
        final FCollection<Row> fCollection3 = FCollection.of("input3", input3, DataType.ROW, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(1.1, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else if("b".equals(((Row)row).getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(2.2, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else {
                    Assert.assertEquals(3L, Objects.requireNonNull(((Row)row).getInt64("longField")).longValue());
                    Assert.assertEquals(3.3, Objects.requireNonNull(((Row)row).getDouble("doubleField")).doubleValue(), DELTA);
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }


    // Avro Test
    @Test
    public void testUnionSameSchemaAvro() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final org.apache.avro.Schema schema1 = SchemaBuilder.record("s1").fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema2 = SchemaBuilder.record("s2").fields()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema3 = SchemaBuilder.record("s3").fields()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .endRecord();

        final GenericRecord record1 = new GenericRecordBuilder(schema1)
                .set("stringField", "a")
                .set("bytesField", ByteBuffer.wrap(new byte[1]))
                .set("booleanField", true)
                .set("longField", 1L)
                .set("doubleField", 1.1D)
                .set("dateField", (int)LocalDate.of(2021, 1, 1).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2021-01-01T01:01:01Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record2 = new GenericRecordBuilder(schema2)
                .set("stringField", "b")
                .set("bytesField", ByteBuffer.wrap(new byte[2]))
                .set("booleanField", false)
                .set("longField", 2L)
                .set("doubleField", 2.2D)
                .set("dateField", (int)LocalDate.of(2022, 2, 2).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2022-02-02T02:02:02Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record3 = new GenericRecordBuilder(schema3)
                .set("stringField", "c")
                .set("bytesField", ByteBuffer.wrap(new byte[3]))
                .set("booleanField", true)
                .set("longField", 3L)
                .set("doubleField", 3.3D)
                .set("dateField", (int)LocalDate.of(2023, 3, 3).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2023-03-03T03:03:03Z").toEpochMilli() * 1000)
                .build();

        final PCollection<GenericRecord> input1 = pipeline
                .apply("CreateDummy1", Create.of(record1, record1).withCoder(AvroCoder.of(schema1)));
        final PCollection<GenericRecord> input2 = pipeline
                .apply("CreateDummy2", Create.of(record2, record2).withCoder(AvroCoder.of(schema2)));
        final PCollection<GenericRecord> input3 = pipeline
                .apply("CreateDummy3", Create.of(record3, record3).withCoder(AvroCoder.of(schema3)));

        final FCollection<GenericRecord> fCollection1 = FCollection.of("input1", input1, DataType.AVRO, schema1);
        final FCollection<GenericRecord> fCollection2 = FCollection.of("input2", input2, DataType.AVRO, schema2);
        final FCollection<GenericRecord> fCollection3 = FCollection.of("input3", input3, DataType.AVRO, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(records -> {
            int count = 0;
            for (var record : records) {
                count++;
                if("a".equals(((GenericRecord)record).get("stringField").toString())) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((GenericRecord)record).get("longField")));
                    Assert.assertEquals(1.1D, Objects.requireNonNull(((GenericRecord)record).get("doubleField")));
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)record).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2021,1,1).toEpochDay(), Objects.requireNonNull(((GenericRecord)record).get("dateField")));
                } else if("b".equals(((GenericRecord)record).get("stringField").toString())) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((GenericRecord)record).get("longField")));
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((GenericRecord)record).get("doubleField")));
                    Assert.assertEquals(false, Objects.requireNonNull(((GenericRecord)record).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2022,2,2).toEpochDay(), Objects.requireNonNull(((GenericRecord)record).get("dateField")));
                } else {
                    Assert.assertEquals(3L, Objects.requireNonNull(((GenericRecord)record).get("longField")));
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((GenericRecord)record).get("doubleField")));
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)record).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2023,3,3).toEpochDay(), Objects.requireNonNull(((GenericRecord)record).get("dateField")));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaAvro() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final org.apache.avro.Schema schema1 = SchemaBuilder.record("s1").fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema2 = SchemaBuilder.record("s2").fields()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema3 = SchemaBuilder.record("s3").fields()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .endRecord();

        final GenericRecord record1 = new GenericRecordBuilder(schema1)
                .set("stringField", "a")
                .set("booleanField", true)
                .set("longField", 1L)
                .set("dateField", (int)LocalDate.of(2021, 1, 1).toEpochDay())
                .build();

        final GenericRecord record2 = new GenericRecordBuilder(schema2)
                .set("stringField", "b")
                .set("doubleField", 2.2D)
                .set("dateField", (int)LocalDate.of(2022, 2, 2).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2022-02-02T02:02:02Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record3 = new GenericRecordBuilder(schema3)
                .set("stringField", "c")
                .set("bytesField", ByteBuffer.wrap(new byte[3]))
                .set("booleanField", true)
                .set("doubleField", 3.3D)
                .build();

        final PCollection<GenericRecord> input1 = pipeline
                .apply("CreateDummy1", Create.of(record1, record1).withCoder(AvroCoder.of(schema1)));
        final PCollection<GenericRecord> input2 = pipeline
                .apply("CreateDummy2", Create.of(record2, record2).withCoder(AvroCoder.of(schema2)));
        final PCollection<GenericRecord> input3 = pipeline
                .apply("CreateDummy3", Create.of(record3, record3).withCoder(AvroCoder.of(schema3)));

        final FCollection<GenericRecord> fCollection1 = FCollection.of("input1", input1, DataType.AVRO, schema1);
        final FCollection<GenericRecord> fCollection2 = FCollection.of("input2", input2, DataType.AVRO, schema2);
        final FCollection<GenericRecord> fCollection3 = FCollection.of("input3", input3, DataType.AVRO, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertNull(((GenericRecord)row).get("doubleField"));
                    Assert.assertNull(((GenericRecord)row).get("timestampField"));
                    Assert.assertEquals(1L, Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(true, ((GenericRecord)row).get("booleanField"));
                    Assert.assertEquals((int)LocalDate.of(2021,1,1).toEpochDay(), ((GenericRecord)row).get("dateField"));
                } else if("b".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertNull(((GenericRecord)row).get("longField"));
                    Assert.assertNull(((GenericRecord)row).get("booleanField"));
                    Assert.assertEquals(2.2D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals((int)LocalDate.of(2022,2,2).toEpochDay(), ((GenericRecord)row).get("dateField"));
                } else {
                    Assert.assertNull(((GenericRecord)row).get("longField"));
                    Assert.assertNull(((GenericRecord)row).get("dateField"));
                    Assert.assertNull(((GenericRecord)row).get("timestampField"));
                    Assert.assertEquals(3.3D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, ((GenericRecord)row).get("booleanField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithBaseInputAvro() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        config.setParameters(parameters);

        final org.apache.avro.Schema schema1 = SchemaBuilder.record("s1").fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema2 = SchemaBuilder.record("s2").fields()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema3 = SchemaBuilder.record("s1").fields()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .endRecord();

        final GenericRecord record1 = new GenericRecordBuilder(schema1)
                .set("stringField", "a")
                .set("booleanField", true)
                .set("longField", 1L)
                .set("doubleField", 1.1D)
                .set("timestampField", java.time.Instant.parse("2021-01-01T01:01:01Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record2 = new GenericRecordBuilder(schema2)
                .set("stringField", "b")
                .set("longField", 2L)
                .set("doubleField", 2.2D)
                .set("dateField", (int)LocalDate.of(2022, 2, 2).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2022-02-02T02:02:02Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record3 = new GenericRecordBuilder(schema3)
                .set("stringField", "c")
                .set("bytesField", ByteBuffer.wrap(new byte[3]))
                .set("booleanField", true)
                .set("doubleField", 3.3D)
                .build();

        final PCollection<GenericRecord> input1 = pipeline
                .apply("CreateDummy1", Create.of(record1, record1).withCoder(AvroCoder.of(schema1)));
        final PCollection<GenericRecord> input2 = pipeline
                .apply("CreateDummy2", Create.of(record2, record2).withCoder(AvroCoder.of(schema2)));
        final PCollection<GenericRecord> input3 = pipeline
                .apply("CreateDummy3", Create.of(record3, record3).withCoder(AvroCoder.of(schema3)));

        final FCollection<GenericRecord> fCollection1 = FCollection.of("input1", input1, DataType.AVRO, schema1);
        final FCollection<GenericRecord> fCollection2 = FCollection.of("input2", input2, DataType.AVRO, schema2);
        final FCollection<GenericRecord> fCollection3 = FCollection.of("input3", input3, DataType.AVRO, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(5, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertEquals(1L, (long)Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(1.1D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)row).get("booleanField")));
                    Assert.assertNull(((GenericRecord)row).get("dateField"));
                    Assert.assertNull(((GenericRecord)row).get("bytesField"));
                } else if("b".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertEquals(2L, (long)Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(2.2D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertNull(((GenericRecord)row).get("booleanField"));
                    Assert.assertNull(((GenericRecord)row).get("dateField"));
                    Assert.assertNull(((GenericRecord)row).get("bytesField"));
                } else {
                    Assert.assertNull(((GenericRecord)row).get("longField"));
                    Assert.assertEquals(3.3D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)row).get("booleanField")));
                    Assert.assertNull(((GenericRecord)row).get("dateField"));
                    Assert.assertNull(((GenericRecord)row).get("bytesField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithMappingAvro() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonArray mappings = new JsonArray();

        // For longField
        final JsonObject mapping1 = new JsonObject();
        mapping1.addProperty("outputField", "longField");
        final JsonArray mappingInputs1 = new JsonArray();

        final JsonObject mappingInput1 = new JsonObject();
        mappingInput1.addProperty("input", "input2");
        mappingInput1.addProperty("field", "longFieldB");
        mappingInputs1.add(mappingInput1);
        final JsonObject mappingInput2 = new JsonObject();
        mappingInput2.addProperty("input", "input3");
        mappingInput2.addProperty("field", "longFieldC");
        mappingInputs1.add(mappingInput2);

        mapping1.add("inputs", mappingInputs1);
        mappings.add(mapping1);

        // For doubleField
        final JsonObject mapping2 = new JsonObject();
        mapping2.addProperty("outputField", "doubleField");
        final JsonArray mappingInputs2 = new JsonArray();

        final JsonObject mappingInput3 = new JsonObject();
        mappingInput3.addProperty("input", "input2");
        mappingInput3.addProperty("field", "doubleFieldB");
        mappingInputs2.add(mappingInput3);
        final JsonObject mappingInput4 = new JsonObject();
        mappingInput4.addProperty("input", "input3");
        mappingInput4.addProperty("field", "doubleFieldC");
        mappingInputs2.add(mappingInput4);

        mapping2.add("inputs", mappingInputs2);
        mappings.add(mapping2);


        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        parameters.add("mappings", mappings);
        config.setParameters(parameters);

        final org.apache.avro.Schema schema1 = SchemaBuilder.record("s1").fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("longField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("doubleField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema2 = SchemaBuilder.record("s2").fields()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("longFieldB").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("doubleFieldB").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();

        final org.apache.avro.Schema schema3 = SchemaBuilder.record("s2").fields()
                .name("bytesField").type(AvroSchemaUtil.NULLABLE_BYTES).noDefault()
                .name("longFieldC").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("doubleFieldC").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("dateField").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestampField").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .name("booleanField").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .endRecord();

        final GenericRecord record1 = new GenericRecordBuilder(schema1)
                .set("stringField", "a")
                .set("bytesField", ByteBuffer.wrap(new byte[1]))
                .set("booleanField", true)
                .set("longField", 1L)
                .set("doubleField", 1.1D)
                .set("dateField", (int)LocalDate.of(2021, 1, 1).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2021-01-01T01:01:01Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record2 = new GenericRecordBuilder(schema2)
                .set("stringField", "b")
                .set("bytesField", ByteBuffer.wrap(new byte[2]))
                .set("booleanField", true)
                .set("longFieldB", 2L)
                .set("doubleFieldB", 2.2D)
                .set("dateField", (int)LocalDate.of(2022, 2, 2).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2022-02-02T02:02:02Z").toEpochMilli() * 1000)
                .build();

        final GenericRecord record3 = new GenericRecordBuilder(schema3)
                .set("stringField", "c")
                .set("bytesField", ByteBuffer.wrap(new byte[3]))
                .set("booleanField", true)
                .set("longFieldC", 3L)
                .set("doubleFieldC", 3.3D)
                .set("dateField", (int)LocalDate.of(2023, 3, 3).toEpochDay())
                .set("timestampField", java.time.Instant.parse("2023-03-03T03:03:03Z").toEpochMilli() * 1000)
                .build();

        final PCollection<GenericRecord> input1 = pipeline
                .apply("CreateDummy1", Create.of(record1, record1).withCoder(AvroCoder.of(schema1)));
        final PCollection<GenericRecord> input2 = pipeline
                .apply("CreateDummy2", Create.of(record2, record2).withCoder(AvroCoder.of(schema2)));
        final PCollection<GenericRecord> input3 = pipeline
                .apply("CreateDummy3", Create.of(record3, record3).withCoder(AvroCoder.of(schema3)));

        final FCollection<GenericRecord> fCollection1 = FCollection.of("input1", input1, DataType.AVRO, schema1);
        final FCollection<GenericRecord> fCollection2 = FCollection.of("input2", input2, DataType.AVRO, schema2);
        final FCollection<GenericRecord> fCollection3 = FCollection.of("input3", input3, DataType.AVRO, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(1.1D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)row).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2021,1,1).toEpochDay(), ((GenericRecord)row).get("dateField"));
                } else if("b".equals(((GenericRecord)row).get("stringField").toString())) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(2.2D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)row).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2022,2,2).toEpochDay(), ((GenericRecord)row).get("dateField"));
                } else {
                    Assert.assertEquals(3L, (long)Objects.requireNonNull(((GenericRecord)row).get("longField")));
                    Assert.assertEquals(3.3D, (double)Objects.requireNonNull(((GenericRecord)row).get("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((GenericRecord)row).get("booleanField")));
                    Assert.assertEquals((int)LocalDate.of(2023,3,3).toEpochDay(), ((GenericRecord)row).get("dateField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    // Struct Test
    @Test
    public void testUnionSameSchemaStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final Type schema1 = Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp())
        );

        final Type schema2 = Type.struct(
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("stringField", Type.string())
        );

        final Type schema3 = Type.struct(
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("booleanField", Type.bool())
        );

        final Struct struct1 = Struct.newBuilder()
                .set("stringField").to("a")
                .set("bytesField").to(ByteArray.copyFrom(new byte[1]))
                .set("booleanField").to(true)
                .set("longField").to(1L)
                .set("doubleField").to(1.1D)
                .set("dateField").to(Date.parseDate("2021-01-01"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2021-01-01T01:01:01Z").getMillis() * 1000))
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("stringField").to("b")
                .set("bytesField").to(ByteArray.copyFrom(new byte[2]))
                .set("booleanField").to(true)
                .set("longField").to(2L)
                .set("doubleField").to(2.2D)
                .set("dateField").to(Date.parseDate("2022-02-02"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2022-02-02T02:02:02Z").getMillis() * 1000))
                .build();

        final Struct struct3 = Struct.newBuilder()
                .set("stringField").to("c")
                .set("bytesField").to(ByteArray.copyFrom(new byte[3]))
                .set("booleanField").to(true)
                .set("longField").to(3L)
                .set("doubleField").to(3.3D)
                .set("dateField").to(Date.parseDate("2023-03-03"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2023-03-03T03:03:03Z").getMillis() * 1000))
                .build();

        final PCollection<Struct> input1 = pipeline
                .apply("CreateDummy1", Create.of(struct1, struct1));
        final PCollection<Struct> input2 = pipeline
                .apply("CreateDummy2", Create.of(struct2, struct2));
        final PCollection<Struct> input3 = pipeline
                .apply("CreateDummy3", Create.of(struct3, struct3));

        final FCollection<Struct> fCollection1 = FCollection.of("input1", input1, DataType.STRUCT, schema1);
        final FCollection<Struct> fCollection2 = FCollection.of("input2", input2, DataType.STRUCT, schema2);
        final FCollection<Struct> fCollection3 = FCollection.of("input3", input3, DataType.STRUCT, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(1.1D, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Struct)row).getBoolean("booleanField")));
                    Assert.assertEquals(Date.parseDate("2021-01-01"), Objects.requireNonNull(((Struct)row).getDate("dateField")));
                } else if("b".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Struct)row).getBoolean("booleanField")));
                    Assert.assertEquals(Date.parseDate("2022-02-02"), Objects.requireNonNull(((Struct)row).getDate("dateField")));
                } else {
                    Assert.assertEquals(3L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Struct)row).getBoolean("booleanField")));
                    Assert.assertEquals(Date.parseDate("2023-03-03"), Objects.requireNonNull(((Struct)row).getDate("dateField")));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        config.setParameters(parameters);

        final Type schema1 = Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("dateField", Type.date())
        );

        final Type schema2 = Type.struct(
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("stringField", Type.string())
        );

        final Type schema3 = Type.struct(
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("booleanField", Type.bool())
        );

        final Struct struct1 = Struct.newBuilder()
                .set("stringField").to("a")
                .set("booleanField").to(true)
                .set("longField").to(1L)
                .set("dateField").to(Date.parseDate("2021-01-01"))
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("stringField").to("b")
                .set("doubleField").to(2.2D)
                .set("dateField").to(Date.parseDate("2022-02-02"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2022-02-02T02:02:02Z").getMillis() * 1000))
                .build();

        final Struct struct3 = Struct.newBuilder()
                .set("stringField").to("c")
                .set("bytesField").to(ByteArray.copyFrom(new byte[3]))
                .set("booleanField").to(true)
                .set("doubleField").to(3.3D)
                .build();

        final PCollection<Struct> input1 = pipeline
                .apply("CreateDummy1", Create.of(struct1, struct1));
        final PCollection<Struct> input2 = pipeline
                .apply("CreateDummy2", Create.of(struct2, struct2));
        final PCollection<Struct> input3 = pipeline
                .apply("CreateDummy3", Create.of(struct3, struct3));

        final FCollection<Struct> fCollection1 = FCollection.of("input1", input1, DataType.STRUCT, schema1);
        final FCollection<Struct> fCollection2 = FCollection.of("input2", input2, DataType.STRUCT, schema2);
        final FCollection<Struct> fCollection3 = FCollection.of("input3", input3, DataType.STRUCT, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);

        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertTrue(((Struct)row).isNull("doubleField"));
                    Assert.assertTrue(((Struct)row).isNull("timestampField"));
                    Assert.assertEquals(1L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(true, ((Struct)row).getBoolean("booleanField"));
                    Assert.assertEquals(Date.parseDate("2021-01-01"), ((Struct)row).getDate("dateField"));
                } else if("b".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertTrue(((Struct)row).isNull("longField"));
                    Assert.assertTrue(((Struct)row).isNull("booleanField"));
                    Assert.assertEquals(2.2D, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(Date.parseDate("2022-02-02"), ((Struct)row).getDate("dateField"));
                } else {
                    Assert.assertTrue(((Struct)row).isNull("longField"));
                    Assert.assertTrue(((Struct)row).isNull("dateField"));
                    Assert.assertTrue(((Struct)row).isNull("timestampField"));
                    Assert.assertEquals(3.3D, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                    Assert.assertEquals(true, ((Struct)row).getBoolean("booleanField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithBaseInputStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        config.setParameters(parameters);

        final Type schema1 = Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("timestampField", Type.timestamp())
        );

        final Type schema2 = Type.struct(
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("stringField", Type.string())
        );

        final Type schema3 = Type.struct(
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("booleanField", Type.bool())
        );

        final Struct struct1 = Struct.newBuilder()
                .set("stringField").to("a")
                .set("booleanField").to(true)
                .set("longField").to(1L)
                .set("doubleField").to(1.1D)
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2021-01-01T01:01:01Z").getMillis() * 1000))
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("stringField").to("b")
                .set("longField").to(2L)
                .set("doubleField").to(2.2D)
                .set("dateField").to(Date.parseDate("2022-02-02"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2022-02-02T02:02:02Z").getMillis() * 1000))
                .build();

        final Struct struct3 = Struct.newBuilder()
                .set("stringField").to("c")
                .set("bytesField").to(ByteArray.copyFrom(new byte[3]))
                .set("booleanField").to(true)
                .set("doubleField").to(3.3D)
                .build();

        final PCollection<Struct> input1 = pipeline
                .apply("CreateDummy1", Create.of(struct1, struct1));
        final PCollection<Struct> input2 = pipeline
                .apply("CreateDummy2", Create.of(struct2, struct2));
        final PCollection<Struct> input3 = pipeline
                .apply("CreateDummy3", Create.of(struct3, struct3));

        final FCollection<Struct> fCollection1 = FCollection.of("input1", input1, DataType.STRUCT, schema1);
        final FCollection<Struct> fCollection2 = FCollection.of("input2", input2, DataType.STRUCT, schema2);
        final FCollection<Struct> fCollection3 = FCollection.of("input3", input3, DataType.STRUCT, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(5, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;

                if("a".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(1L, (long)Objects.requireNonNull(((Struct)row).getLong("longField")));
                    Assert.assertEquals(1.1D, (double)Objects.requireNonNull(((Struct)row).getDouble("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Struct)row).getBoolean("booleanField")));
                } else if("b".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(2L, (long)Objects.requireNonNull(((Struct)row).getLong("longField")));
                    Assert.assertEquals(2.2D, (double)Objects.requireNonNull(((Struct)row).getDouble("doubleField")), DELTA);
                    Assert.assertTrue(((Struct)row).isNull("booleanField"));
                } else {
                    Assert.assertTrue(((Struct)row).isNull("longField"));
                    Assert.assertEquals(3.3D, (double)Objects.requireNonNull(((Struct)row).getDouble("doubleField")), DELTA);
                    Assert.assertEquals(true, Objects.requireNonNull(((Struct)row).getBoolean("booleanField")));
                    Assert.assertTrue(((Struct)row).isNull("timestampField"));
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionDifferentSchemaWithMappingStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("union");
        config.setModule("union");
        config.setInputs(Arrays.asList("input1","input2","input3"));

        final JsonArray mappings = new JsonArray();

        // For longField
        final JsonObject mapping1 = new JsonObject();
        mapping1.addProperty("outputField", "longField");
        final JsonArray mappingInputs1 = new JsonArray();

        final JsonObject mappingInput1 = new JsonObject();
        mappingInput1.addProperty("input", "input2");
        mappingInput1.addProperty("field", "longFieldB");
        mappingInputs1.add(mappingInput1);
        final JsonObject mappingInput2 = new JsonObject();
        mappingInput2.addProperty("input", "input3");
        mappingInput2.addProperty("field", "longFieldC");
        mappingInputs1.add(mappingInput2);

        mapping1.add("inputs", mappingInputs1);
        mappings.add(mapping1);

        // For doubleField
        final JsonObject mapping2 = new JsonObject();
        mapping2.addProperty("outputField", "doubleField");
        final JsonArray mappingInputs2 = new JsonArray();

        final JsonObject mappingInput3 = new JsonObject();
        mappingInput3.addProperty("input", "input2");
        mappingInput3.addProperty("field", "doubleFieldB");
        mappingInputs2.add(mappingInput3);
        final JsonObject mappingInput4 = new JsonObject();
        mappingInput4.addProperty("input", "input3");
        mappingInput4.addProperty("field", "doubleFieldC");
        mappingInputs2.add(mappingInput4);

        mapping2.add("inputs", mappingInputs2);
        mappings.add(mapping2);


        final JsonObject parameters = new JsonObject();
        parameters.addProperty("baseInput", "input1");
        parameters.add("mappings", mappings);
        config.setParameters(parameters);

        final Type schema1 = Type.struct(
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("longField", Type.int64()),
                Type.StructField.of("doubleField", Type.float64()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp())
        );

        final Type schema2 = Type.struct(
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("longFieldB", Type.int64()),
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("booleanField", Type.bool()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("doubleFieldB", Type.float64()),
                Type.StructField.of("stringField", Type.string())
        );

        final Type schema3 = Type.struct(
                Type.StructField.of("bytesField", Type.bytes()),
                Type.StructField.of("longFieldC", Type.int64()),
                Type.StructField.of("stringField", Type.string()),
                Type.StructField.of("doubleFieldC", Type.float64()),
                Type.StructField.of("dateField", Type.date()),
                Type.StructField.of("timestampField", Type.timestamp()),
                Type.StructField.of("booleanField", Type.bool())
        );

        final Struct struct1 = Struct.newBuilder()
                .set("stringField").to("a")
                .set("bytesField").to(ByteArray.copyFrom(new byte[1]))
                .set("booleanField").to(true)
                .set("longField").to(1L)
                .set("doubleField").to(1.1D)
                .set("dateField").to(Date.parseDate("2021-01-01"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2021-01-01T01:01:01Z").getMillis() * 1000))
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("stringField").to("b")
                .set("bytesField").to(ByteArray.copyFrom(new byte[2]))
                .set("booleanField").to(true)
                .set("longFieldB").to(2L)
                .set("doubleFieldB").to(2.2D)
                .set("dateField").to(Date.parseDate("2022-02-02"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2022-02-02T02:02:02Z").getMillis() * 1000))
                .build();

        final Struct struct3 = Struct.newBuilder()
                .set("stringField").to("c")
                .set("bytesField").to(ByteArray.copyFrom(new byte[3]))
                .set("booleanField").to(true)
                .set("longFieldC").to(3L)
                .set("doubleFieldC").to(3.3D)
                .set("dateField").to(Date.parseDate("2023-03-03"))
                .set("timestampField").to(Timestamp.ofTimeMicroseconds(Instant.parse("2023-03-03T03:03:03Z").getMillis() * 1000))
                .build();

        final PCollection<Struct> input1 = pipeline
                .apply("CreateDummy1", Create.of(struct1, struct1));
        final PCollection<Struct> input2 = pipeline
                .apply("CreateDummy2", Create.of(struct2, struct2));
        final PCollection<Struct> input3 = pipeline
                .apply("CreateDummy3", Create.of(struct3, struct3));

        final FCollection<Struct> fCollection1 = FCollection.of("input1", input1, DataType.STRUCT, schema1);
        final FCollection<Struct> fCollection2 = FCollection.of("input2", input2, DataType.STRUCT, schema2);
        final FCollection<Struct> fCollection3 = FCollection.of("input3", input3, DataType.STRUCT, schema3);

        final FCollection<?> unified = UnionTransform.transform(Arrays.asList(fCollection1, fCollection2, fCollection3), config);
        final Schema unifiedSchema = unified.getSchema();
        Assert.assertEquals(7, unifiedSchema.getFieldCount());
        Assert.assertTrue(unifiedSchema.hasField("stringField"));
        Assert.assertTrue(unifiedSchema.hasField("longField"));
        Assert.assertTrue(unifiedSchema.hasField("doubleField"));
        Assert.assertTrue(unifiedSchema.hasField("booleanField"));
        Assert.assertTrue(unifiedSchema.hasField("dateField"));
        Assert.assertTrue(unifiedSchema.hasField("timestampField"));
        Assert.assertTrue(unifiedSchema.hasField("bytesField"));

        PAssert.that(unified.getCollection()).satisfies(rows -> {
            int count = 0;
            for (var row : rows) {
                count++;
                if("a".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(1.1, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else if("b".equals(((Struct)row).getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(2.2, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                } else {
                    Assert.assertEquals(3L, Objects.requireNonNull(((Struct)row).getLong("longField")).longValue());
                    Assert.assertEquals(3.3, Objects.requireNonNull(((Struct)row).getDouble("doubleField")).doubleValue(), DELTA);
                }
            }
            Assert.assertEquals(6, count);
            return null;
        });

        pipeline.run();
    }

}
