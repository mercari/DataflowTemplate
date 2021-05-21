package com.mercari.solution.module.transform;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.collect.Lists;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.ResourceUtil;
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
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class JavaScriptTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testStatelessRow() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFunc1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFunc2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);
        config.setParameters(parameters);

        final List<Row> rows = createRows();
        final Schema schema = rows.get(0).getSchema();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(rows))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<Row> outputJS1 = (FCollection<Row>) outputs1.get("js1");
        final Schema outputSchema = outputJS1.getSchema();

        Assert.assertEquals(Schema.FieldType.FLOAT, outputSchema.getField("outputFloatField").getType());
        Assert.assertEquals(Schema.FieldType.STRING, outputSchema.getField("outputStringField").getType());


        final PCollection<Row> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<Row> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> r.getDateTime("timestamp")));

            Assert.assertEquals(3, resultsList.size());

            Row row1 = resultsList.get(0);
            Assert.assertEquals(10F, row1.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("Hellostring1", row1.getString("outputStringField"));

            Row row2 = resultsList.get(1);
            Assert.assertEquals(20F, row2.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("Hellostring2", row2.getString("outputStringField"));

            Row row3 = resultsList.get(2);
            Assert.assertEquals(30F, row3.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("Hellostring3", row3.getString("outputStringField"));

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStatelessRecord() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFunc1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFunc2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject mappingStateless3 = new JsonObject();
        mappingStateless3.addProperty("function", "myFunc3");
        mappingStateless3.addProperty("outputType", "float64");
        mappingStateless3.addProperty("outputField", "outputDoubleField");
        mappings.add(mappingStateless3);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);

        final JsonArray groupFields = new JsonArray();
        groupFields.add("user");
        parameters.add("groupFields", groupFields);
        config.setParameters(parameters);

        final List<GenericRecord> records = createRecords();
        final org.apache.avro.Schema schema = records.get(0).getSchema();

        final PCollection<GenericRecord> inputRecords1 = pipeline
                .apply("CreateDummy1", Create.of(records).withCoder(AvroCoder.of(schema)))
                .apply("WithTimestamp", WithTimestamps.of((GenericRecord r) -> Instant.ofEpochMilli((long)(r.get("timestamp")) / 1000)));
        final FCollection<GenericRecord> fCollection1 = FCollection.of("rowInput", inputRecords1, DataType.AVRO, schema);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<GenericRecord> outputJS1 = (FCollection<GenericRecord>) outputs1.get("js1");
        final org.apache.avro.Schema outputSchema = outputJS1.getAvroSchema();

        Assert.assertEquals(org.apache.avro.Schema.Type.FLOAT, AvroSchemaUtil.unnestUnion(outputSchema.getField("outputFloatField").schema()).getType());
        Assert.assertEquals(org.apache.avro.Schema.Type.STRING, AvroSchemaUtil.unnestUnion(outputSchema.getField("outputStringField").schema()).getType());
        Assert.assertEquals(org.apache.avro.Schema.Type.DOUBLE, AvroSchemaUtil.unnestUnion(outputSchema.getField("outputDoubleField").schema()).getType());


        final PCollection<GenericRecord> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<GenericRecord> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> (Long)r.get("timestamp")));

            Assert.assertEquals(3, resultsList.size());

            GenericRecord row1 = resultsList.get(0);
            Assert.assertEquals(10F, (Float)row1.get("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring1", row1.get("outputStringField").toString());
            Assert.assertEquals((Float)row1.get("outputFloatField") * 2, (Double)row1.get("outputDoubleField"), DELTA);

            GenericRecord row2 = resultsList.get(1);
            Assert.assertEquals(20F, (Float)row2.get("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring2", row2.get("outputStringField").toString());
            Assert.assertEquals((Float)row2.get("outputFloatField") * 2, (Double)row2.get("outputDoubleField"), DELTA);

            GenericRecord row3 = resultsList.get(2);
            Assert.assertEquals(30F, (Float)row3.get("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring3", row3.get("outputStringField").toString());
            Assert.assertEquals((Float)row3.get("outputFloatField") * 2, (Double)row3.get("outputDoubleField"), DELTA);

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStatelessStruct() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFunc1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFunc2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);

        final JsonArray groupFields = new JsonArray();
        groupFields.add("user");
        parameters.add("groupFields", groupFields);
        config.setParameters(parameters);

        final List<Struct> structs = createStructs();
        final Type type = structs.get(0).getType();

        final PCollection<Struct> inputStructs1 = pipeline
                .apply("CreateDummy1", Create.of(structs))
                .apply("WithTimestamp", WithTimestamps.of((Struct r) -> Instant.ofEpochMilli(Timestamps.toMicros(r.getTimestamp("timestamp").toProto()))));
        final FCollection<Struct> fCollection1 = FCollection.of("rowInput", inputStructs1, DataType.STRUCT, type);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<Struct> outputJS1 = (FCollection<Struct>) outputs1.get("js1");
        final Type outputType = outputJS1.getSpannerType();

        Assert.assertEquals(Type.Code.FLOAT64, outputType.getStructFields().stream().filter(f -> f.getName().equals("outputFloatField")).findAny().get().getType().getCode());
        Assert.assertEquals(Type.Code.STRING, outputType.getStructFields().stream().filter(f -> f.getName().equals("outputStringField")).findAny().get().getType().getCode());


        final PCollection<Struct> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<Struct> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> r.getTimestamp("timestamp")));

            Assert.assertEquals(3, resultsList.size());

            Struct row1 = resultsList.get(0);
            Assert.assertEquals(10D, row1.getDouble("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring1", row1.getString("outputStringField"));

            Struct row2 = resultsList.get(1);
            Assert.assertEquals(20D, row2.getDouble("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring2", row2.getString("outputStringField"));

            Struct row3 = resultsList.get(2);
            Assert.assertEquals(30D, row3.getDouble("outputFloatField"), DELTA);
            Assert.assertEquals("Hellostring3", row3.getString("outputStringField"));

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStatelessEntity() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFunc1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFunc2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);

        final JsonArray groupFields = new JsonArray();
        groupFields.add("user");
        parameters.add("groupFields", groupFields);
        config.setParameters(parameters);

        final List<Entity> entities = createEntities();
        final Schema schema = createRows().get(0).getSchema();

        final PCollection<Entity> inputEntities1 = pipeline
                .apply("CreateDummy1", Create.of(entities))
                .apply("WithTimestamp", WithTimestamps.of((Entity r) -> Instant.ofEpochMilli(Timestamps.toMicros(r.getPropertiesOrThrow("timestamp").getTimestampValue()))));
        final FCollection<Entity> fCollection1 = FCollection.of("rowInput", inputEntities1, DataType.ENTITY, schema);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<Entity> outputJS1 = (FCollection<Entity>) outputs1.get("js1");


        final PCollection<Entity> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<Entity> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> r.getPropertiesOrThrow("timestamp").getTimestampValue().getSeconds()));

            Assert.assertEquals(3, resultsList.size());

            Entity row1 = resultsList.get(0);
            Assert.assertEquals(10D, row1.getPropertiesOrThrow("outputFloatField").getDoubleValue(), DELTA);
            Assert.assertEquals("Hellostring1", row1.getPropertiesOrThrow("outputStringField").getStringValue());

            Entity row2 = resultsList.get(1);
            Assert.assertEquals(20D, row2.getPropertiesOrThrow("outputFloatField").getDoubleValue(), DELTA);
            Assert.assertEquals("Hellostring2", row2.getPropertiesOrThrow("outputStringField").getStringValue());

            Entity row3 = resultsList.get(2);
            Assert.assertEquals(30D, row3.getPropertiesOrThrow("outputFloatField").getDoubleValue(), DELTA);
            Assert.assertEquals("Hellostring3", row3.getPropertiesOrThrow("outputStringField").getStringValue());

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStatefulRow() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFuncWithState1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFuncWithState2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);
        parameters.addProperty("stateUpdateFunction", "stateUpdateFunc");
        config.setParameters(parameters);

        final List<Row> rows = createRows();
        final Schema schema = rows.get(0).getSchema();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(rows))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<Row> outputJS1 = (FCollection<Row>) outputs1.get("js1");
        final Schema outputSchema = outputJS1.getSchema();

        Assert.assertEquals(Schema.FieldType.FLOAT, outputSchema.getField("outputFloatField").getType());
        Assert.assertEquals(Schema.FieldType.STRING, outputSchema.getField("outputStringField").getType());

        final PCollection<Row> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<Row> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> r.getDateTime("timestamp")));

            Assert.assertEquals(3, resultsList.size());

            Row row1 = resultsList.get(0);
            Assert.assertEquals(1F, row1.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("under1 string1", row1.getString("outputStringField"));

            Row row2 = resultsList.get(1);
            Assert.assertEquals(3F, row2.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("under1 string2", row2.getString("outputStringField"));

            Row row3 = resultsList.get(2);
            Assert.assertEquals(5F, row3.getFloat("outputFloatField").floatValue(), DELTA);
            Assert.assertEquals("over1 string3", row3.getString("outputStringField"));

            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStatefulRecord() throws IOException {

        final String script = ResourceUtil.getResourceFileAsString("module/transform/javascript/script.js");

        final TransformConfig config = new TransformConfig();
        config.setName("js1");
        config.setModule("javascript");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray mappings = new JsonArray();
        final JsonObject mappingStateless1 = new JsonObject();
        mappingStateless1.addProperty("function", "myFuncWithState1");
        mappingStateless1.addProperty("outputType", "float32");
        mappingStateless1.addProperty("outputField", "outputFloatField");
        mappings.add(mappingStateless1);

        final JsonObject mappingStateless2 = new JsonObject();
        mappingStateless2.addProperty("function", "myFuncWithState2");
        mappingStateless2.addProperty("outputType", "string");
        mappingStateless2.addProperty("outputField", "outputStringField");
        mappings.add(mappingStateless2);

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("script", script);
        parameters.add("mappings", mappings);
        parameters.addProperty("stateUpdateFunction", "stateUpdateFunc");
        config.setParameters(parameters);

        final JsonArray groupFields = new JsonArray();
        groupFields.add("user");
        parameters.add("groupFields", groupFields);
        config.setParameters(parameters);

        final List<GenericRecord> records = createRecords();
        final org.apache.avro.Schema schema = records.get(0).getSchema();

        final PCollection<GenericRecord> inputRecords1 = pipeline
                .apply("CreateDummy1", Create.of(records).withCoder(AvroCoder.of(schema)))
                .apply("WithTimestamp", WithTimestamps.of((GenericRecord r) -> Instant.ofEpochMilli((long)(r.get("timestamp")) / 1000)));
        final FCollection<GenericRecord> fCollection1 = FCollection.of("rowInput", inputRecords1, DataType.AVRO, schema);

        final Map<String, FCollection<?>> outputs1 = JavaScriptTransform.transform(Arrays.asList(fCollection1), config);
        final FCollection<GenericRecord> outputJS1 = (FCollection<GenericRecord>) outputs1.get("js1");
        final org.apache.avro.Schema outputSchema = outputJS1.getAvroSchema();

        Assert.assertEquals(org.apache.avro.Schema.Type.FLOAT, AvroSchemaUtil.unnestUnion(outputSchema.getField("outputFloatField").schema()).getType());
        Assert.assertEquals(org.apache.avro.Schema.Type.STRING, AvroSchemaUtil.unnestUnion(outputSchema.getField("outputStringField").schema()).getType());


        final PCollection<GenericRecord> outputPJS1 = outputJS1.getCollection();

        PAssert.that(outputPJS1).satisfies(results -> {
            final List<GenericRecord> resultsList = Lists.newArrayList(results);
            resultsList.sort(Comparator.comparing(r -> (Long)r.get("timestamp")));

            Assert.assertEquals(3, resultsList.size());

            GenericRecord row1 = resultsList.get(0);
            Assert.assertEquals(1F, (Float)row1.get("outputFloatField"), DELTA);
            Assert.assertEquals("under1 string1", row1.get("outputStringField").toString());

            GenericRecord row2 = resultsList.get(1);
            Assert.assertEquals(3F, (Float)row2.get("outputFloatField"), DELTA);
            Assert.assertEquals("under1 string2", row2.get("outputStringField").toString());

            GenericRecord row3 = resultsList.get(2);
            Assert.assertEquals(5F, (Float)row3.get("outputFloatField"), DELTA);
            Assert.assertEquals("over1 string3", row3.get("outputStringField").toString());

            return null;
        });

        pipeline.run();
    }

    private List<Row> createRows() {
        Schema schema = Schema.builder()
                .addField(Schema.Field.of("user", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("booleanValue", Schema.FieldType.BOOLEAN.withNullable(false)))
                .addField(Schema.Field.of("stringValue", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("intValue", Schema.FieldType.INT32.withNullable(false)))
                .addField(Schema.Field.of("longValue", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("floatValue", Schema.FieldType.FLOAT.withNullable(false)))
                .addField(Schema.Field.of("doubleValue", Schema.FieldType.DOUBLE.withNullable(false)))
                .addField(Schema.Field.of("dateValue", Schema.FieldType.logicalType(CalciteUtils.DATE.getLogicalType()).withNullable(false)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("user", "0001")
                .withFieldValue("booleanValue", true)
                .withFieldValue("stringValue", "string1")
                .withFieldValue("intValue", 1)
                .withFieldValue("longValue", 1L)
                .withFieldValue("floatValue", 1F)
                .withFieldValue("doubleValue", 1D)
                .withFieldValue("dateValue", LocalDate.of(2021, 4, 11))
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("user", "0001")
                .withFieldValue("booleanValue", false)
                .withFieldValue("stringValue", "string2")
                .withFieldValue("intValue", 2)
                .withFieldValue("longValue", 2L)
                .withFieldValue("floatValue", 2F)
                .withFieldValue("doubleValue", 2D)
                .withFieldValue("dateValue", LocalDate.of(2021, 4, 11))
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:20Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("user", "0001")
                .withFieldValue("booleanValue", true)
                .withFieldValue("stringValue", "string3")
                .withFieldValue("intValue", 3)
                .withFieldValue("longValue", 3L)
                .withFieldValue("floatValue", 3F)
                .withFieldValue("doubleValue", 3D)
                .withFieldValue("dateValue", LocalDate.of(2021, 4, 11))
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:40Z"))
                .build();

        return Arrays.asList(row1, row2, row3);
    }

    public static List<GenericRecord> createRecords() {

        final org.apache.avro.Schema schema = SchemaBuilder.record("root").fields()
                .name("user").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("booleanValue").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("stringValue").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("intValue").type(AvroSchemaUtil.NULLABLE_INT).noDefault()
                .name("longValue").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("floatValue").type(AvroSchemaUtil.NULLABLE_FLOAT).noDefault()
                .name("doubleValue").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .name("dateValue").type(AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE).noDefault()
                .name("timestamp").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
                .endRecord();

        final GenericRecord record1 = new GenericRecordBuilder(schema)
                .set("user", "0001")
                .set("booleanValue", true)
                .set("stringValue", "string1")
                .set("intValue", 1)
                .set("longValue", 1L)
                .set("floatValue", 1F)
                .set("doubleValue", 1D)
                .set("dateValue", Long.valueOf(LocalDate.of(2021, 4, 11).toEpochDay()).intValue())
                .set("timestamp", Instant.parse("2021-04-11T00:00:00Z").getMillis() * 1000)
                .build();
        final GenericRecord record2 = new GenericRecordBuilder(schema)
                .set("user", "0001")
                .set("booleanValue", true)
                .set("stringValue", "string2")
                .set("intValue", 2)
                .set("longValue", 2L)
                .set("floatValue", 2F)
                .set("doubleValue", 2D)
                .set("dateValue", Long.valueOf(LocalDate.of(2021, 4, 11).toEpochDay()).intValue())
                .set("timestamp", Instant.parse("2021-04-11T00:00:20Z").getMillis() * 1000)
                .build();
        final GenericRecord record3 = new GenericRecordBuilder(schema)
                .set("user", "0001")
                .set("booleanValue", true)
                .set("stringValue", "string3")
                .set("intValue", 3)
                .set("longValue", 3L)
                .set("floatValue", 3F)
                .set("doubleValue", 3D)
                .set("dateValue", Long.valueOf(LocalDate.of(2021, 4, 11).toEpochDay()).intValue())
                .set("timestamp", Instant.parse("2021-04-11T00:00:40Z").getMillis() * 1000)
                .build();

        return Arrays.asList(record1, record2, record3);
    }

    public static List<Struct> createStructs() {
        final Struct struct1 = Struct.newBuilder()
                .set("user").to("0001")
                .set("booleanValue").to(true)
                .set("stringValue").to("string1")
                .set("intValue").to(1)
                .set("longValue").to(1L)
                .set("floatValue").to(1F)
                .set("doubleValue").to(1D)
                .set("dateValue").to(Date.fromYearMonthDay(2021, 4, 11))
                .set("timestamp").to(Timestamp.parseTimestamp("2021-04-11T00:00:00Z"))
                .build();
        final Struct struct2 = Struct.newBuilder()
                .set("user").to("0001")
                .set("booleanValue").to(true)
                .set("stringValue").to("string2")
                .set("intValue").to(2)
                .set("longValue").to(2L)
                .set("floatValue").to(2F)
                .set("doubleValue").to(2D)
                .set("dateValue").to(Date.fromYearMonthDay(2021, 4, 11))
                .set("timestamp").to(Timestamp.parseTimestamp("2021-04-11T00:00:20Z"))
                .build();
        final Struct struct3 = Struct.newBuilder()
                .set("user").to("0001")
                .set("booleanValue").to(true)
                .set("stringValue").to("string3")
                .set("intValue").to(3)
                .set("longValue").to(3L)
                .set("floatValue").to(3F)
                .set("doubleValue").to(3D)
                .set("dateValue").to(Date.fromYearMonthDay(2021, 4, 11))
                .set("timestamp").to(Timestamp.parseTimestamp("2021-04-11T00:00:40Z"))
                .build();

        return Arrays.asList(struct1, struct2, struct3);
    }

    public static List<Entity> createEntities() {
        final Entity entity1 = Entity.newBuilder()
                .putProperties("user", Value.newBuilder().setStringValue("0001").build())
                .putProperties("booleanValue", Value.newBuilder().setBooleanValue(true).build())
                .putProperties("stringValue", Value.newBuilder().setStringValue("string1").build())
                .putProperties("intValue", Value.newBuilder().setIntegerValue(1).build())
                .putProperties("longValue", Value.newBuilder().setIntegerValue(1L).build())
                .putProperties("floatValue", Value.newBuilder().setDoubleValue(1F).build())
                .putProperties("doubleValue", Value.newBuilder().setDoubleValue(1D).build())
                .putProperties("timestamp", Value.newBuilder().setTimestampValue(Timestamp.parseTimestamp("2021-04-11T00:00:00Z").toProto()).build())
                .build();
        final Entity entity2 = Entity.newBuilder()
                .putProperties("user", Value.newBuilder().setStringValue("0001").build())
                .putProperties("booleanValue", Value.newBuilder().setBooleanValue(true).build())
                .putProperties("stringValue", Value.newBuilder().setStringValue("string2").build())
                .putProperties("intValue", Value.newBuilder().setIntegerValue(2).build())
                .putProperties("longValue", Value.newBuilder().setIntegerValue(2L).build())
                .putProperties("floatValue", Value.newBuilder().setDoubleValue(2F).build())
                .putProperties("doubleValue", Value.newBuilder().setDoubleValue(2D).build())
                .putProperties("timestamp", Value.newBuilder().setTimestampValue(Timestamp.parseTimestamp("2021-04-11T00:00:20Z").toProto()).build())
                .build();
        final Entity entity3 = Entity.newBuilder()
                .putProperties("user", Value.newBuilder().setStringValue("0001").build())
                .putProperties("booleanValue", Value.newBuilder().setBooleanValue(true).build())
                .putProperties("stringValue", Value.newBuilder().setStringValue("string3").build())
                .putProperties("intValue", Value.newBuilder().setIntegerValue(3).build())
                .putProperties("longValue", Value.newBuilder().setIntegerValue(3L).build())
                .putProperties("floatValue", Value.newBuilder().setDoubleValue(3F).build())
                .putProperties("doubleValue", Value.newBuilder().setDoubleValue(3D).build())
                .putProperties("timestamp", Value.newBuilder().setTimestampValue(Timestamp.parseTimestamp("2021-04-11T00:00:40Z").toProto()).build())
                .build();

        return Arrays.asList(entity1, entity2, entity3);
    }

}
