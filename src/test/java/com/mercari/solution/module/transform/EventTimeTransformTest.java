package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.TestDatum;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class EventTimeTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("eventTime1");
        config.setModule("eventtime");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("eventtimeField", "timestampField");
        parameters.addProperty("into", false);
        config.setParameters(parameters);

        final Row dummyRow = TestDatum.generateRow();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(dummyRow, dummyRow, dummyRow));
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, dummyRow.getSchema());

        // Test withField (set eventtime from field value)
        final Map<String, FCollection<?>> outputs1 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Row> outputRows1 = (PCollection<Row>) outputs1.get("eventTime1").getCollection();

        final Schema inputSchema = dummyRow.getSchema();
        final Schema outputSchema1 = outputs1.get("eventTime1").getSchema();
        Assert.assertEquals(inputSchema.getFieldCount(), outputSchema1.getFieldCount());
        for(final Schema.Field field : inputSchema.getFields()) {
            Assert.assertEquals(field.getName(), outputSchema1.getField(field.getName()).getName());
            Assert.assertEquals(field.getType(), outputSchema1.getField(field.getName()).getType());
        }

        PAssert.that(outputRows1).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        // Test toField (add field from eventtime)
        config.setName("eventTime2");
        parameters.addProperty("eventtimeField", "eventtimeField");
        parameters.addProperty("into", true);
        config.setParameters(parameters);

        final Map<String, FCollection<?>> outputs2 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Row> outputRows2 = (PCollection<Row>) outputs2.get("eventTime2").getCollection();

        final Schema outputSchema2 = outputs2.get("eventTime2").getSchema();
        Assert.assertEquals(inputSchema.getFieldCount(), outputSchema2.getFieldCount() - 1);
        Assert.assertTrue(outputSchema2.hasField("eventtimeField"));
        Assert.assertEquals(Schema.FieldType.DATETIME.getTypeName(), outputSchema2.getField("eventtimeField").getType().getTypeName());
        for(final Schema.Field field : inputSchema.getFields()) {
            Assert.assertEquals(field.getName(), outputSchema2.getField(field.getName()).getName());
            Assert.assertEquals(field.getType(), outputSchema2.getField(field.getName()).getType());
        }

        PAssert.that(outputRows2).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                Assert.assertEquals(Long.MIN_VALUE/1000, row.getDateTime("eventtimeField").toInstant().getMillis());
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRecord() {
        final TransformConfig config = new TransformConfig();
        config.setName("eventTime1");
        config.setModule("eventtime");
        config.setInputs(Arrays.asList("recordInput"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("eventtimeField", "timestampField");
        parameters.addProperty("into", false);
        config.setParameters(parameters);

        final GenericRecord dummyRecord = TestDatum.generateRecord();

        final PCollection<GenericRecord> inputRecords = pipeline
                .apply("CreateDummy", Create.of(dummyRecord, dummyRecord, dummyRecord).withCoder(AvroCoder.of(dummyRecord.getSchema())));
        final FCollection<GenericRecord> fCollection = FCollection.of("recordInput", inputRecords, DataType.AVRO, dummyRecord.getSchema());

        // Test withField (set eventtime from field value)
        final Map<String, FCollection<?>> outputs1 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<GenericRecord> outputRecords1 = (PCollection<GenericRecord>) outputs1.get("eventTime1").getCollection();

        final org.apache.avro.Schema inputSchema = dummyRecord.getSchema();
        final org.apache.avro.Schema outputSchema1 = outputs1.get("eventTime1").getAvroSchema();
        Assert.assertEquals(inputSchema.getFields().size(), outputSchema1.getFields().size());
        for(final org.apache.avro.Schema.Field field : inputSchema.getFields()) {
            Assert.assertEquals(field.name(), outputSchema1.getField(field.name()).name());
            Assert.assertEquals(field.schema().getType(), outputSchema1.getField(field.name()).schema().getType());
        }

        PAssert.that(outputRecords1).satisfies(records -> {
            int count = 0;
            for (final GenericRecord record : records) {
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        // Test toField (add field from eventtime)
        config.setName("eventTime2");
        parameters.addProperty("eventtimeField", "eventtimeField");
        parameters.addProperty("into", true);
        config.setParameters(parameters);

        final Map<String, FCollection<?>> outputs2 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<GenericRecord> outputRecords2 = (PCollection<GenericRecord>) outputs2.get("eventTime2").getCollection();

        final org.apache.avro.Schema outputSchema2 = outputs2.get("eventTime2").getAvroSchema();
        Assert.assertEquals(inputSchema.getFields().size(), outputSchema2.getFields().size() - 1);
        Assert.assertNotNull(outputSchema2.getField("eventtimeField"));
        Assert.assertEquals(AvroSchemaUtil.REQUIRED_LOGICAL_TIMESTAMP_MICRO_TYPE.getType(),
                outputSchema2.getField("eventtimeField").schema().getType());
        for(final org.apache.avro.Schema.Field field : inputSchema.getFields()) {
            Assert.assertNotNull(outputSchema2.getField(field.name()));
            Assert.assertEquals(field.schema().getType(), outputSchema2.getField(field.name()).schema().getType());
        }

        PAssert.that(outputRecords2).satisfies(records -> {
            int count = 0;
            for (final GenericRecord record : records) {
                Assert.assertEquals(Long.MIN_VALUE/1000 * 1000, record.get("eventtimeField"));
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("eventTime1");
        config.setModule("eventtime");
        config.setInputs(Arrays.asList("structInput"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("eventtimeField", "timestampField");
        parameters.addProperty("into", false);
        config.setParameters(parameters);

        final Struct dummyStruct = TestDatum.generateStruct();

        final PCollection<Struct> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyStruct, dummyStruct, dummyStruct));
        final FCollection<Struct> fCollection = FCollection.of("structInput", inputStructs, DataType.STRUCT, dummyStruct.getType());

        // Test withField (set eventtime from field value)
        final Map<String, FCollection<?>> outputs1 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Struct> outputStructs1 = (PCollection<Struct>) outputs1.get("eventTime1").getCollection();

        final Type inputType = dummyStruct.getType();
        final Type outputType1 = outputs1.get("eventTime1").getSpannerType();
        Assert.assertEquals(inputType.getStructFields().size(), outputType1.getStructFields().size());
        for(final Type.StructField field : inputType.getStructFields()) {
            Assert.assertTrue(outputType1.getStructFields().stream().anyMatch(f -> f.getName().equals(field.getName())));
            Assert.assertEquals(field.getType().getCode(),
                    getStructField(outputType1, field.getName()).getType().getCode());
        }

        PAssert.that(outputStructs1).satisfies(structs -> {
            int count = 0;
            for (final Struct struct : structs) {
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        // Test toField (add field from eventtime)
        config.setName("eventTime2");
        parameters.addProperty("eventtimeField", "eventtimeField");
        parameters.addProperty("into", true);
        config.setParameters(parameters);

        final Map<String, FCollection<?>> outputs2 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Struct> outputStructs2 = (PCollection<Struct>) outputs2.get("eventTime2").getCollection();

        final Type outputType2 = outputs2.get("eventTime2").getSpannerType();
        Assert.assertEquals(inputType.getStructFields().size(), outputType2.getStructFields().size() - 1);
        Assert.assertTrue(outputType2.getStructFields().stream().anyMatch(f -> f.getName().equals("eventtimeField")));
        Assert.assertEquals(Type.Code.TIMESTAMP,
                getStructField(outputType2, "eventtimeField").getType().getCode());
        for(final Type.StructField field : inputType.getStructFields()) {
            Assert.assertTrue(outputType2.getStructFields().stream().anyMatch(f -> f.getName().equals(field.getName())));
            Assert.assertEquals(field.getType().getCode(), getStructField(outputType2, field.getName()).getType().getCode());
        }

        PAssert.that(outputStructs2).satisfies(structs -> {
            int count = 0;
            for (final Struct struct : structs) {
                Assert.assertEquals(-9223372036855L, Timestamps.toMicros(struct.getTimestamp("eventtimeField").toProto()));
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testEntity() {
        final TransformConfig config = new TransformConfig();
        config.setName("eventTime1");
        config.setModule("eventtime");
        config.setInputs(Arrays.asList("entityInput"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("eventtimeField", "timestampField");
        parameters.addProperty("into", false);
        config.setParameters(parameters);

        final Entity dummyEntity = TestDatum.generateEntity();
        final Schema inputSchema = TestDatum.generateRow().getSchema();

        final PCollection<Entity> inputEntities = pipeline
                .apply("CreateDummy", Create.of(dummyEntity, dummyEntity, dummyEntity));
        final FCollection<Entity> fCollection = FCollection.of("entityInput", inputEntities, DataType.ENTITY, inputSchema);

        // Test withField (set eventtime from field value)
        final Map<String, FCollection<?>> outputs1 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Entity> outputEntities1 = (PCollection<Entity>) outputs1.get("eventTime1").getCollection();

        final Schema outputSchema1 = outputs1.get("eventTime1").getSchema();
        Assert.assertEquals(inputSchema.getFieldCount(), outputSchema1.getFieldCount());
        for(final Schema.Field field : inputSchema.getFields()) {
            Assert.assertNotNull(outputSchema1.getField(field.getName()));
            Assert.assertEquals(field.getType().getTypeName(), outputSchema1.getField(field.getName()).getType().getTypeName());
        }

        PAssert.that(outputEntities1).satisfies(entities -> {
            int count = 0;
            for (final Entity entity : entities) {
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        // Test toField (add field from eventtime)
        config.setName("eventTime2");
        parameters.addProperty("eventtimeField", "eventtimeField");
        parameters.addProperty("into", true);
        config.setParameters(parameters);

        final Map<String, FCollection<?>> outputs2 = EventTimeTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Entity> outputEntities2 = (PCollection<Entity>) outputs2.get("eventTime2").getCollection();

        final Schema outputSchema2 = outputs2.get("eventTime2").getSchema();
        Assert.assertEquals(inputSchema.getFieldCount(), outputSchema2.getFieldCount() - 1);
        Assert.assertNotNull(outputSchema2.getField("eventtimeField"));
        Assert.assertEquals(Schema.FieldType.DATETIME.getTypeName(), outputSchema2.getField("eventtimeField").getType().getTypeName());
        for(final Schema.Field field : inputSchema.getFields()) {
            Assert.assertNotNull(outputSchema2.getField(field.getName()));
            Assert.assertEquals(field.getType().getTypeName(), outputSchema2.getField(field.getName()).getType().getTypeName());
        }

        PAssert.that(outputEntities2).satisfies(entities -> {
            int count = 0;
            for (final Entity entity : entities) {
                Assert.assertEquals(-62135596800L * 1000, Timestamps.toMillis(entity.getPropertiesOrThrow("eventtimeField").getTimestampValue()));
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    private static Type.StructField getStructField(final Type type, final String fieldName) {
        return type.getStructFields().stream().filter(f -> f.getName().equals(fieldName)).findAny().orElse(null);
    }

}
