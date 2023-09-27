package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.firestore.v1.Document;
import com.google.gson.*;
import com.google.protobuf.NullValue;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
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
import java.util.List;
import java.util.Map;

public class PartitionTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testRow() {
        final TransformConfig config = new TransformConfig();
        config.setName("partition");
        config.setModule("partition");
        config.setInputs(List.of("rowInput"));
        config.setParameters(createPartitionParameters());

        final Schema rowSchema = Schema.builder()
                .addField("stringField", Schema.FieldType.STRING.withNullable(true))
                .addInt64Field("intField")
                .addDoubleField("floatField")
                .build();

        Row testRow1 = Row.withSchema(rowSchema)
                .withFieldValue("stringField", "a")
                .withFieldValue("intField", 1L)
                .withFieldValue("floatField", 0.15D)
                .build();

        Row testRow2 = Row.withSchema(rowSchema)
                .withFieldValue("stringField", "b")
                .withFieldValue("intField", 2L)
                .withFieldValue("floatField", -0.15D)
                .build();

        Row testRow3 = Row.withSchema(rowSchema)
                .withFieldValue("stringField", null)
                .withFieldValue("intField", 3L)
                .withFieldValue("floatField", 0.0D)
                .build();

        final PCollection<Row> inputStructs = pipeline
                .apply("CreateDummy", Create.of(testRow1, testRow2, testRow3));
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputStructs, DataType.ROW, rowSchema);

        final Map<String, FCollection<?>> outputs = PartitionTransform.transform(List.of(fCollection), config);

        final PCollection<Row> outputRows1 = (PCollection<Row>) outputs.get("partition.output1").getCollection();
        final PCollection<Row> outputRows2 = (PCollection<Row>) outputs.get("partition.output2").getCollection();
        final PCollection<Row> outputRows3 = (PCollection<Row>) outputs.get("partition.output3").getCollection();
        final PCollection<Row> outputRows4 = (PCollection<Row>) outputs.get("partition.output4").getCollection();
        final PCollection<Row> outputRows5 = (PCollection<Row>) outputs.get("partition.output5").getCollection();
        final PCollection<Row> outputRows6 = (PCollection<Row>) outputs.get("partition.output6").getCollection();

        PAssert.that(outputRows1).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertEquals("a", row.getString("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRows2).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertTrue(row.getInt64("intField") <= 2);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputRows3).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertTrue(row.getDouble("floatField") > 0);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRows4).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertEquals("a", row.getString("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRows5).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertNull(row.getValue("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRows6).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertNotNull(row.getValue("stringField"));
                Assert.assertEquals(120D, row.getDouble("doubleField").doubleValue(), DELTA);
                Assert.assertEquals(row.getInt64("intField") * row.getDouble("floatField2"), row.getDouble("expressionField"), DELTA);
                switch (row.getString("stringField")) {
                    case "a" -> {
                        Assert.assertEquals(1L, row.getInt64("intField"), DELTA);
                        Assert.assertEquals(0.15D, row.getDouble("floatField2"), DELTA);
                        Assert.assertEquals(0.15D, row.getDouble("expressionField"), DELTA);
                        Assert.assertEquals("a_1_0.15", row.getString("concatField"));
                    }
                    case "b" -> {
                        Assert.assertEquals(2L, row.getInt64("intField"), DELTA);
                        Assert.assertEquals(-0.15D, row.getDouble("floatField2"), DELTA);
                        Assert.assertEquals(-0.3D, row.getDouble("expressionField"), DELTA);
                        Assert.assertEquals("b_2_-0.15", row.getString("concatField"));
                    }
                    default -> Assert.fail("illegal stringField value: " + row.getString("stringField"));
                }
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testAvro() {
        final TransformConfig config = new TransformConfig();
        config.setName("partition");
        config.setModule("partition");
        config.setInputs(List.of("rowInput"));
        config.setParameters(createPartitionParameters());

        final org.apache.avro.Schema avroSchema = SchemaBuilder.builder()
                .record("record").fields()
                .name("stringField").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("intField").type(AvroSchemaUtil.NULLABLE_LONG).noDefault()
                .name("floatField").type(AvroSchemaUtil.NULLABLE_DOUBLE).noDefault()
                .endRecord();

        final GenericRecord testRecord1  = new GenericRecordBuilder(avroSchema)
                .set("stringField", "a")
                .set("intField", 1L)
                .set("floatField", 0.15D)
                .build();

        final GenericRecord testRecord2  = new GenericRecordBuilder(avroSchema)
                .set("stringField", "b")
                .set("intField", 2L)
                .set("floatField", -0.15D)
                .build();

        final GenericRecord testRecord3  = new GenericRecordBuilder(avroSchema)
                .set("stringField", null)
                .set("intField", 3L)
                .set("floatField", 0.0D)
                .build();

        final PCollection<GenericRecord> inputStructs = pipeline
                .apply("CreateDummy", Create.of(testRecord1, testRecord2, testRecord3).withCoder(AvroCoder.of(avroSchema)));
                //.setCoder(AvroCoder.of(avroSchema));
        final FCollection<GenericRecord> fCollection = FCollection.of("avroInput", inputStructs, DataType.AVRO, avroSchema);

        final Map<String, FCollection<?>> outputs = PartitionTransform.transform(List.of(fCollection), config);

        final PCollection<GenericRecord> outputRecords1 = (PCollection<GenericRecord>) outputs.get("partition.output1").getCollection();
        final PCollection<GenericRecord> outputRecords2 = (PCollection<GenericRecord>) outputs.get("partition.output2").getCollection();
        final PCollection<GenericRecord> outputRecords3 = (PCollection<GenericRecord>) outputs.get("partition.output3").getCollection();
        final PCollection<GenericRecord> outputRecords4 = (PCollection<GenericRecord>) outputs.get("partition.output4").getCollection();
        final PCollection<GenericRecord> outputRecords5 = (PCollection<GenericRecord>) outputs.get("partition.output5").getCollection();
        final PCollection<GenericRecord> outputRecords6 = (PCollection<GenericRecord>) outputs.get("partition.output6").getCollection();

        PAssert.that(outputRecords1).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertEquals("a", record.get("stringField").toString());
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRecords2).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertTrue((Long)record.get("intField") <= 2);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputRecords3).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertTrue((Double)record.get("floatField") > 0);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRecords4).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertEquals("a", record.get("stringField").toString());
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRecords5).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertNull(record.get("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputRecords6).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertNotNull(record.get("stringField"));
                Assert.assertEquals(120D, (Double)record.get("doubleField"), DELTA);
                Assert.assertEquals((Long)record.get("intField") * (Double)record.get("floatField2"), (Double)record.get("expressionField"), DELTA);
                switch (record.get("stringField").toString()) {
                    case "a" -> {
                        Assert.assertEquals(1L, (Long)record.get("intField"), DELTA);
                        Assert.assertEquals(0.15D, (Double)record.get("floatField2"), DELTA);
                        Assert.assertEquals(0.15D, (Double)record.get("expressionField"), DELTA);
                        Assert.assertEquals("a_1_0.15", record.get("concatField").toString());
                    }
                    case "b" -> {
                        Assert.assertEquals(2L, (Long)record.get("intField"), DELTA);
                        Assert.assertEquals(-0.15D, (Double)record.get("floatField2"), DELTA);
                        Assert.assertEquals(-0.3D, (Double)record.get("expressionField"), DELTA);
                        Assert.assertEquals("b_2_-0.15", record.get("concatField").toString());
                    }
                    default -> Assert.fail("illegal stringField value: " + record.get("stringField"));
                }
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStruct() {
        final TransformConfig config = new TransformConfig();
        config.setName("partition");
        config.setModule("partition");
        config.setInputs(List.of("structInput"));
        config.setParameters(createPartitionParameters());

        Struct testStruct1 = Struct.newBuilder()
                .set("stringField").to("a")
                .set("intField").to(1L)
                .set("floatField").to(0.15D)
                .build();

        Struct testStruct2 = Struct.newBuilder()
                .set("stringField").to("b")
                .set("intField").to(2L)
                .set("floatField").to(-0.15D)
                .build();

        Struct testStruct3 = Struct.newBuilder()
                .set("stringField").to((String)null)
                .set("intField").to(3L)
                .set("floatField").to(0.0D)
                .build();

        final PCollection<Struct> inputStructs = pipeline
                .apply("CreateDummy", Create.of(testStruct1, testStruct2, testStruct3));
        final FCollection<Struct> fCollection = FCollection.of("structInput", inputStructs, DataType.STRUCT, testStruct1.getType());

        final Map<String, FCollection<?>> outputs = PartitionTransform.transform(List.of(fCollection), config);

        final PCollection<Struct> outputStructs1 = (PCollection<Struct>) outputs.get("partition.output1").getCollection();
        final PCollection<Struct> outputStructs2 = (PCollection<Struct>) outputs.get("partition.output2").getCollection();
        final PCollection<Struct> outputStructs3 = (PCollection<Struct>) outputs.get("partition.output3").getCollection();
        final PCollection<Struct> outputStructs4 = (PCollection<Struct>) outputs.get("partition.output4").getCollection();
        final PCollection<Struct> outputStructs5 = (PCollection<Struct>) outputs.get("partition.output5").getCollection();
        final PCollection<Struct> outputStructs6 = (PCollection<Struct>) outputs.get("partition.output6").getCollection();

        PAssert.that(outputStructs1).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertEquals("a", struct.getString("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputStructs2).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertTrue(struct.getLong("intField") <= 2);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputStructs3).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertTrue(struct.getDouble("floatField") > 0);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputStructs4).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertEquals("a", struct.getString("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputStructs5).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertTrue(struct.isNull("stringField"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputStructs6).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertNotNull(struct.getString("stringField"));
                Assert.assertEquals(120D, struct.getDouble("doubleField"), DELTA);
                Assert.assertEquals(struct.getLong("intField") * struct.getDouble("floatField2"), struct.getDouble("expressionField"), DELTA);
                switch (struct.getString("stringField")) {
                    case "a" -> {
                        Assert.assertEquals(1L, struct.getLong("intField"), DELTA);
                        Assert.assertEquals(0.15D, struct.getDouble("floatField2"), DELTA);
                        Assert.assertEquals(0.15D, struct.getDouble("expressionField"), DELTA);
                        Assert.assertEquals("a_1_0.15", struct.getString("concatField"));
                    }
                    case "b" -> {
                        Assert.assertEquals(2L, struct.getLong("intField"), DELTA);
                        Assert.assertEquals(-0.15D, struct.getDouble("floatField2"), DELTA);
                        Assert.assertEquals(-0.3D, struct.getDouble("expressionField"), DELTA);
                        Assert.assertEquals("b_2_-0.15", struct.getString("concatField"));
                    }
                    default -> Assert.fail("illegal stringField value: " + struct.getString("stringField"));
                }
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });


        pipeline.run();
    }

    @Test
    public void testDocument() {
        final TransformConfig config = new TransformConfig();
        config.setName("partition");
        config.setModule("partition");
        config.setInputs(List.of("documentInput"));
        config.setParameters(createPartitionParameters());

        final Schema rowSchema = Schema.builder()
                .addStringField("stringField")
                .addInt64Field("intField")
                .addDoubleField("floatField")
                .build();

        final Document testDocument1 = Document.newBuilder()
                .putFields("stringField", com.google.firestore.v1.Value.newBuilder().setStringValue("a").build())
                .putFields("intField", com.google.firestore.v1.Value.newBuilder().setIntegerValue(1).build())
                .putFields("floatField", com.google.firestore.v1.Value.newBuilder().setDoubleValue(0.15D).build())
                .build();

        final Document testDocument2 = Document.newBuilder()
                .putFields("stringField", com.google.firestore.v1.Value.newBuilder().setStringValue("b").build())
                .putFields("intField", com.google.firestore.v1.Value.newBuilder().setIntegerValue(2).build())
                .putFields("floatField", com.google.firestore.v1.Value.newBuilder().setDoubleValue(-0.15D).build())
                .build();

        final Document testDocument3 = Document.newBuilder()
                .putFields("stringField", com.google.firestore.v1.Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putFields("intField", com.google.firestore.v1.Value.newBuilder().setIntegerValue(3).build())
                .putFields("floatField", com.google.firestore.v1.Value.newBuilder().setDoubleValue(0D).build())
                .build();

        final PCollection<Document> inputEntities = pipeline
                .apply("CreateDummy", Create.of(testDocument1, testDocument2, testDocument3));
        final FCollection<Document> fCollection = FCollection.of("documentInput", inputEntities, DataType.DOCUMENT, rowSchema);

        final Map<String, FCollection<?>> outputs = PartitionTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<Document> outputDocuments1 = (PCollection<Document>) outputs.get("partition.output1").getCollection();
        final PCollection<Document> outputDocuments2 = (PCollection<Document>) outputs.get("partition.output2").getCollection();
        final PCollection<Document> outputDocuments3 = (PCollection<Document>) outputs.get("partition.output3").getCollection();
        final PCollection<Document> outputDocuments4 = (PCollection<Document>) outputs.get("partition.output4").getCollection();
        final PCollection<Document> outputDocuments5 = (PCollection<Document>) outputs.get("partition.output5").getCollection();
        final PCollection<Document> outputDocuments6 = (PCollection<Document>) outputs.get("partition.output6").getCollection();

        PAssert.that(outputDocuments1).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertEquals("a", document.getFieldsMap().get("stringField").getStringValue());
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputDocuments2).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertTrue(document.getFieldsMap().get("intField").getIntegerValue() <= 2);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputDocuments3).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertTrue(document.getFieldsMap().get("floatField").getDoubleValue() > 0);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputDocuments4).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertEquals("a", document.getFieldsOrThrow("stringField").getStringValue());
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputDocuments5).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertEquals(document.getFieldsOrThrow("stringField").getNullValue(), NullValue.NULL_VALUE);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputDocuments6).satisfies(documents -> {
            int count = 0;
            for(final Document document : documents) {
                Assert.assertNotNull(document.getFieldsOrThrow("stringField").getStringValue());
                Assert.assertEquals(120D, document.getFieldsOrThrow("doubleField").getDoubleValue(), DELTA);
                Assert.assertEquals(document.getFieldsOrThrow("intField").getIntegerValue() * document.getFieldsOrThrow("floatField2").getDoubleValue(), document.getFieldsOrThrow("expressionField").getDoubleValue(), DELTA);
                switch (document.getFieldsOrThrow("stringField").getStringValue()) {
                    case "a" -> {
                        Assert.assertEquals(1L, document.getFieldsOrThrow("intField").getIntegerValue(), DELTA);
                        Assert.assertEquals(0.15D, document.getFieldsOrThrow("floatField2").getDoubleValue(), DELTA);
                        Assert.assertEquals(0.15D, document.getFieldsOrThrow("expressionField").getDoubleValue(), DELTA);
                        Assert.assertEquals("a_1_0.15", document.getFieldsOrThrow("concatField").getStringValue());
                    }
                    case "b" -> {
                        Assert.assertEquals(2L, document.getFieldsOrThrow("intField").getIntegerValue(), DELTA);
                        Assert.assertEquals(-0.15D, document.getFieldsOrThrow("floatField2").getDoubleValue(), DELTA);
                        Assert.assertEquals(-0.3D, document.getFieldsOrThrow("expressionField").getDoubleValue(), DELTA);
                        Assert.assertEquals("b_2_-0.15", document.getFieldsOrThrow("concatField").getStringValue());
                    }
                    default -> Assert.fail("illegal stringField value: " + document.getFieldsOrThrow("stringField"));
                }
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testEntity() {
        final TransformConfig config = new TransformConfig();
        config.setName("partition");
        config.setModule("partition");
        config.setInputs(List.of("entityInput"));
        config.setParameters(createPartitionParameters());

        final Schema rowSchema = Schema.builder()
                .addStringField("stringField")
                .addInt64Field("intField")
                .addDoubleField("floatField")
                .build();

        final Entity testEntity1 = Entity.newBuilder()
                .putProperties("stringField", Value.newBuilder().setStringValue("a").build())
                .putProperties("intField", Value.newBuilder().setIntegerValue(1).build())
                .putProperties("floatField", Value.newBuilder().setDoubleValue(0.15D).build())
                .build();

        final Entity testEntity2 = Entity.newBuilder()
                .putProperties("stringField", Value.newBuilder().setStringValue("b").build())
                .putProperties("intField", Value.newBuilder().setIntegerValue(2).build())
                .putProperties("floatField", Value.newBuilder().setDoubleValue(-0.15D).build())
                .build();

        final Entity testEntity3 = Entity.newBuilder()
                .putProperties("stringField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putProperties("intField", Value.newBuilder().setIntegerValue(3).build())
                .putProperties("floatField", Value.newBuilder().setDoubleValue(0D).build())
                .build();

        final PCollection<Entity> inputEntities = pipeline
                .apply("CreateDummy", Create.of(testEntity1, testEntity2, testEntity3));
        final FCollection<Entity> fCollection = FCollection.of("entityInput", inputEntities, DataType.ENTITY, rowSchema);

        final Map<String, FCollection<?>> outputs = PartitionTransform.transform(List.of(fCollection), config);

        final PCollection<Entity> outputEntities1 = (PCollection<Entity>) outputs.get("partition.output1").getCollection();
        final PCollection<Entity> outputEntities2 = (PCollection<Entity>) outputs.get("partition.output2").getCollection();
        final PCollection<Entity> outputEntities3 = (PCollection<Entity>) outputs.get("partition.output3").getCollection();
        final PCollection<Entity> outputEntities4 = (PCollection<Entity>) outputs.get("partition.output4").getCollection();
        final PCollection<Entity> outputEntities5 = (PCollection<Entity>) outputs.get("partition.output5").getCollection();
        final PCollection<Entity> outputEntities6 = (PCollection<Entity>) outputs.get("partition.output6").getCollection();

        PAssert.that(outputEntities1).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertEquals("a", entity.getPropertiesMap().get("stringField").getStringValue());
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputEntities2).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertTrue(entity.getPropertiesMap().get("intField").getIntegerValue() <= 2);
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        PAssert.that(outputEntities3).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertTrue(entity.getPropertiesMap().get("floatField").getDoubleValue() > 0);
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputEntities4).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertTrue(entity.getPropertiesOrThrow("stringField").getStringValue().equals("a"));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputEntities5).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertTrue(entity.getPropertiesOrThrow("stringField").getNullValue().equals(NullValue.NULL_VALUE));
                count++;
            }
            Assert.assertEquals(1, count);
            return null;
        });

        PAssert.that(outputEntities6).satisfies(entities -> {
            int count = 0;
            for(final Entity entity : entities) {
                Assert.assertNotNull(entity.getPropertiesOrThrow("stringField").getStringValue());
                Assert.assertEquals(120D, entity.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
                Assert.assertEquals(entity.getPropertiesOrThrow("intField").getIntegerValue() * entity.getPropertiesOrThrow("floatField2").getDoubleValue(), entity.getPropertiesOrThrow("expressionField").getDoubleValue(), DELTA);
                switch (entity.getPropertiesOrThrow("stringField").getStringValue()) {
                    case "a" -> {
                        Assert.assertEquals(1L, entity.getPropertiesOrThrow("intField").getIntegerValue(), DELTA);
                        Assert.assertEquals(0.15D, entity.getPropertiesOrThrow("floatField2").getDoubleValue(), DELTA);
                        Assert.assertEquals(0.15D, entity.getPropertiesOrThrow("expressionField").getDoubleValue(), DELTA);
                        Assert.assertEquals("a_1_0.15", entity.getPropertiesOrThrow("concatField").getStringValue());
                    }
                    case "b" -> {
                        Assert.assertEquals(2L, entity.getPropertiesOrThrow("intField").getIntegerValue(), DELTA);
                        Assert.assertEquals(-0.15D, entity.getPropertiesOrThrow("floatField2").getDoubleValue(), DELTA);
                        Assert.assertEquals(-0.3D, entity.getPropertiesOrThrow("expressionField").getDoubleValue(), DELTA);
                        Assert.assertEquals("b_2_-0.15", entity.getPropertiesOrThrow("concatField").getStringValue());
                    }
                    default -> Assert.fail("illegal stringField value: " + entity.getPropertiesOrThrow("stringField"));
                }
                count++;
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

    private JsonObject createPartitionParameters() {
        final JsonArray partitions = new JsonArray();

        {
            final JsonObject partition1 = new JsonObject();
            partition1.addProperty("output", "output1");
            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "=");
            filter.addProperty("value", "a");
            filters.add(filter);
            partition1.add("filters", filters);
            partitions.add(partition1);
        }
        {
            final JsonObject partition2 = new JsonObject();
            partition2.addProperty("output", "output2");
            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "intField");
            filter.addProperty("op", "<=");
            filter.addProperty("value", 2);
            filters.add(filter);
            partition2.add("filters", filters);
            partitions.add(partition2);
        }
        {
            final JsonObject partition3 = new JsonObject();
            partition3.addProperty("output", "output3");
            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "floatField");
            filter.addProperty("op", ">");
            filter.addProperty("value", 0);
            filters.add(filter);
            partition3.add("filters", filters);
            partitions.add(partition3);
        }
        {
            final JsonObject partition4 = new JsonObject();
            partition4.addProperty("output", "output4");
            final JsonArray filters = new JsonArray();
            {
                final JsonObject filter = new JsonObject();
                filter.addProperty("key", "stringField");
                filter.addProperty("op", "!=");
                filter.addProperty("value", "null");
                filters.add(filter);
            }
            {
                final JsonObject filter = new JsonObject();
                filter.addProperty("key", "intField");
                filter.addProperty("op", "<");
                filter.addProperty("value", 2);
                filters.add(filter);
            }
            {
                final JsonObject filter = new JsonObject();
                filter.addProperty("key", "floatField");
                filter.addProperty("op", ">");
                filter.addProperty("value", 0);
                filters.add(filter);
            }

            partition4.add("filters", filters);
            partitions.add(partition4);
        }
        {
            final JsonObject partition5 = new JsonObject();
            partition5.addProperty("output", "output5");
            final JsonArray filters = new JsonArray();
            {
                final JsonObject filter = new JsonObject();
                filter.addProperty("key", "stringField");
                filter.addProperty("op", "=");
                filter.add("value", JsonNull.INSTANCE);
                filters.add(filter);
            }
            partition5.add("filters", filters);
            partitions.add(partition5);
        }
        {
            final JsonObject partition6 = new JsonObject();
            partition6.addProperty("output", "output6");
            final JsonArray filters = new JsonArray();
            {
                final JsonObject filter = new JsonObject();
                filter.addProperty("key", "stringField");
                filter.addProperty("op", "!=");
                filter.add("value", JsonNull.INSTANCE);
                filters.add(filter);
            }
            partition6.add("filters", filters);

            final JsonArray select = new JsonArray();
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "stringField");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "intField");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "floatField2");
                selectFunction.addProperty("field", "floatField");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "doubleField");
                selectFunction.addProperty("value", 120);
                selectFunction.addProperty("type", "double");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "hashField");
                selectFunction.addProperty("op", "hash");
                selectFunction.addProperty("field", "stringField");
                selectFunction.addProperty("secret", "my secret");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "expressionField");
                selectFunction.addProperty("expression", "floatField * intField");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "currentTimestampField");
                selectFunction.addProperty("func", "current_timestamp");
                select.add(selectFunction);
            }
            {
                final JsonObject selectFunction = new JsonObject();
                selectFunction.addProperty("name", "concatField");
                selectFunction.addProperty("func", "concat");
                selectFunction.addProperty("delimiter", "_");
                final JsonArray concatFields = new JsonArray();
                concatFields.add("stringField");
                concatFields.add("intField");
                concatFields.add("floatField");
                selectFunction.add("fields", concatFields);
                select.add(selectFunction);
            }
            partition6.add("select", select);

            partitions.add(partition6);
        }

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("exclusive", false);
        parameters.add("partitions", partitions);

        return parameters;
    }

}
