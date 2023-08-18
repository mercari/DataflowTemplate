package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.TestDatum;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilterTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testRowFilter() {
        // filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter1");
            config.setModule("filter");
            config.setInputs(Arrays.asList("rowInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final Row dummyRow = TestDatum.generateRow();

            final PCollection<Row> inputStructs = pipeline
                    .apply("CreateDummy1", Create.of(dummyRow, dummyRow, dummyRow));
            final FCollection<Row> fCollection = FCollection.of("rowInput", inputStructs, DataType.ROW, dummyRow.getSchema());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<Row> outputRows = (PCollection<Row>) outputs.get("filter1").getCollection();

            PAssert.that(outputRows).satisfies(rows -> {
                int count = 0;
                for (final Row row : rows) {
                    count++;
                }
                Assert.assertEquals(3, count);
                return null;
            });
        }

        // Not filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter2");
            config.setModule("filter");
            config.setInputs(Arrays.asList("rowInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "!=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final Row dummyRow = TestDatum.generateRow();

            final PCollection<Row> inputStructs = pipeline
                    .apply("CreateDummy2", Create.of(dummyRow, dummyRow, dummyRow));
            final FCollection<Row> fCollection = FCollection.of("rowInput", inputStructs, DataType.ROW, dummyRow.getSchema());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<Row> outputRows = (PCollection<Row>) outputs.get("filter2").getCollection();

            PAssert.that(outputRows).satisfies(rows -> {
                int count = 0;
                for (final Row row : rows) {
                    count++;
                }
                Assert.assertEquals(0, count);
                return null;
            });
        }

        pipeline.run();
    }

    @Test
    public void testRecordFilter() {
        // filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter1");
            config.setModule("filter");
            config.setInputs(Arrays.asList("recordInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final GenericRecord dummyRecord = TestDatum.generateRecord();

            final PCollection<GenericRecord> inputStructs = pipeline
                    .apply("CreateDummy1", Create
                            .of(dummyRecord, dummyRecord, dummyRecord)
                            .withCoder(AvroCoder.of(dummyRecord.getSchema())));
            final FCollection<GenericRecord> fCollection = FCollection
                    .of("recordInput", inputStructs, DataType.AVRO, dummyRecord.getSchema());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<GenericRecord> outputRows = (PCollection<GenericRecord>) outputs.get("filter1").getCollection();

            PAssert.that(outputRows).satisfies(records -> {
                int count = 0;
                for (final GenericRecord record : records) {
                    count++;
                }
                Assert.assertEquals(3, count);
                return null;
            });
        }

        // Not filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter2");
            config.setModule("filter");
            config.setInputs(Arrays.asList("recordInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "!=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final GenericRecord dummyRecord = TestDatum.generateRecord();

            final PCollection<GenericRecord> inputStructs = pipeline
                    .apply("CreateDummy2", Create
                            .of(dummyRecord, dummyRecord, dummyRecord)
                            .withCoder(AvroCoder.of(dummyRecord.getSchema())));
            final FCollection<GenericRecord> fCollection = FCollection
                    .of("recordInput", inputStructs, DataType.AVRO, dummyRecord.getSchema());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<GenericRecord> outputRows = (PCollection<GenericRecord>) outputs.get("filter2").getCollection();

            PAssert.that(outputRows).satisfies(records -> {
                int count = 0;
                for (final GenericRecord record : records) {
                    count++;
                }
                Assert.assertEquals(0, count);
                return null;
            });
        }

        pipeline.run();
    }

    @Test
    public void testStructFilter() {
        // filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter1");
            config.setModule("filter");
            config.setInputs(Arrays.asList("structInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final Struct dummyStruct = TestDatum.generateStruct();

            final PCollection<Struct> inputStructs = pipeline
                    .apply("CreateDummy1", Create.of(dummyStruct, dummyStruct, dummyStruct));
            final FCollection<Struct> fCollection = FCollection.of("structInput", inputStructs, DataType.STRUCT, dummyStruct.getType());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<Struct> outputRows = (PCollection<Struct>) outputs.get("filter1").getCollection();

            PAssert.that(outputRows).satisfies(structs -> {
                int count = 0;
                for (final Struct struct : structs) {
                    count++;
                }
                Assert.assertEquals(3, count);
                return null;
            });
        }

        // Not filter
        {
            final TransformConfig config = new TransformConfig();
            config.setName("filter2");
            config.setModule("filter");
            config.setInputs(Arrays.asList("structInput"));

            final JsonArray filters = new JsonArray();
            final JsonObject filter = new JsonObject();
            filter.addProperty("key", "stringField");
            filter.addProperty("op", "!=");
            filter.addProperty("value", "stringValue");
            filters.add(filter);

            final JsonObject parameters = new JsonObject();
            parameters.add("filters", filters);
            config.setParameters(parameters);

            final Struct dummyStruct = TestDatum.generateStruct();

            final PCollection<Struct> inputStructs = pipeline
                    .apply("CreateDummy2", Create.of(dummyStruct, dummyStruct, dummyStruct));
            final FCollection<Struct> fCollection = FCollection.of("structInput", inputStructs, DataType.STRUCT, dummyStruct.getType());

            final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);
            final PCollection<Struct> outputRows = (PCollection<Struct>) outputs.get("filter2").getCollection();

            PAssert.that(outputRows).satisfies(structs -> {
                int count = 0;
                for (final Struct struct : structs) {
                    count++;
                }
                Assert.assertEquals(0, count);
                return null;
            });
        }

        pipeline.run();
    }

    @Test
    public void testRowFields() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray fields = new JsonArray();
        final List<String> fieldNames = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        fieldNames.forEach(fields::add);

        final JsonObject parameters = new JsonObject();
        parameters.add("fields", fields);
        config.setParameters(parameters);

        final Row dummyRow = TestDatum.generateRow();

        final PCollection<Row> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyRow, dummyRow, dummyRow));
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputStructs, DataType.ROW, dummyRow.getSchema());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<Row> outputRows = (PCollection<Row>) outputs.get("filter").getCollection();

        PAssert.that(outputRows).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertEquals(5, row.getFieldCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), row.getString("stringField"));
                Assert.assertEquals(TestDatum.getIntFieldValue(), row.getInt32("intField"));
                Assert.assertEquals(TestDatum.getLongFieldValue(), row.getInt64("longField"));

                final Row selectedRowChild = row.getRow("recordField");
                Assert.assertEquals(5, selectedRowChild.getFieldCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedRowChild.getString("stringField"));
                Assert.assertEquals(TestDatum.getDoubleFieldValue(), selectedRowChild.getDouble("doubleField"));
                Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedRowChild.getBoolean("booleanField"));

                final Row selectedRowGrandchild = selectedRowChild.getRow("recordField");
                Assert.assertEquals(2, selectedRowGrandchild.getFieldCount());
                Assert.assertEquals(TestDatum.getIntFieldValue(), selectedRowGrandchild.getInt32("intField"));
                Assert.assertEquals(TestDatum.getFloatFieldValue(), selectedRowGrandchild.getFloat("floatField"));

                Assert.assertEquals(2, row.getArray("recordArrayField").size());
                for(final Row child : row.<Row>getArray("recordArrayField")) {
                    Assert.assertEquals(4, child.getFieldCount());
                    Assert.assertEquals(TestDatum.getStringFieldValue(), child.getString("stringField"));
                    Assert.assertEquals(TestDatum.getTimestampFieldValue(), child.getDateTime("timestampField"));

                    Assert.assertEquals(2, child.getArray("recordArrayField").size());
                    for(final Row grandchild : child.<Row>getArray("recordArrayField")) {
                        Assert.assertEquals(2, grandchild.getFieldCount());
                        Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.getInt32("intField"));
                        Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.getFloat("floatField"));
                    }

                    final Row grandchild = child.getRow("recordField");
                    Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.getInt32("intField"));
                    Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.getFloat("floatField"));
                }

                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRecordFields() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("recordInput"));

        final JsonArray fields = new JsonArray();
        final List<String> fieldNames = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        fieldNames.forEach(fields::add);

        final JsonObject parameters = new JsonObject();
        parameters.add("fields", fields);
        config.setParameters(parameters);

        final GenericRecord dummyRecord = TestDatum.generateRecord();

        final PCollection<GenericRecord> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyRecord, dummyRecord, dummyRecord).withCoder(AvroCoder.of(dummyRecord.getSchema())));
        final FCollection<GenericRecord> fCollection = FCollection.of("recordInput", inputStructs, DataType.AVRO, dummyRecord.getSchema());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<GenericRecord> outputRows = (PCollection<GenericRecord>) outputs.get("filter").getCollection();

        PAssert.that(outputRows).satisfies(records -> {
            int count = 0;
            for(final GenericRecord selectedRecord : records) {
                Assert.assertEquals(5, selectedRecord.getSchema().getFields().size());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedRecord.get("stringField").toString());
                Assert.assertEquals(TestDatum.getIntFieldValue(), selectedRecord.get("intField"));
                Assert.assertEquals(TestDatum.getLongFieldValue(), selectedRecord.get("longField"));

                final GenericRecord selectedRecordChild = (GenericRecord) selectedRecord.get("recordField");
                Assert.assertEquals(5, selectedRecordChild.getSchema().getFields().size());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedRecordChild.get("stringField").toString());
                Assert.assertEquals(TestDatum.getDoubleFieldValue(), selectedRecordChild.get("doubleField"));
                Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedRecordChild.get("booleanField"));

                final GenericRecord selectedRecordGrandchild = (GenericRecord) selectedRecordChild.get("recordField");
                Assert.assertEquals(2, selectedRecordGrandchild.getSchema().getFields().size());
                Assert.assertEquals(TestDatum.getIntFieldValue(), selectedRecordGrandchild.get("intField"));
                Assert.assertEquals(TestDatum.getFloatFieldValue(), selectedRecordGrandchild.get("floatField"));

                Assert.assertEquals(2, ((List)selectedRecord.get("recordArrayField")).size());
                for(final GenericRecord child : (List<GenericRecord>)selectedRecord.get("recordArrayField")) {
                    Assert.assertEquals(4, child.getSchema().getFields().size());
                    Assert.assertEquals(TestDatum.getStringFieldValue(), child.get("stringField").toString());
                    Assert.assertEquals(TestDatum.getTimestampFieldValue(), Instant.ofEpochMilli((Long)child.get("timestampField") / 1000));

                    Assert.assertEquals(2, ((List)child.get("recordArrayField")).size());
                    for(final GenericRecord grandchild : (List<GenericRecord>)child.get("recordArrayField")) {
                        Assert.assertEquals(2, grandchild.getSchema().getFields().size());
                        Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
                        Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
                    }

                    final GenericRecord grandchild = (GenericRecord) child.get("recordField");
                    Assert.assertEquals(TestDatum.getIntFieldValue(), grandchild.get("intField"));
                    Assert.assertEquals(TestDatum.getFloatFieldValue(), grandchild.get("floatField"));
                }

                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testStructFields() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("structInput"));

        final JsonArray fields = new JsonArray();
        final List<String> fieldNames = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        fieldNames.forEach(fields::add);

        final JsonObject parameters = new JsonObject();
        parameters.add("fields", fields);
        config.setParameters(parameters);

        final Struct dummyStruct = TestDatum.generateStruct();

        final PCollection<Struct> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyStruct, dummyStruct, dummyStruct));
        final FCollection<Struct> fCollection = FCollection.of("structInput", inputStructs, DataType.STRUCT, dummyStruct.getType());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<Struct> outputRows = (PCollection<Struct>) outputs.get("filter").getCollection();

        PAssert.that(outputRows).satisfies(structs -> {
            int count = 0;
            for(final Struct selectedStruct : structs) {
                Assert.assertEquals(5, selectedStruct.getColumnCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedStruct.getString("stringField"));
                Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), ((Long)selectedStruct.getLong("intField")).intValue());
                Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedStruct.getLong("longField"));

                final Struct selectedStructChild = selectedStruct.getStruct("recordField");
                Assert.assertEquals(5, selectedStructChild.getColumnCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedStructChild.getString("stringField"));
                Assert.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedStructChild.getDouble("doubleField"), DELTA);
                Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedStructChild.getBoolean("booleanField"));

                final Struct selectedStructGrandchild = selectedStructChild.getStruct("recordField");
                Assert.assertEquals(2, selectedStructGrandchild.getColumnCount());
                Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(selectedStructGrandchild.getLong("intField")).intValue());
                Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(selectedStructGrandchild.getDouble("floatField")).floatValue(), DELTA);

                Assert.assertEquals(2, selectedStruct.getStructList("recordArrayField").size());
                for(final Struct child : selectedStruct.getStructList("recordArrayField")) {
                    Assert.assertEquals(4, child.getColumnCount());
                    Assert.assertEquals(TestDatum.getStringFieldValue(), child.getString("stringField"));
                    Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(), Timestamps.toMillis(child.getTimestamp("timestampField").toProto()));

                    Assert.assertEquals(2, child.getStructList("recordArrayField").size());
                    for(final Struct grandchild : child.getStructList("recordArrayField")) {
                        Assert.assertEquals(2, grandchild.getColumnCount());
                        Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
                        Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getDouble("floatField")).floatValue(), DELTA);
                    }

                    final Struct grandchild = child.getStruct("recordField");
                    Assert.assertEquals(TestDatum.getIntFieldValue().intValue(), Long.valueOf(grandchild.getLong("intField")).intValue());
                    Assert.assertEquals(TestDatum.getFloatFieldValue().floatValue(), Double.valueOf(grandchild.getDouble("floatField")).floatValue(), DELTA);
                }

                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testEntityFields() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("entityInput"));

        final JsonArray fields = new JsonArray();
        final List<String> fieldNames = Arrays.asList(
                "stringField", "intField", "longField",
                "recordField.stringField", "recordField.doubleField", "recordField.booleanField",
                "recordField.recordField.intField", "recordField.recordField.floatField",
                "recordField.recordArrayField.intField", "recordField.recordArrayField.floatField",
                "recordArrayField.stringField", "recordArrayField.timestampField",
                "recordArrayField.recordField.intField", "recordArrayField.recordField.floatField",
                "recordArrayField.recordArrayField.intField", "recordArrayField.recordArrayField.floatField");
        fieldNames.forEach(fields::add);

        final JsonObject parameters = new JsonObject();
        parameters.add("fields", fields);
        config.setParameters(parameters);

        final Row dummyRpw = TestDatum.generateRow();
        final Entity dummyEntity = TestDatum.generateEntity();

        final PCollection<Entity> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyEntity, dummyEntity, dummyEntity));
        final FCollection<Entity> fCollection = FCollection.of("entityInput", inputStructs, DataType.ENTITY, dummyRpw.getSchema());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<Entity> outputEntities = (PCollection<Entity>) outputs.get("filter").getCollection();

        PAssert.that(outputEntities).satisfies(entities -> {
            int count = 0;
            for(final Entity selectedEntity : entities) {
                Assert.assertEquals(5, selectedEntity.getPropertiesCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedEntity.getPropertiesOrThrow("stringField").getStringValue());
                Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("intField").getIntegerValue());
                Assert.assertEquals(TestDatum.getLongFieldValue().longValue(), selectedEntity.getPropertiesOrThrow("longField").getIntegerValue());

                final Entity selectedEntityChild = selectedEntity.getPropertiesOrThrow("recordField").getEntityValue();
                Assert.assertEquals(5, selectedEntityChild.getPropertiesCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), selectedEntityChild.getPropertiesOrThrow("stringField").getStringValue());
                Assert.assertEquals(TestDatum.getDoubleFieldValue().doubleValue(), selectedEntityChild.getPropertiesOrThrow("doubleField").getDoubleValue(), DELTA);
                Assert.assertEquals(TestDatum.getBooleanFieldValue(), selectedEntityChild.getPropertiesOrThrow("booleanField").getBooleanValue());

                final Entity selectedEntityGrandchild = selectedEntityChild.getPropertiesOrThrow("recordField").getEntityValue();
                Assert.assertEquals(2, selectedEntityGrandchild.getPropertiesCount());
                Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), selectedEntityGrandchild.getPropertiesOrThrow("intField").getIntegerValue());
                Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), selectedEntityGrandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);

                Assert.assertEquals(2, selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
                for(final Value childValue : selectedEntity.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                    final Entity child = childValue.getEntityValue();
                    Assert.assertEquals(4, child.getPropertiesCount());
                    Assert.assertEquals(TestDatum.getStringFieldValue(), child.getPropertiesOrThrow("stringField").getStringValue());
                    Assert.assertEquals(TestDatum.getTimestampFieldValue().getMillis(),
                            Timestamps.toMillis(child.getPropertiesOrThrow("timestampField").getTimestampValue()));

                    Assert.assertEquals(2, child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesCount());
                    for(final Value grandchilValue : child.getPropertiesOrThrow("recordArrayField").getArrayValue().getValuesList()) {
                        final Entity grandchild = grandchilValue.getEntityValue();
                        Assert.assertEquals(2, grandchild.getPropertiesCount());
                        Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
                        Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
                    }

                    final Entity grandchild = child.getPropertiesOrThrow("recordField").getEntityValue();
                    Assert.assertEquals(TestDatum.getIntFieldValue().longValue(), grandchild.getPropertiesOrThrow("intField").getIntegerValue());
                    Assert.assertEquals(TestDatum.getFloatFieldValue().doubleValue(), grandchild.getPropertiesOrThrow("floatField").getDoubleValue(), DELTA);
                }

                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRowSelect() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray selectFields = new JsonArray();
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "stringField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "renamedIntField");
            selectField.addProperty("field", "intField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "constantTimestampField");
            selectField.addProperty("type", "timestamp");
            selectField.addProperty("value", "2023-08-15T00:00:00Z");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "constantLongField");
            selectField.addProperty("type", "long");
            selectField.addProperty("value", 120L);
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "expressionField");
            selectField.addProperty("expression", "longField * doubleField / intField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "hashField");
            selectField.addProperty("func", "hash");
            selectField.addProperty("field", "stringField");
            selectField.addProperty("secret", "my secret");
            selectField.addProperty("size", 16);
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "nestedStringField");
            selectField.addProperty("field", "recordField.stringField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "nestedArrayStringField");
            selectField.addProperty("field", "recordField.stringArrayField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "currentTimestampField");
            selectField.addProperty("func", "current_timestamp");
            selectFields.add(selectField);
        }

        final JsonObject parameters = new JsonObject();
        parameters.add("select", selectFields);
        config.setParameters(parameters);

        final Row dummyRow = TestDatum.generateRow();

        final PCollection<Row> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyRow, dummyRow, dummyRow));
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputStructs, DataType.ROW, dummyRow.getSchema());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<Row> outputRows = (PCollection<Row>) outputs.get("filter").getCollection();

        PAssert.that(outputRows).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                Assert.assertEquals(9, row.getFieldCount());
                Assert.assertEquals(TestDatum.getStringFieldValue(), row.getString("stringField"));
                Assert.assertEquals(TestDatum.getIntFieldValue(), row.getInt32("renamedIntField"));
                Assert.assertEquals(Instant.parse("2023-08-15T00:00:00.000Z"), row.getDateTime("constantTimestampField"));
                Assert.assertEquals(120L, row.getInt64("constantLongField").longValue());
                Assert.assertEquals((Double)(TestDatum.getLongFieldValue() * TestDatum.getDoubleFieldValue() / TestDatum.getIntFieldValue()), row.getDouble("expressionField"));
                Assert.assertEquals("5fa62a43c07f6d83", row.getString("hashField"));
                Assert.assertEquals(TestDatum.getStringFieldValue(), row.getString("nestedStringField"));
                Assert.assertEquals(TestDatum.getStringArrayFieldValues(), row.getArray("nestedArrayStringField"));
                Assert.assertNotNull(row.getDateTime("currentTimestampField"));
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testRecordSelect() {
        final TransformConfig config = new TransformConfig();
        config.setName("filter");
        config.setModule("filter");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray selectFields = new JsonArray();
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "stringField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "renamedIntField");
            selectField.addProperty("field", "intField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "constantTimestampField");
            selectField.addProperty("type", "timestamp");
            selectField.addProperty("value", "2023-08-15T00:00:00Z");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "constantLongField");
            selectField.addProperty("type", "long");
            selectField.addProperty("value", 120L);
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "expressionField");
            selectField.addProperty("expression", "longField * doubleField / intField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "hashField");
            selectField.addProperty("func", "hash");
            selectField.addProperty("field", "stringField");
            selectField.addProperty("secret", "my secret");
            selectField.addProperty("size", 16);
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "nestedStringField");
            selectField.addProperty("field", "recordField.stringField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "nestedArrayStringField");
            selectField.addProperty("field", "recordField.stringArrayField");
            selectFields.add(selectField);
        }
        {
            final JsonObject selectField = new JsonObject();
            selectField.addProperty("name", "currentTimestampField");
            selectField.addProperty("func", "current_timestamp");
            selectFields.add(selectField);
        }

        final JsonObject parameters = new JsonObject();
        parameters.add("select", selectFields);
        config.setParameters(parameters);

        final GenericRecord dummyRecord = TestDatum.generateRecord();

        final PCollection<GenericRecord> inputStructs = pipeline
                .apply("CreateDummy", Create.of(dummyRecord, dummyRecord, dummyRecord).withCoder(AvroCoder.of(dummyRecord.getSchema())));
        final FCollection<GenericRecord> fCollection = FCollection.of("recordInput", inputStructs, DataType.AVRO, dummyRecord.getSchema());

        final Map<String, FCollection<?>> outputs = FilterTransform.transform(Arrays.asList(fCollection), config);

        final PCollection<GenericRecord> outputRecords = (PCollection<GenericRecord>) outputs.get("filter").getCollection();

        PAssert.that(outputRecords).satisfies(records -> {
            int count = 0;
            for(final GenericRecord record : records) {
                Assert.assertEquals(9, record.getSchema().getFields().size());
                Assert.assertEquals(TestDatum.getStringFieldValue(), record.get("stringField").toString());
                Assert.assertEquals(TestDatum.getIntFieldValue(), record.get("renamedIntField"));
                Assert.assertEquals(Instant.parse("2023-08-15T00:00:00.000Z").getMillis() * 1000L, record.get("constantTimestampField"));
                Assert.assertEquals(120L, record.get("constantLongField"));
                Assert.assertEquals((Double)(TestDatum.getLongFieldValue() * TestDatum.getDoubleFieldValue() / TestDatum.getIntFieldValue()), record.get("expressionField"));
                Assert.assertEquals("5fa62a43c07f6d83", record.get("hashField").toString());
                Assert.assertEquals(TestDatum.getStringFieldValue(), record.get("nestedStringField").toString());
                final List<String> nestedArrayStringField = new ArrayList<>();
                for(Object value : (Iterable)record.get("nestedArrayStringField")) {
                    nestedArrayStringField.add(value == null ? null : value.toString());
                }
                Assert.assertEquals(TestDatum.getStringArrayFieldValues(), nestedArrayStringField);
                Assert.assertNotNull(record.get("currentTimestampField"));
                count++;
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();
    }

}
