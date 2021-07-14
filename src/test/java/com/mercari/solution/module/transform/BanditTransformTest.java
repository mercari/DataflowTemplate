package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.avro.generic.GenericRecord;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BanditTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    //@Test
    public void testBanditRowTS() {
        final TransformConfig config = new TransformConfig();
        config.setName("bandit1");
        config.setModule("bandit");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray initialArms = new JsonArray();
        initialArms.add("a");
        initialArms.add("b");
        initialArms.add("c");

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("algorithm", "ts");
        parameters.addProperty("armField", "arm");
        parameters.addProperty("countField", "count");
        parameters.addProperty("rewardField", "reward");
        parameters.add("initialArms", initialArms);
        config.setParameters(parameters);

        final List<Row> rows = createRows();
        final Schema schema = rows.get(0).getSchema();

        // With window buffer
        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(rows))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = BanditTransform.transform(Arrays.asList(fCollection1), config);
        final PCollection<GenericRecord> outputArms1 = (PCollection<GenericRecord>) outputs1.get("bandit1").getCollection();

        final List<String> armNames = Arrays.asList("a","b","c");

        PAssert.that(outputArms1).satisfies(arms -> {
            final List<GenericRecord> armsList = Lists.newArrayList(arms);
            armsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());
            Assert.assertEquals(10, armsList.size());
            Assert.assertEquals("", (armsList.get(0)).get("target").toString());
            Assert.assertEquals("ts", (armsList.get(0)).get("algorithm").toString());
            Assert.assertTrue(armNames.contains(armsList.get(0).get("selectedArm").toString()));
            Assert.assertEquals(3, ((List)armsList.get(0).get("states")).size());
            List<GenericRecord> states = (List<GenericRecord>)armsList.get(0).get("states");
            for(final GenericRecord state : states) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(5L, state.get("count"));
                    Assert.assertEquals(0.4, state.get("value"));
                    Assert.assertEquals(0.32258064516129026D, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(2L, state.get("count"));
                    Assert.assertEquals(0.5, state.get("value"));
                    Assert.assertEquals(0.3763440860215053, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(3L, state.get("count"));
                    Assert.assertEquals(1.0/3, state.get("value"));
                    Assert.assertEquals(0.30107526881720426, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }

            Assert.assertEquals(3, ((List)armsList.get(1).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(2).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(3).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(4).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(5).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(6).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(7).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(8).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(9).get("states")).size());
            return null;
        });

        // No window buffer
        final PCollection<Row> inputRows2 = pipeline
                .apply("CreateDummy2", Create.of(rows));
        final FCollection<Row> fCollection2 = FCollection.of("rowInput", inputRows2, DataType.ROW, schema);

        config.setName("bandit2");
        final Map<String, FCollection<?>> outputs2 = BanditTransform.transform(Arrays.asList(fCollection2), config);
        final PCollection<GenericRecord> outputArms2 = (PCollection<GenericRecord>) outputs2.get("bandit2").getCollection();

        PAssert.that(outputArms2).satisfies(arms -> {
            final List<GenericRecord> armsList = Lists.newArrayList(arms);
            armsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());

            Assert.assertEquals(1, armsList.size());
            Assert.assertEquals("", (armsList.get(0)).get("target").toString());
            Assert.assertEquals("ts", (armsList.get(0)).get("algorithm").toString());
            Assert.assertTrue(armNames.contains(armsList.get(0).get("selectedArm").toString()));
            Assert.assertEquals(3, ((List)armsList.get(0).get("states")).size());
            List<GenericRecord> states = (List<GenericRecord>)armsList.get(0).get("states");
            for(final GenericRecord state : states) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(5L, state.get("count"));
                    Assert.assertEquals(0.4, state.get("value"));
                    Assert.assertEquals(0.32258064516129026D, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(2L, state.get("count"));
                    Assert.assertEquals(0.5, state.get("value"));
                    Assert.assertEquals(0.3763440860215053, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(3L, state.get("count"));
                    Assert.assertEquals(1.0/3, state.get("value"));
                    Assert.assertEquals(0.30107526881720426, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }
            return null;
        });

        pipeline.run();
    }

    //@Test
    public void testBanditRowUCB() {
        final TransformConfig config = new TransformConfig();
        config.setName("bandit1");
        config.setModule("bandit");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray initialArms = new JsonArray();
        initialArms.add("a");
        initialArms.add("b");
        initialArms.add("c");

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("algorithm", "ucb");
        parameters.addProperty("armField", "arm");
        parameters.addProperty("countField", "count");
        parameters.addProperty("rewardField", "reward");
        parameters.add("initialArms", initialArms);
        config.setParameters(parameters);

        final List<Row> rows = createRows();
        final Schema schema = rows.get(0).getSchema();

        // With window buffer
        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(rows))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = BanditTransform.transform(Arrays.asList(fCollection1), config);
        final PCollection<GenericRecord> outputArms1 = (PCollection<GenericRecord>) outputs1.get("bandit1").getCollection();

        final List<String> armNames = Arrays.asList("a","b","c");

        PAssert.that(outputArms1).satisfies(arms -> {
            final List<GenericRecord> armsList = Lists.newArrayList(arms);
            armsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());

            Assert.assertEquals(10, armsList.size());
            // Last result
            Assert.assertEquals("", (armsList.get(0)).get("target").toString());
            Assert.assertEquals("ucb", (armsList.get(0)).get("algorithm").toString());
            Assert.assertEquals("b", (armsList.get(0)).get("selectedArm").toString());
            Assert.assertEquals(3, ((List)armsList.get(0).get("states")).size());
            List<GenericRecord> states = (List<GenericRecord>)armsList.get(0).get("states");
            for(final GenericRecord state : states) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(5L, state.get("count"));
                    Assert.assertEquals(0.4, state.get("value"));
                    Assert.assertEquals(0.2827801726007821, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(2L, state.get("count"));
                    Assert.assertEquals(0.5, state.get("value"));
                    Assert.assertEquals(0.41303436346213185, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(3L, state.get("count"));
                    Assert.assertEquals(1D/3, state.get("value"));
                    Assert.assertEquals(0.304185463937086, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }

            Assert.assertEquals(3, ((List)armsList.get(1).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(2).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(3).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(4).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(5).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(6).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(7).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(8).get("states")).size());
            Assert.assertEquals(3, ((List)armsList.get(9).get("states")).size());

            // First result
            Assert.assertEquals(3, ((List)armsList.get(9).get("states")).size());
            List<GenericRecord> statesFirst = (List<GenericRecord>)armsList.get(9).get("states");
            for(final GenericRecord state : statesFirst) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(1L, state.get("count"));
                    Assert.assertEquals(1D, state.get("value"));
                    Assert.assertEquals(0D, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(0L, state.get("count"));
                    Assert.assertEquals(0D, state.get("value"));
                    Assert.assertEquals(0.5, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(0L, state.get("count"));
                    Assert.assertEquals(0D, state.get("value"));
                    Assert.assertEquals(0.5, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }

            // Second result
            Assert.assertEquals(3, ((List)armsList.get(8).get("states")).size());
            Assert.assertEquals("c", (armsList.get(8)).get("selectedArm").toString());
            List<GenericRecord> statesSecond = (List<GenericRecord>)armsList.get(8).get("states");
            for(final GenericRecord state : statesSecond) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(1L, state.get("count"));
                    Assert.assertEquals(1D, state.get("value"));
                    Assert.assertEquals(0D, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(1L, state.get("count"));
                    Assert.assertEquals(1D, state.get("value"));
                    Assert.assertEquals(0D, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(0L, state.get("count"));
                    Assert.assertEquals(0D, state.get("value"));
                    Assert.assertEquals(1D, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }

            return null;
        });

        // No window buffer
        final PCollection<Row> inputRows2 = pipeline
                .apply("CreateDummy2", Create.of(rows));
        final FCollection<Row> fCollection2 = FCollection.of("rowInput", inputRows2, DataType.ROW, schema);

        config.setName("bandit2");
        final Map<String, FCollection<?>> outputs2 = BanditTransform.transform(Arrays.asList(fCollection2), config);
        final PCollection<GenericRecord> outputArms2 = (PCollection<GenericRecord>) outputs2.get("bandit2").getCollection();

        PAssert.that(outputArms2).satisfies(arms -> {
            final List<GenericRecord> armsList = Lists.newArrayList(arms);
            armsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());

            Assert.assertEquals(1, armsList.size());
            Assert.assertEquals("", (armsList.get(0)).get("target").toString());
            Assert.assertEquals("ucb", (armsList.get(0)).get("algorithm").toString());
            Assert.assertEquals("b", (armsList.get(0)).get("selectedArm").toString());
            Assert.assertEquals(3, ((List)armsList.get(0).get("states")).size());
            List<GenericRecord> states = (List<GenericRecord>)armsList.get(0).get("states");
            for(final GenericRecord state : states) {
                if(state.get("arm").toString().equals("a")) {
                    Assert.assertEquals(5L, state.get("count"));
                    Assert.assertEquals(0.4, state.get("value"));
                    Assert.assertEquals(0.2827801726007821, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("b")) {
                    Assert.assertEquals(2L, state.get("count"));
                    Assert.assertEquals(0.5, state.get("value"));
                    Assert.assertEquals(0.41303436346213185, (Double)state.get("probability"), DELTA);
                } else if(state.get("arm").toString().equals("c")) {
                    Assert.assertEquals(3L, state.get("count"));
                    Assert.assertEquals(1D/3, state.get("value"));
                    Assert.assertEquals(0.304185463937086, (Double)state.get("probability"), DELTA);
                } else {
                    Assert.fail("Impossible arm value: " + state.get("arm"));
                }
            }
            return null;
        });

        pipeline.run();
    }

    private List<Row> createRows() {
        Schema schema = Schema.builder()
                .addField(Schema.Field.of("arm", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("count", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("reward", Schema.FieldType.DOUBLE.withNullable(false)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("arm", "a")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 1D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("arm", "b")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 1D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T01:00:00Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("arm", "c")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T02:00:00Z"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("arm", "c")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T03:00:00Z"))
                .build();
        final Row row5 = Row.withSchema(schema)
                .withFieldValue("arm", "b")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T04:00:00Z"))
                .build();
        final Row row6 = Row.withSchema(schema)
                .withFieldValue("arm", "a")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 1D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T05:00:00Z"))
                .build();
        final Row row7 = Row.withSchema(schema)
                .withFieldValue("arm", "a")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T06:00:00Z"))
                .build();
        final Row row8 = Row.withSchema(schema)
                .withFieldValue("arm", "a")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T07:00:00Z"))
                .build();
        final Row row9 = Row.withSchema(schema)
                .withFieldValue("arm", "a")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 0D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T08:00:00Z"))
                .build();
        final Row row10 = Row.withSchema(schema)
                .withFieldValue("arm", "c")
                .withFieldValue("count", 1L)
                .withFieldValue("reward", 1D)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T09:00:00Z"))
                .build();

        return Arrays.asList(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10);
    }

}
