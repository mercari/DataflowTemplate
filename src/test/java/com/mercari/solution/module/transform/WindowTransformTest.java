package com.mercari.solution.module.transform;

import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class WindowTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testFixedWindow() {
        // window module config
        final TransformConfig config = new TransformConfig();
        config.setName("withWindow");
        config.setModule("window");
        config.setInputs(Arrays.asList("withEventtime"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("type", "fixed");
        parameters.addProperty("size", 1);
        parameters.addProperty("unit", "minute");
        config.setParameters(parameters);

        // eventtime module config
        final TransformConfig configEventtime = new TransformConfig();
        configEventtime.setName("withEventtime");
        configEventtime.setModule("eventtime");
        configEventtime.setInputs(Arrays.asList("rowInput"));

        final JsonObject eventtimeParameters = new JsonObject();
        eventtimeParameters.addProperty("eventtimeField", "timestampField");
        eventtimeParameters.addProperty("into", false);
        configEventtime.setParameters(eventtimeParameters);

        // beamsql module config
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsql");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT stringField, COUNT(*) AS count, MAX(timestampField) AS mt FROM withWindow GROUP BY stringField");
        beamsqlParameters.addProperty("planner", "zetasql");
        configBeamSql.setParameters(beamsqlParameters);


        final Schema schema = Schema.builder()
                .addField("stringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                .addField("timestampField", org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:03.000Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:59.000Z"))
                .build();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(row1, row2, row3, row4))
                .setRowSchema(schema);
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, schema);

        final Map<String, FCollection<?>> withEventTime = EventTimeTransform.transform(Arrays.asList(fCollection), configEventtime);
        final Map<String, FCollection<?>> withWindow = WindowTransform.transform(new ArrayList<>(withEventTime.values()), config);
        final FCollection<Row> results = BeamSQLTransform.transform(new ArrayList<>(withWindow.values()), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                if("a".equals(row.getString("stringField"))) {
                    Assert.assertEquals(1L, Objects.requireNonNull(row.getInt64("count")).longValue());
                } else if("b".equals(row.getString("stringField"))) {
                    Assert.assertEquals(2L, Objects.requireNonNull(row.getInt64("count")).longValue());
                    Assert.assertEquals(Instant.parse("2022-01-01T01:02:59.000Z"), Objects.requireNonNull(row.getDateTime("mt")));
                } else {
                    Assert.fail("illegal stringField value: " + row.getString("stringField"));
                }
            }
            Assert.assertEquals(3, count);
            return null;
        });


        pipeline.run();
    }

    @Test
    public void testGlobalWindowAndRepeatedAfterPaneTrigger() {
        // window module config
        final TransformConfig config = new TransformConfig();
        config.setName("withWindow");
        config.setModule("window");
        config.setInputs(Arrays.asList("withEventtime"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("type", "global");
        parameters.addProperty("unit", "minute");
        final JsonObject triggerParameters = new JsonObject();
        triggerParameters.addProperty("type", "repeatedly");
        final JsonObject foreverTriggerParameters = new JsonObject();
        foreverTriggerParameters.addProperty("type", "afterPane");
        foreverTriggerParameters.addProperty("elementCountAtLeast", 2);
        triggerParameters.add("foreverTrigger", foreverTriggerParameters);
        parameters.add("trigger", triggerParameters);
        config.setParameters(parameters);

        // eventtime module config
        final TransformConfig configEventtime = new TransformConfig();
        configEventtime.setName("withEventtime");
        configEventtime.setModule("eventtime");
        configEventtime.setInputs(Arrays.asList("rowInput"));

        final JsonObject eventtimeParameters = new JsonObject();
        eventtimeParameters.addProperty("eventtimeField", "timestampField");
        eventtimeParameters.addProperty("into", false);
        configEventtime.setParameters(eventtimeParameters);

        // beamsql module config
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsql");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT stringField, COUNT(*) AS count, MAX(timestampField) AS mt FROM withWindow GROUP BY stringField HAVING count > 0");
        beamsqlParameters.addProperty("planner", "zetasql");
        configBeamSql.setParameters(beamsqlParameters);


        final Schema schema = Schema.builder()
                .addField("stringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                .addField("timestampField", org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Row row1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:03.000Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:03.000Z"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:59.000Z"))
                .build();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(row1, row2, row3, row4))
                .setRowSchema(schema);
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, schema);

        final Map<String, FCollection<?>> withEventTime = EventTimeTransform.transform(Arrays.asList(fCollection), configEventtime);
        final Map<String, FCollection<?>> withWindow = WindowTransform.transform(new ArrayList<>(withEventTime.values()), config);
        final FCollection<Row> results = BeamSQLTransform.transform(new ArrayList<>(withWindow.values()), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                Assert.assertEquals(2L, Objects.requireNonNull(row.getInt64("count")).longValue());
            }
            Assert.assertEquals(2, count);
            return null;
        });


        pipeline.run();
    }

    @Test
    public void testGlobalWindowAndJoinRepeatedTrigger() {

        // eventtime module config
        final TransformConfig configEventtime = new TransformConfig();
        configEventtime.setName("withEventtime");
        configEventtime.setModule("eventtime");
        configEventtime.setInputs(Arrays.asList("rowInputA", "rowInputB"));

        final JsonObject eventtimeParameters = new JsonObject();
        eventtimeParameters.addProperty("eventtimeField", "timestampField");
        eventtimeParameters.addProperty("into", false);
        configEventtime.setParameters(eventtimeParameters);

        // window module config
        final TransformConfig config = new TransformConfig();
        config.setName("withWindow");
        config.setModule("window");
        config.setInputs(Arrays.asList("withEventtime.rowInputA", "withEventtime.rowInputB"));

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("type", "global");
        parameters.addProperty("accumulationMode", "discarding"); // accumulating, discarding
        final JsonObject triggerParameters = new JsonObject();
        triggerParameters.addProperty("type", "repeatedly");
        final JsonObject foreverTriggerParameters = new JsonObject();
        foreverTriggerParameters.addProperty("type", "afterPane");
        foreverTriggerParameters.addProperty("elementCountAtLeast", 2);
        triggerParameters.add("foreverTrigger", foreverTriggerParameters);

        parameters.add("trigger", triggerParameters);
        config.setParameters(parameters);

        // beamsql module config
        final TransformConfig configBeamSql = new TransformConfig();
        configBeamSql.setName("beamsql");
        configBeamSql.setModule("beamsql");
        configBeamSql.setInputs(Arrays.asList("withWindow.withEventtime.rowInputA", "withWindow.withEventtime.rowInputB"));

        final JsonObject beamsqlParameters = new JsonObject();
        beamsqlParameters.addProperty("sql", "SELECT A.stringField AS sfa, MAX(B.stringField) AS sfb, COUNT(*) AS cnt FROM `withWindow.withEventtime.rowInputA` AS A LEFT JOIN `withWindow.withEventtime.rowInputB` AS B ON A.stringField = B.stringField GROUP BY A.stringField HAVING cnt > 0");
        beamsqlParameters.addProperty("planner", "zetasql");
        configBeamSql.setParameters(beamsqlParameters);


        final Schema schema = Schema.builder()
                .addField("stringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(true))
                .addField("timestampField", org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(true))
                .build();

        final Row rowA1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:01.000Z"))
                .build();
        final Row rowA2 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:01.000Z"))
                .build();

        final Row rowB1 = Row.withSchema(schema)
                .withFieldValue("stringField", "a")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:02:02.000Z"))
                .build();
        final Row rowB2 = Row.withSchema(schema)
                .withFieldValue("stringField", "b")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:03:03.000Z"))
                .build();
        final Row rowB3 = Row.withSchema(schema)
                .withFieldValue("stringField", "c")
                .withFieldValue("timestampField", Instant.parse("2022-01-01T01:04:31.000Z"))
                .build();

        final PCollection<Row> inputRowsA = pipeline
                .apply("CreateDummyA", Create.of(rowA1, rowA2))
                .setRowSchema(schema);
        final FCollection<Row> fCollectionA = FCollection.of("rowInputA", inputRowsA, DataType.ROW, schema);
        final PCollection<Row> inputRowsB = pipeline
                .apply("CreateDummyB", Create.of(rowB1, rowB2, rowB3))
                .setRowSchema(schema);
        final FCollection<Row> fCollectionB = FCollection.of("rowInputB", inputRowsB, DataType.ROW, schema);

        final Map<String, FCollection<?>> withEventTime = EventTimeTransform.transform(Arrays.asList(fCollectionA, fCollectionB), configEventtime);
        final Map<String, FCollection<?>> withWindow = WindowTransform.transform(new ArrayList<>(withEventTime.values()), config);
        final FCollection<Row> results = BeamSQLTransform.transform(new ArrayList<>(withWindow.values()), configBeamSql);

        PAssert.that(results.getCollection()).satisfies(rows -> {
            int count = 0;
            for (final Row row : rows) {
                count++;
                Assert.assertEquals(1L, Objects.requireNonNull(row.getInt64("cnt")).longValue());
                Assert.assertEquals(row.getString("sfa"), row.getString("sfb"));
            }
            Assert.assertEquals(2, count);
            return null;
        });

        pipeline.run();
    }

}
