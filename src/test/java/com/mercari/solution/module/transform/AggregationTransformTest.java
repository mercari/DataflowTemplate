package com.mercari.solution.module.transform;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.coders.RowCoder;
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

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

public class AggregationTransformTest {

    private static final double DELTA = 1e-15;

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testAggregation() {

        final TransformConfig config = new TransformConfig();
        config.setName("aggregation");
        config.setModule("aggregation");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray groupFields = new JsonArray();
        groupFields.add("bool");

        final JsonArray definitions = new JsonArray();
        {
            final JsonObject definition = new JsonObject();
            definition.addProperty("input", "rowInput");

            final JsonArray fields = new JsonArray();
            {
                final JsonObject fieldCount = new JsonObject();
                fieldCount.addProperty("name", "count");
                fieldCount.addProperty("op", "count");
                fields.add(fieldCount);
            }
            {
                final JsonObject fieldMaxDouble = new JsonObject();
                fieldMaxDouble.addProperty("name", "maxDouble");
                fieldMaxDouble.addProperty("op", "max");
                fieldMaxDouble.addProperty("field", "double");
                fields.add(fieldMaxDouble);
            }
            {
                final JsonObject fieldStringDouble = new JsonObject();
                fieldStringDouble.addProperty("name", "maxString");
                fieldStringDouble.addProperty("op", "max");
                fieldStringDouble.addProperty("field", "string");
                fields.add(fieldStringDouble);
            }
            {
                final JsonObject fieldMinExpression = new JsonObject();
                fieldMinExpression.addProperty("name", "minExpression");
                fieldMinExpression.addProperty("op", "min");
                fieldMinExpression.addProperty("expression", "long * double");
                fields.add(fieldMinExpression);
            }
            {
                final JsonObject fieldArgMaxLong = new JsonObject();
                fieldArgMaxLong.addProperty("name", "argmaxLong");
                fieldArgMaxLong.addProperty("op", "argmax");
                fieldArgMaxLong.addProperty("comparingField", "long");
                fieldArgMaxLong.addProperty("comparingValueField", "argmaxLongComparingValue");

                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("long");
                targets.add("timestamp");
                fieldArgMaxLong.add("fields", targets);

                fields.add(fieldArgMaxLong);
            }
            {
                final JsonObject fieldArgMinExpression = new JsonObject();
                fieldArgMinExpression.addProperty("name", "argminExpression");
                fieldArgMinExpression.addProperty("op", "argmin");
                fieldArgMinExpression.addProperty("comparingExpression", "long * double");

                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("timestamp");
                fieldArgMinExpression.add("fields", targets);
                fields.add(fieldArgMinExpression);
            }
            {
                final JsonObject fieldLast = new JsonObject();
                fieldLast.addProperty("name", "last");
                fieldLast.addProperty("op", "last");

                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("date");
                targets.add("time");
                targets.add("timestamp");
                fieldLast.add("fields", targets);

                fields.add(fieldLast);
            }
            {
                final JsonObject fieldFirst = new JsonObject();
                fieldFirst.addProperty("name", "first");
                fieldFirst.addProperty("op", "first");

                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("long");
                targets.add("double");
                targets.add("bool");
                targets.add("date");
                targets.add("time");
                targets.add("timestamp");
                fieldFirst.add("fields", targets);

                fields.add(fieldFirst);
            }
            {
                final JsonObject fieldSumLong = new JsonObject();
                fieldSumLong.addProperty("name", "sumLong");
                fieldSumLong.addProperty("op", "sum");
                fieldSumLong.addProperty("field", "long");
                fields.add(fieldSumLong);
            }
            {
                final JsonObject fieldSumExpression = new JsonObject();
                fieldSumExpression.addProperty("name", "sumExpression");
                fieldSumExpression.addProperty("op", "sum");
                fieldSumExpression.addProperty("expression", "long * double");
                fields.add(fieldSumExpression);
            }
            {
                final JsonObject fieldAvgDouble = new JsonObject();
                fieldAvgDouble.addProperty("name", "avgDouble");
                fieldAvgDouble.addProperty("op", "avg");
                fieldAvgDouble.addProperty("field", "double");
                fields.add(fieldAvgDouble);
            }
            {
                final JsonObject fieldAvgWeightField = new JsonObject();
                fieldAvgWeightField.addProperty("name", "avgWeightField");
                fieldAvgWeightField.addProperty("op", "avg");
                fieldAvgWeightField.addProperty("field", "double");
                fieldAvgWeightField.addProperty("weightField", "long");
                fields.add(fieldAvgWeightField);
            }
            {
                final JsonObject fieldAvgExpression = new JsonObject();
                fieldAvgExpression.addProperty("name", "avgExpression");
                fieldAvgExpression.addProperty("op", "avg");
                fieldAvgExpression.addProperty("expression", "long * double");
                fields.add(fieldAvgExpression);
            }
            {
                final JsonObject fieldStdDouble = new JsonObject();
                fieldStdDouble.addProperty("name", "stdLong");
                fieldStdDouble.addProperty("op", "std");
                fieldStdDouble.addProperty("field", "long");
                //fields.add(fieldStdDouble);
            }
            {
                final JsonObject simpleRegression = new JsonObject();
                simpleRegression.addProperty("name", "simpleRegression");
                simpleRegression.addProperty("op", "regression");
                simpleRegression.addProperty("field", "double");
                fields.add(simpleRegression);
            }
            {
                final JsonObject arrayAgg = new JsonObject();
                arrayAgg.addProperty("name", "arrayAggDouble");
                arrayAgg.addProperty("op", "array_agg");
                arrayAgg.addProperty("field", "double");
                fields.add(arrayAgg);
            }
            {
                final JsonObject arrayAgg = new JsonObject();
                arrayAgg.addProperty("name", "arrayAggDoubleString");
                arrayAgg.addProperty("op", "array_agg");
                final JsonArray arrayAggFields = new JsonArray();
                arrayAggFields.add("double");
                arrayAggFields.add("string");
                arrayAggFields.add("timestamp");
                arrayAgg.add("fields", arrayAggFields);
                fields.add(arrayAgg);
            }
            definition.add("fields", fields);

            definitions.add(definition);
        }

        final JsonObject parameters = new JsonObject();
        parameters.add("groupFields", groupFields);

        final JsonObject windowParameters = new JsonObject();
        windowParameters.addProperty("type", "fixed");
        windowParameters.addProperty("unit", "minute");
        windowParameters.addProperty("size", 5);
        parameters.add("window", windowParameters);

        //final JsonObject triggerParameters = new JsonObject();
        //parameters.add("trigger", triggerParameters);

        parameters.add("aggregations", definitions);
        config.setParameters(parameters);

        final Schema inputSchema = Schema.builder()
                .addField(Schema.Field.of("id", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("string", Schema.FieldType.STRING.withNullable(true)))
                .addField(Schema.Field.of("bool", Schema.FieldType.BOOLEAN.withNullable(false)))
                .addField(Schema.Field.of("long", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("double", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("float", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("date", CalciteUtils.DATE.withNullable(true)))
                .addField(Schema.Field.of("time", CalciteUtils.TIME.withNullable(true)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        final Row row1 = Row.withSchema(inputSchema)
                .withFieldValue("id", "1")
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 1L)
                .withFieldValue("double", 100D)
                .withFieldValue("float", -10D)
                .withFieldValue("date", LocalDate.parse("2022-01-01"))
                .withFieldValue("time", LocalTime.parse("01:01:01.001"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(inputSchema)
                .withFieldValue("id", "2")
                .withFieldValue("string", "A")
                .withFieldValue("bool", true)
                .withFieldValue("long", 2L)
                .withFieldValue("double", 200D)
                .withFieldValue("float", -20D)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:01:01Z"))
                .build();
        final Row row3 = Row.withSchema(inputSchema)
                .withFieldValue("id", "3")
                .withFieldValue("string", "B")
                .withFieldValue("bool", true)
                .withFieldValue("long", 3L)
                .withFieldValue("double", null)
                .withFieldValue("float", -20D)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:02:02Z"))
                .build();
        final Row row4 = Row.withSchema(inputSchema)
                .withFieldValue("id", "4")
                .withFieldValue("string", "C")
                .withFieldValue("bool", true)
                .withFieldValue("long", 4L)
                .withFieldValue("double", 300D)
                .withFieldValue("float", 20D)
                .withFieldValue("date", LocalDate.parse("2022-03-03"))
                .withFieldValue("time", LocalTime.parse("03:03:03.003"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:03:03Z"))
                .build();
        final Row row5 = Row.withSchema(inputSchema)
                .withFieldValue("id", "5")
                .withFieldValue("string", "D")
                .withFieldValue("bool", true)
                .withFieldValue("long", 5L)
                .withFieldValue("double", 400D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-04-04"))
                .withFieldValue("time", LocalTime.parse("04:04:04.004"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:04:04Z"))
                .build();
        final Row row6 = Row.withSchema(inputSchema)
                .withFieldValue("id", "6")
                .withFieldValue("string", "E")
                .withFieldValue("bool", true)
                .withFieldValue("long", 6L)
                .withFieldValue("double", 500D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", null)
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:05:05Z"))
                .build();
        final Row row7 = Row.withSchema(inputSchema)
                .withFieldValue("id", "7")
                .withFieldValue("string", "F")
                .withFieldValue("bool", true)
                .withFieldValue("long", 7L)
                .withFieldValue("double", null)
                .withFieldValue("date", LocalDate.parse("2022-06-06"))
                .withFieldValue("time", LocalTime.parse("06:06:06.006"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:06:06Z"))
                .build();
        final Row row8 = Row.withSchema(inputSchema)
                .withFieldValue("id", "8")
                .withFieldValue("string", "G")
                .withFieldValue("bool", true)
                .withFieldValue("long", 8L)
                .withFieldValue("double", 600D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-07-07"))
                .withFieldValue("time", LocalTime.parse("07:07:07.007"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:07:07Z"))
                .build();
        final Row row9 = Row.withSchema(inputSchema)
                .withFieldValue("id", "9")
                .withFieldValue("string", "H")
                .withFieldValue("bool", true)
                .withFieldValue("long", 9L)
                .withFieldValue("double", 700D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-08-08"))
                .withFieldValue("time", LocalTime.parse("08:08:08.008"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:08:08Z"))
                .build();
        final Row row10 = Row.withSchema(inputSchema)
                .withFieldValue("id", "10")
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 10L)
                .withFieldValue("double", 800D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-09-09"))
                .withFieldValue("time", LocalTime.parse("09:09:09.009"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:09:09Z"))
                .build();
        final Row row11 = Row.withSchema(inputSchema)
                .withFieldValue("id", "11")
                .withFieldValue("string", "I")
                .withFieldValue("bool", true)
                .withFieldValue("long", 11L)
                .withFieldValue("double", 900D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-10-10"))
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:10:10Z"))
                .build();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create
                        .of(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11)
                        .withCoder(RowCoder.of(inputSchema)))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, inputSchema);

        final FCollection<?> outputs1 = AggregationTransform.transform(Arrays.asList(fCollection1), config);

        final Schema outputSchema1 = outputs1.getSchema();
        //System.out.println(outputSchema1);

        final PCollection<Row> output1 = (PCollection<Row>) outputs1.getCollection();
        PAssert.that(output1).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                //System.out.println(row);
                if("1".equals(row.getString("first_id"))) {
                    Assert.assertTrue(row.getBoolean("bool"));
                    Assert.assertEquals(5L, row.getInt64("count").longValue());
                    Assert.assertEquals(400D, row.getDouble("maxDouble").doubleValue(), DELTA);
                    Assert.assertEquals("D", row.getString("maxString"));
                    Assert.assertEquals(100D, row.getDouble("minExpression").doubleValue(), DELTA);

                    Assert.assertEquals("5", row.getString("argmaxLong_id"));
                    Assert.assertEquals(5L, row.getInt64("argmaxLong_long").longValue());
                    Assert.assertEquals(5L, row.getInt64("argmaxLongComparingValue").longValue());

                    Assert.assertEquals("1", row.getString("argminExpression_id"));
                    Assert.assertNull(row.getString("argminExpression_string"));

                    Assert.assertNull(row.getString("first_string"));
                    Assert.assertEquals(1L, row.getInt64("first_long").longValue());
                    Assert.assertEquals(100D, row.getDouble("first_double").doubleValue(), DELTA);
                    Assert.assertEquals(true, row.getBoolean("first_bool"));
                    Assert.assertEquals(LocalDate.of(2022,1,1), row.getLogicalTypeValue("first_date", LocalDate.class));
                    Assert.assertEquals(LocalTime.of(1,1,1, 1000_000), row.getLogicalTypeValue("first_time", LocalTime.class));
                    Assert.assertEquals("5", row.getString("last_id"));
                    Assert.assertEquals("D", row.getString("last_string"));
                    Assert.assertEquals(LocalDate.of(2022,4,4), row.getLogicalTypeValue("last_date", LocalDate.class));
                    Assert.assertEquals(LocalTime.of(4,4,4, 4000_000), row.getLogicalTypeValue("last_time", LocalTime.class));

                    Assert.assertEquals(15L, row.getInt64("sumLong").longValue());
                    Assert.assertEquals(3700D, row.getDouble("sumExpression").doubleValue(), DELTA);
                    Assert.assertEquals(250D, row.getDouble("avgDouble").doubleValue(), DELTA);
                    Assert.assertEquals(925D, row.getDouble("avgExpression").doubleValue(), DELTA);
                    //Assert.assertEquals(1.5811388300841898D, row.getDouble("stdLong"), DELTA);

                    count += 1;
                } else if("6".equals(row.getString("first_id"))) {
                    Assert.assertTrue(row.getBoolean("bool"));
                    Assert.assertEquals(5L, row.getInt64("count").longValue());
                    Assert.assertEquals("E", row.getString("first_string"));
                    //Assert.assertEquals(1.5811388300841898D, row.getDouble("stdLong"), DELTA);
                    Assert.assertNull(row.getString("last_string"));

                    count += 1;
                } else if("11".equals(row.getString("first_id"))) {
                    Assert.assertTrue(row.getBoolean("bool"));
                    Assert.assertEquals(1L, row.getInt64("count").longValue());
                    Assert.assertEquals(900D, row.getDouble("maxDouble").doubleValue(), DELTA);
                    Assert.assertEquals("I", row.getString("maxString"));
                    Assert.assertEquals(9900D, row.getDouble("minExpression").doubleValue(), DELTA);

                    Assert.assertEquals(11L, row.getInt64("first_long").longValue());


                    Assert.assertEquals("I", row.getString("first_string"));
                    Assert.assertEquals("I", row.getString("last_string"));
                    Assert.assertEquals("11", row.getString("last_id"));

                    Assert.assertEquals(LocalDate.of(2022,10,10), row.getLogicalTypeValue("first_date", LocalDate.class));
                    Assert.assertEquals(LocalDate.of(2022,10,10), row.getLogicalTypeValue("last_date", LocalDate.class));
                    Assert.assertNull(row.getLogicalTypeValue("first_time", LocalTime.class));
                    Assert.assertNull(row.getLogicalTypeValue("last_time", LocalTime.class));

                    Assert.assertEquals(11L, row.getInt64("sumLong").longValue());
                    Assert.assertEquals(900D, row.getDouble("avgDouble").doubleValue(), DELTA);
                    Assert.assertEquals(9900D, row.getDouble("avgExpression").doubleValue(), DELTA);

                    count += 1;
                }
                //System.out.println("row; " + row);
            }
            Assert.assertEquals(3, count);
            return null;
        });

        pipeline.run();

    }

    @Test
    public void testAggregationFilterAndSelect() {

        final TransformConfig config = new TransformConfig();
        config.setName("aggregation");
        config.setModule("aggregation");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray definitions = new JsonArray();
        {
            final JsonObject definition = new JsonObject();
            definition.addProperty("input", "rowInput");

            final JsonArray fields = new JsonArray();

            final JsonObject fieldCount = new JsonObject();
            fieldCount.addProperty("name", "count");
            fieldCount.addProperty("op", "count");
            fields.add(fieldCount);

            final JsonObject fieldMaxDouble = new JsonObject();
            fieldMaxDouble.addProperty("name", "maxDouble");
            fieldMaxDouble.addProperty("op", "max");
            fieldMaxDouble.addProperty("field", "double");
            fields.add(fieldMaxDouble);

            final JsonObject fieldStringDouble = new JsonObject();
            fieldStringDouble.addProperty("name", "maxString");
            fieldStringDouble.addProperty("op", "max");
            fieldStringDouble.addProperty("field", "string");
            fields.add(fieldStringDouble);

            final JsonObject fieldMinExpression = new JsonObject();
            fieldMinExpression.addProperty("name", "minExpression");
            fieldMinExpression.addProperty("op", "min");
            fieldMinExpression.addProperty("expression", "long * double");
            fields.add(fieldMinExpression);

            final JsonObject fieldArgMaxLong = new JsonObject();
            fieldArgMaxLong.addProperty("name", "argmaxLong");
            fieldArgMaxLong.addProperty("op", "argmax");
            fieldArgMaxLong.addProperty("comparingField", "long");
            fieldArgMaxLong.addProperty("comparingValueField", "argmaxLongComparingValue");
            {
                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("long");
                targets.add("timestamp");
                fieldArgMaxLong.add("fields", targets);
            }
            fields.add(fieldArgMaxLong);

            final JsonObject fieldArgMinExpression = new JsonObject();
            fieldArgMinExpression.addProperty("name", "argminExpression");
            fieldArgMinExpression.addProperty("op", "argmin");
            fieldArgMinExpression.addProperty("comparingExpression", "long * double");
            {
                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("timestamp");
                fieldArgMinExpression.add("fields", targets);
            }
            fields.add(fieldArgMinExpression);

            final JsonObject fieldLast = new JsonObject();
            fieldLast.addProperty("name", "last");
            fieldLast.addProperty("op", "last");
            {
                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("date");
                targets.add("time");
                targets.add("timestamp");
                fieldLast.add("fields", targets);
            }
            fields.add(fieldLast);

            final JsonObject fieldFirst = new JsonObject();
            fieldFirst.addProperty("name", "first");
            fieldFirst.addProperty("op", "first");
            {
                final JsonArray targets = new JsonArray();
                targets.add("id");
                targets.add("string");
                targets.add("long");
                targets.add("double");
                targets.add("bool");
                targets.add("date");
                targets.add("time");
                targets.add("timestamp");
                fieldFirst.add("fields", targets);
            }
            fields.add(fieldFirst);

            final JsonObject fieldSumLong = new JsonObject();
            fieldSumLong.addProperty("name", "sumLong");
            fieldSumLong.addProperty("op", "sum");
            fieldSumLong.addProperty("field", "long");
            fields.add(fieldSumLong);

            final JsonObject fieldSumExpression = new JsonObject();
            fieldSumExpression.addProperty("name", "sumExpression");
            fieldSumExpression.addProperty("op", "sum");
            fieldSumExpression.addProperty("expression", "long * double");
            fields.add(fieldSumExpression);

            final JsonObject fieldAvgDouble = new JsonObject();
            fieldAvgDouble.addProperty("name", "avgDouble");
            fieldAvgDouble.addProperty("op", "avg");
            fieldAvgDouble.addProperty("field", "double");
            fields.add(fieldAvgDouble);

            final JsonObject fieldAvgWeightField = new JsonObject();
            fieldAvgWeightField.addProperty("name", "avgWeightField");
            fieldAvgWeightField.addProperty("op", "avg");
            fieldAvgWeightField.addProperty("field", "double");
            fieldAvgWeightField.addProperty("weightField", "long");
            fields.add(fieldAvgWeightField);

            final JsonObject fieldAvgExpression = new JsonObject();
            fieldAvgExpression.addProperty("name", "avgExpression");
            fieldAvgExpression.addProperty("op", "avg");
            fieldAvgExpression.addProperty("expression", "long * double");
            fields.add(fieldAvgExpression);

            final JsonObject fieldStdDouble = new JsonObject();
            fieldStdDouble.addProperty("name", "stdLong");
            fieldStdDouble.addProperty("op", "std");
            fieldStdDouble.addProperty("field", "long");
            //fields.add(fieldStdDouble);

            final JsonObject simpleRegression = new JsonObject();
            simpleRegression.addProperty("name", "simpleRegression");
            simpleRegression.addProperty("op", "regression");
            simpleRegression.addProperty("field", "double");
            fields.add(simpleRegression);

            definition.add("fields", fields);

            definitions.add(definition);
        }

        final JsonObject parameters = new JsonObject();

        final JsonArray groupFields = new JsonArray();
        groupFields.add("bool");
        parameters.add("groupFields", groupFields);

        final JsonObject windowParameters = new JsonObject();
        windowParameters.addProperty("type", "fixed");
        windowParameters.addProperty("unit", "minute");
        windowParameters.addProperty("size", 5);
        parameters.add("window", windowParameters);

        //final JsonObject triggerParameters = new JsonObject();
        //parameters.add("trigger", triggerParameters);

        final JsonObject filter = new JsonObject();
        filter.addProperty("key", "maxString");
        filter.addProperty("op", "=");
        filter.addProperty("value", "D");
        parameters.add("filter", filter);

        final JsonArray select = new JsonArray();
        {
            final JsonObject countField = new JsonObject();
            countField.addProperty("name", "count");
            select.add(countField);
        }
        {
            final JsonObject renameBoolField = new JsonObject();
            renameBoolField.addProperty("name", "boolField");
            renameBoolField.addProperty("field", "bool");
            select.add(renameBoolField);
        }
        {
            final JsonObject renameTimestampField = new JsonObject();
            renameTimestampField.addProperty("name", "timestampField");
            renameTimestampField.addProperty("field", "timestamp");
            select.add(renameTimestampField);
        }
        {
            final JsonObject boolField = new JsonObject();
            boolField.addProperty("name", "firstID");
            boolField.addProperty("field", "first_id");
            select.add(boolField);
        }
        {
            final JsonObject constantStringField = new JsonObject();
            constantStringField.addProperty("name", "constantStringField");
            constantStringField.addProperty("type", "string");
            constantStringField.addProperty("value", "CONSTANT_STRING");
            select.add(constantStringField);
        }
        {
            final JsonObject constantTimestampField = new JsonObject();
            constantTimestampField.addProperty("name", "constantTimestampField");
            constantTimestampField.addProperty("type", "timestamp");
            constantTimestampField.addProperty("value", "2023-08-05T00:00:00Z");
            select.add(constantTimestampField);
        }
        {
            final JsonObject currentTimestampField = new JsonObject();
            currentTimestampField.addProperty("name", "currentTimestampField");
            currentTimestampField.addProperty("func", "current_timestamp");
            select.add(currentTimestampField);
        }
        {
            final JsonObject expressionField = new JsonObject();
            expressionField.addProperty("name", "expressionField");
            expressionField.addProperty("expression", "timestamp_diff_day(constantTimestampField, timestamp)");
            select.add(expressionField);
        }
        {
            final JsonObject hashField = new JsonObject();
            hashField.addProperty("name", "hashField");
            hashField.addProperty("func", "hash");
            hashField.addProperty("field", "last_string");
            hashField.addProperty("secret", "my secret key");
            select.add(hashField);
        }
        parameters.add("select", select);


        parameters.add("aggregations", definitions);
        config.setParameters(parameters);

        final Schema inputSchema = Schema.builder()
                .addField(Schema.Field.of("id", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("string", Schema.FieldType.STRING.withNullable(true)))
                .addField(Schema.Field.of("bool", Schema.FieldType.BOOLEAN.withNullable(false)))
                .addField(Schema.Field.of("long", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("double", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("float", Schema.FieldType.DOUBLE.withNullable(true)))
                .addField(Schema.Field.of("date", CalciteUtils.DATE.withNullable(true)))
                .addField(Schema.Field.of("time", CalciteUtils.TIME.withNullable(true)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        final Row row1 = Row.withSchema(inputSchema)
                .withFieldValue("id", "1")
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 1L)
                .withFieldValue("double", 100D)
                .withFieldValue("float", -10D)
                .withFieldValue("date", LocalDate.parse("2022-01-01"))
                .withFieldValue("time", LocalTime.parse("01:01:01.001"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(inputSchema)
                .withFieldValue("id", "2")
                .withFieldValue("string", "A")
                .withFieldValue("bool", true)
                .withFieldValue("long", 2L)
                .withFieldValue("double", 200D)
                .withFieldValue("float", -20D)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:01:01Z"))
                .build();
        final Row row3 = Row.withSchema(inputSchema)
                .withFieldValue("id", "3")
                .withFieldValue("string", "B")
                .withFieldValue("bool", true)
                .withFieldValue("long", 3L)
                .withFieldValue("double", null)
                .withFieldValue("float", -20D)
                .withFieldValue("date", LocalDate.parse("2022-02-02"))
                .withFieldValue("time", LocalTime.parse("02:02:02.002"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:02:02Z"))
                .build();
        final Row row4 = Row.withSchema(inputSchema)
                .withFieldValue("id", "4")
                .withFieldValue("string", "C")
                .withFieldValue("bool", true)
                .withFieldValue("long", 4L)
                .withFieldValue("double", 300D)
                .withFieldValue("float", 20D)
                .withFieldValue("date", LocalDate.parse("2022-03-03"))
                .withFieldValue("time", LocalTime.parse("03:03:03.003"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:03:03Z"))
                .build();
        final Row row5 = Row.withSchema(inputSchema)
                .withFieldValue("id", "5")
                .withFieldValue("string", "D")
                .withFieldValue("bool", true)
                .withFieldValue("long", 5L)
                .withFieldValue("double", 400D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-04-04"))
                .withFieldValue("time", LocalTime.parse("04:04:04.004"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:04:04Z"))
                .build();
        final Row row6 = Row.withSchema(inputSchema)
                .withFieldValue("id", "6")
                .withFieldValue("string", "E")
                .withFieldValue("bool", true)
                .withFieldValue("long", 6L)
                .withFieldValue("double", 500D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", null)
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:05:05Z"))
                .build();
        final Row row7 = Row.withSchema(inputSchema)
                .withFieldValue("id", "7")
                .withFieldValue("string", "F")
                .withFieldValue("bool", true)
                .withFieldValue("long", 7L)
                .withFieldValue("double", null)
                .withFieldValue("date", LocalDate.parse("2022-06-06"))
                .withFieldValue("time", LocalTime.parse("06:06:06.006"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:06:06Z"))
                .build();
        final Row row8 = Row.withSchema(inputSchema)
                .withFieldValue("id", "8")
                .withFieldValue("string", "G")
                .withFieldValue("bool", true)
                .withFieldValue("long", 8L)
                .withFieldValue("double", 600D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-07-07"))
                .withFieldValue("time", LocalTime.parse("07:07:07.007"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:07:07Z"))
                .build();
        final Row row9 = Row.withSchema(inputSchema)
                .withFieldValue("id", "9")
                .withFieldValue("string", "H")
                .withFieldValue("bool", true)
                .withFieldValue("long", 9L)
                .withFieldValue("double", 700D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-08-08"))
                .withFieldValue("time", LocalTime.parse("08:08:08.008"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:08:08Z"))
                .build();
        final Row row10 = Row.withSchema(inputSchema)
                .withFieldValue("id", "10")
                .withFieldValue("string", null)
                .withFieldValue("bool", true)
                .withFieldValue("long", 10L)
                .withFieldValue("double", 800D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-09-09"))
                .withFieldValue("time", LocalTime.parse("09:09:09.009"))
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:09:09Z"))
                .build();
        final Row row11 = Row.withSchema(inputSchema)
                .withFieldValue("id", "11")
                .withFieldValue("string", "I")
                .withFieldValue("bool", true)
                .withFieldValue("long", 11L)
                .withFieldValue("double", 900D)
                .withFieldValue("float", 30D)
                .withFieldValue("date", LocalDate.parse("2022-10-10"))
                .withFieldValue("time", null)
                .withFieldValue("timestamp", Instant.parse("2022-01-01T00:10:10Z"))
                .build();

        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create
                        .of(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11)
                        .withCoder(RowCoder.of(inputSchema)))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, inputSchema);

        final FCollection<?> outputs1 = AggregationTransform.transform(Arrays.asList(fCollection1), config);

        final Schema outputSchema = outputs1.getSchema();
        Assert.assertEquals(9, outputSchema.getFieldCount());
        Assert.assertEquals(Schema.TypeName.INT64, outputSchema.getField("count").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.BOOLEAN, outputSchema.getField("boolField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, outputSchema.getField("timestampField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("firstID").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("constantStringField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, outputSchema.getField("constantTimestampField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, outputSchema.getField("currentTimestampField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, outputSchema.getField("expressionField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("hashField").getType().getTypeName());


        final PCollection<Row> output1 = (PCollection<Row>) outputs1.getCollection();
        PAssert.that(output1).satisfies(rows -> {
            int count = 0;
            for(final Row row : rows) {
                if("1".equals(row.getString("firstID"))) {
                    Assert.assertTrue(row.getBoolean("boolField"));
                    Assert.assertEquals(5L, row.getInt64("count").longValue());
                    Assert.assertEquals("CONSTANT_STRING", row.getString("constantStringField"));
                    Assert.assertEquals(580D, row.getDouble("expressionField").doubleValue(), DELTA);
                    Assert.assertEquals(Instant.parse("2023-08-05T00:00:00.000Z").getMillis(), row.getDateTime("constantTimestampField").toInstant().getMillis());
                    Assert.assertEquals("418934e57a5fd45999ce70926e7969380584648b4b1da0be61bbe6c118675685", row.getString("hashField"));

                    count += 1;
                }
            }
            Assert.assertEquals(1, count);
            return null;
        });

        pipeline.run();

    }

}
