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

public class BarTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testTimeBar() {
        final TransformConfig config = new TransformConfig();
        config.setName("bar1");
        config.setModule("bar");
        config.setInputs(Arrays.asList("rowInput"));

        final JsonArray symbolFields = new JsonArray();
        symbolFields.add("symbol");

        final JsonObject parameters = new JsonObject();
        parameters.add("symbolFields", symbolFields);
        parameters.addProperty("priceField", "price");
        parameters.addProperty("volumeField", "volume");
        parameters.addProperty("timestampField", "timestamp");
        parameters.addProperty("type", "time");
        parameters.addProperty("unit", "minute");
        parameters.addProperty("size", 1L);
        config.setParameters(parameters);

        final List<Row> rows = createRows();
        final Schema schema = rows.get(0).getSchema();

        // With window buffer
        final PCollection<Row> inputRows1 = pipeline
                .apply("CreateDummy1", Create.of(rows))
                .apply("WithTimestamp", WithTimestamps.of(r -> r.getDateTime("timestamp").toInstant()));
        final FCollection<Row> fCollection1 = FCollection.of("rowInput", inputRows1, DataType.ROW, schema);

        final Map<String, FCollection<?>> outputs1 = BarTransform.transformTimeBar(Arrays.asList(fCollection1), config);
        final PCollection<GenericRecord> outputBars1 = (PCollection<GenericRecord>) outputs1.get("bar1").getCollection();

        PAssert.that(outputBars1).satisfies(bars -> {
            final List<GenericRecord> barsList = Lists.newArrayList(bars);
            barsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());

            Assert.assertEquals(8, barsList.size());
            for(int i=0; i<barsList.size(); i++) {
                Assert.assertEquals("0001", barsList.get(i).get("symbol").toString());
                if(i == 3 || i == 7) {
                    Assert.assertEquals(1L, barsList.get(i).get("count"));
                    Assert.assertEquals(10D, barsList.get(i).get("volume"));
                    Assert.assertEquals(0L, barsList.get(i).get("sizeSecond"));
                    Assert.assertEquals(5D, barsList.get(i).get("vwap"));
                } else {
                    Assert.assertEquals(3L, barsList.get(i).get("count"));
                    Assert.assertEquals(40L, barsList.get(i).get("sizeSecond"));
                    if(i == 0) {
                        Assert.assertEquals(1D, barsList.get(i).get("low"));
                        Assert.assertEquals(3D, barsList.get(i).get("high"));
                        Assert.assertEquals(1D, barsList.get(i).get("open"));
                        Assert.assertEquals(3D, barsList.get(i).get("close"));
                        Assert.assertEquals(6D, barsList.get(i).get("volume"));
                    }
                }
            }

            return null;
        });

        //
        config.setName("bar2");
        parameters.addProperty("type", "time");
        parameters.addProperty("unit", "day");
        parameters.addProperty("size", 1);
        parameters.addProperty("isVolumeAccumulative", true);
        config.setParameters(parameters);

        final Map<String, FCollection<?>> outputs2 = BarTransform.transformTimeBar(Arrays.asList(fCollection1), config);
        final PCollection<GenericRecord> outputBars2 = (PCollection<GenericRecord>) outputs2.get("bar2").getCollection();

        PAssert.that(outputBars2).satisfies(bars -> {
            final List<GenericRecord> barsList = Lists.newArrayList(bars);
            barsList.sort((r1, r2) -> Long.valueOf((long)r1.get("timestamp") - (long)r2.get("timestamp")).intValue());

            Assert.assertEquals(2, barsList.size());
            // day1
            Assert.assertEquals("0001", barsList.get(0).get("symbol").toString());
            Assert.assertEquals(1D, barsList.get(0).get("low"));
            Assert.assertEquals(8D, barsList.get(0).get("high"));
            Assert.assertEquals(1D, barsList.get(0).get("open"));
            Assert.assertEquals(5D, barsList.get(0).get("close"));
            Assert.assertEquals(4.1D, barsList.get(0).get("vwap"));
            Assert.assertEquals(10D, barsList.get(0).get("volume"));
            Assert.assertEquals(10L, barsList.get(0).get("count"));
            Assert.assertEquals(180L, barsList.get(0).get("sizeSecond"));
            // day2
            Assert.assertEquals("0001", barsList.get(1).get("symbol").toString());
            Assert.assertEquals(4D, barsList.get(1).get("low"));
            Assert.assertEquals(8D, barsList.get(1).get("high"));
            Assert.assertEquals(6D, barsList.get(1).get("open"));
            Assert.assertEquals(5D, barsList.get(1).get("close"));
            Assert.assertEquals(5.9D, barsList.get(1).get("vwap"));
            Assert.assertEquals(10D, barsList.get(1).get("volume"));
            Assert.assertEquals(10L, barsList.get(1).get("count"));
            Assert.assertEquals(180L, barsList.get(1).get("sizeSecond"));

            return null;
        });

        pipeline.run();
    }

    private List<Row> createRows() {
        Schema schema = Schema.builder()
                .addField(Schema.Field.of("symbol", Schema.FieldType.STRING.withNullable(false)))
                .addField(Schema.Field.of("price", Schema.FieldType.DOUBLE.withNullable(false)))
                .addField(Schema.Field.of("volume", Schema.FieldType.INT64.withNullable(false)))
                .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        // Day1
        final Row row1 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 1D)
                .withFieldValue("volume", 1L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:00Z"))
                .build();
        final Row row2 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 2D)
                .withFieldValue("volume", 2L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:20Z"))
                .build();
        final Row row3 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 3D)
                .withFieldValue("volume", 3L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:00:40Z"))
                .build();
        final Row row4 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 2D)
                .withFieldValue("volume", 4L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:01:00Z"))
                .build();
        final Row row5 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 3D)
                .withFieldValue("volume", 5L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:01:20Z"))
                .build();
        final Row row6 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 4D)
                .withFieldValue("volume", 6L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:01:40Z"))
                .build();
        final Row row7 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 8D)
                .withFieldValue("volume", 7L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:02:00Z"))
                .build();
        final Row row8 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 7D)
                .withFieldValue("volume", 8L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:02:20Z"))
                .build();
        final Row row9 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 6D)
                .withFieldValue("volume", 9L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:02:40Z"))
                .build();
        final Row row10 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 5D)
                .withFieldValue("volume", 10L)
                .withFieldValue("timestamp", Instant.parse("2021-04-11T00:03:00Z"))
                .build();

        // Day2
        final Row row11 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 6D)
                .withFieldValue("volume", 1L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:00:00Z"))
                .build();
        final Row row12 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 5D)
                .withFieldValue("volume", 2L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:00:20Z"))
                .build();
        final Row row13 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 4D)
                .withFieldValue("volume", 3L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:00:40Z"))
                .build();
        final Row row14 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 5D)
                .withFieldValue("volume", 4L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:01:00Z"))
                .build();
        final Row row15 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 6D)
                .withFieldValue("volume", 5L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:01:20Z"))
                .build();
        final Row row16 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 7D)
                .withFieldValue("volume", 6L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:01:40Z"))
                .build();
        final Row row17 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 8D)
                .withFieldValue("volume", 7L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:02:00Z"))
                .build();
        final Row row18 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 7D)
                .withFieldValue("volume", 8L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:02:20Z"))
                .build();
        final Row row19 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 6D)
                .withFieldValue("volume", 9L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:02:40Z"))
                .build();
        final Row row20 = Row.withSchema(schema)
                .withFieldValue("symbol", "0001")
                .withFieldValue("price", 5D)
                .withFieldValue("volume", 10L)
                .withFieldValue("timestamp", Instant.parse("2021-04-12T00:03:00Z"))
                .build();

        return Arrays.asList(
                row1, row2, row3, row4, row5, row6, row7, row8, row9, row10,
                row11, row12, row13, row14, row15, row16, row17, row18, row19, row20);
    }

}
