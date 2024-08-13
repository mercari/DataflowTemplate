package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectFunctionTest {

    @Test
    public void test() {

        final JsonArray select = new JsonArray();
        {
            final JsonObject pass = new JsonObject();
            pass.addProperty("name", "longField");
            select.add(pass);
        }
        {
            final JsonObject pass = new JsonObject();
            pass.addProperty("name", "enumField");
            select.add(pass);
        }
        {
            final JsonObject rename = new JsonObject();
            rename.addProperty("name", "renameIntField");
            rename.addProperty("field", "intField");
            select.add(rename);
        }
        {
            final JsonObject constant = new JsonObject();
            constant.addProperty("name", "constantStringField");
            constant.addProperty("type", "string");
            constant.addProperty("value", "constantStringValue");
            select.add(constant);
        }
        {
            final JsonObject expression = new JsonObject();
            expression.addProperty("name", "expressionField");
            expression.addProperty("expression", "doubleField * intField / longField");
            select.add(expression);
        }
        {
            final JsonObject hash = new JsonObject();
            hash.addProperty("name", "hashField");
            hash.addProperty("func", "hash");
            hash.addProperty("field", "stringField");
            select.add(hash);
        }
        {
            final JsonObject hash = new JsonObject();
            hash.addProperty("name", "hashArrayField");
            hash.addProperty("func", "hash");
            final JsonArray fields = new JsonArray();
            fields.add("stringField");
            fields.add("longField");
            hash.add("fields", fields);
            select.add(hash);
        }
        {
            final JsonObject currentTimestamp = new JsonObject();
            currentTimestamp.addProperty("name", "currentTimestampField");
            currentTimestamp.addProperty("func", "current_timestamp");
            select.add(currentTimestamp);
        }
        {
            final JsonObject currentTimestamp = new JsonObject();
            currentTimestamp.addProperty("name", "eventTimestampField");
            currentTimestamp.addProperty("func", "event_timestamp");
            select.add(currentTimestamp);
        }
        {
            final JsonObject concat = new JsonObject();
            concat.addProperty("name", "concatField");
            concat.addProperty("func", "concat");
            concat.addProperty("delimiter", " ");
            final JsonArray fields = new JsonArray();
            fields.add("stringField");
            fields.add("intField");
            fields.add("longField");
            concat.add("fields", fields);
            select.add(concat);
        }
        {
            final JsonObject struct = new JsonObject();
            struct.addProperty("name", "structField");
            struct.addProperty("func", "struct");
            struct.addProperty("mode", "repeated");
            final JsonArray fields = new JsonArray();
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "stringFieldA");
                field.addProperty("field", "stringField");
                fields.add(field);
            }
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "enumField");
                fields.add(field);
            }
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "intFieldA");
                field.addProperty("field", "intField");
                fields.add(field);
            }
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "nestedStructField");
                field.addProperty("func", "struct");
                final JsonArray nestedFields = new JsonArray();
                {
                    final JsonObject nestedField = new JsonObject();
                    nestedField.addProperty("name", "nestedNestedStructField");
                    nestedField.addProperty("func", "struct");
                    final JsonArray nestedNestedFields = new JsonArray();
                    {
                        final JsonObject nestedNestedField = new JsonObject();
                        nestedNestedField.addProperty("name", "timestampField");
                        nestedNestedFields.add(nestedNestedField);
                    }
                    {
                        final JsonObject nestedNestedField = new JsonObject();
                        nestedNestedField.addProperty("name", "enumField2");
                        nestedNestedField.addProperty("field", "enumField");
                        nestedNestedFields.add(nestedNestedField);
                    }
                    nestedField.add("fields", nestedNestedFields);
                    nestedFields.add(nestedField);
                }
                field.add("fields", nestedFields);
                fields.add(field);
            }
            struct.add("fields", fields);
            select.add(struct);
        }
        {
            final JsonObject map = new JsonObject();
            map.addProperty("name", "mapField");
            map.addProperty("func", "map");
            final JsonArray fields = new JsonArray();
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "stringFieldA");
                field.addProperty("field", "stringField");
                fields.add(field);
            }
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "stringFieldB");
                field.addProperty("field", "stringField");
                fields.add(field);
            }
            map.add("fields", fields);
            select.add(map);
        }
        {
            final JsonObject json = new JsonObject();
            json.addProperty("name", "jsonField");
            json.addProperty("func", "json");
            final JsonArray fields = new JsonArray();
            {
                final JsonObject field = new JsonObject();
                field.addProperty("name", "stringFieldA");
                field.addProperty("field", "stringField");
                fields.add(field);
            }
            {
                final JsonObject nestedField = new JsonObject();
                nestedField.addProperty("name", "nestedStructField");
                nestedField.addProperty("func", "struct");
                final JsonArray nestedNestedFields = new JsonArray();
                {
                    final JsonObject nestedNestedField = new JsonObject();
                    nestedNestedField.addProperty("name", "timestampField");
                    nestedNestedFields.add(nestedNestedField);
                }
                {
                    final JsonObject nestedNestedField = new JsonObject();
                    nestedNestedField.addProperty("name", "enumField2");
                    nestedNestedField.addProperty("field", "enumField");
                    nestedNestedFields.add(nestedNestedField);
                }
                nestedField.add("fields", nestedNestedFields);
                fields.add(nestedField);
            }
            json.add("fields", fields);
            select.add(json);
        }

        final List<Schema.Field> inputFields = new ArrayList<>();
        inputFields.add(Schema.Field.of("stringField", Schema.FieldType.STRING));
        inputFields.add(Schema.Field.of("intField", Schema.FieldType.INT32));
        inputFields.add(Schema.Field.of("longField", Schema.FieldType.INT64));
        inputFields.add(Schema.Field.of("floatField", Schema.FieldType.FLOAT));
        inputFields.add(Schema.Field.of("doubleField", Schema.FieldType.DOUBLE));
        inputFields.add(Schema.Field.of("booleanField", Schema.FieldType.BOOLEAN));
        inputFields.add(Schema.Field.of("bytesField", Schema.FieldType.BYTES));
        inputFields.add(Schema.Field.of("dateField", CalciteUtils.DATE));
        inputFields.add(Schema.Field.of("timestampField", Schema.FieldType.DATETIME));
        inputFields.add(Schema.Field.of("enumField", Schema.FieldType.logicalType(EnumerationType.create(List.of("a", "b", "c")))));

        // test schema
        final List<SelectFunction> selectFunctions = SelectFunction.of(select, inputFields, DataType.ROW);
        final Schema outputSchema = SelectFunction.createSchema(selectFunctions);
        Assert.assertTrue(outputSchema.hasField("longField"));
        Assert.assertTrue(outputSchema.hasField("renameIntField"));
        Assert.assertTrue(outputSchema.hasField("constantStringField"));
        Assert.assertTrue(outputSchema.hasField("expressionField"));
        Assert.assertTrue(outputSchema.hasField("hashField"));
        Assert.assertTrue(outputSchema.hasField("hashArrayField"));
        Assert.assertTrue(outputSchema.hasField("currentTimestampField"));
        Assert.assertTrue(outputSchema.hasField("eventTimestampField"));
        Assert.assertTrue(outputSchema.hasField("concatField"));
        Assert.assertTrue(outputSchema.hasField("structField"));
        Assert.assertTrue(outputSchema.hasField("mapField"));
        Assert.assertTrue(outputSchema.hasField("jsonField"));
        Assert.assertEquals(Schema.TypeName.INT64, outputSchema.getField("longField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.INT32, outputSchema.getField("renameIntField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("constantStringField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DOUBLE, outputSchema.getField("expressionField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("hashField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("hashArrayField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, outputSchema.getField("currentTimestampField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.DATETIME, outputSchema.getField("eventTimestampField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("concatField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.ARRAY, outputSchema.getField("structField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.MAP, outputSchema.getField("mapField").getType().getTypeName());
        Assert.assertEquals(Schema.TypeName.STRING, outputSchema.getField("jsonField").getType().getTypeName());

        // test apply
        final Schema inputSchema = Schema.builder().addFields(inputFields).build();
        final Row row = Row.withSchema(inputSchema)
                .withFieldValue("stringField", "stringValue")
                .withFieldValue("intField", 32)
                .withFieldValue("longField", 10L)
                .withFieldValue("floatField", -5.5F)
                .withFieldValue("doubleField", 10.10D)
                .withFieldValue("booleanField", true)
                .withFieldValue("bytesField", "bytesValue".getBytes(StandardCharsets.UTF_8))
                .withFieldValue("dateField", LocalDate.parse("2023-08-18"))
                .withFieldValue("timestampField", Instant.parse("2023-08-18T00:00:00Z"))
                .withFieldValue("enumField", new EnumerationType.Value(1))
                .build();

        selectFunctions.forEach(SelectFunction::setup);

        final Instant timestamp = Instant.parse("2024-01-01T00:00:00Z");
        final Map<String, Object> values = SelectFunction.apply(selectFunctions, row, DataType.ROW, DataType.ROW, timestamp);
        final Row output = RowSchemaUtil.create(outputSchema, values);

        Assert.assertEquals((Long) 10L, output.getInt64("longField"));
        Assert.assertEquals((Integer) 32, output.getInt32("renameIntField"));
        Assert.assertEquals("constantStringValue", output.getString("constantStringField"));
        Assert.assertEquals((Double)32.32, output.getDouble("expressionField"));
        Assert.assertEquals("dbcc96aec884f7d5057672df21e7446c1415ca7669fdabac78e49f4d852d5a0a", output.getString("hashField"));
        Assert.assertEquals("e6c8c04775d64362367b36c61dc00615eabd1c00887fec05ceae4e5ab9cd215b", output.getString("hashArrayField"));
        Assert.assertNotNull(output.getDateTime("currentTimestampField"));
        Assert.assertEquals(timestamp, output.getDateTime("eventTimestampField").toInstant());
        Assert.assertEquals("stringValue 32 10", output.getString("concatField"));

        final JsonObject jsonObject = new Gson().fromJson(output.getString("jsonField"), JsonObject.class);
        Assert.assertEquals("stringValue", jsonObject.get("stringFieldA").getAsString());
        final JsonObject nestedJsonObject = jsonObject.get("nestedStructField").getAsJsonObject();
        Assert.assertEquals("2023-08-18T00:00:00.000Z", nestedJsonObject.get("timestampField").getAsString());
        Assert.assertEquals("b", nestedJsonObject.get("enumField2").getAsString());
    }

}
