package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
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
            hash.addProperty("secret", "mysecret");
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
            hash.addProperty("secret", "mysecret");
            select.add(hash);
        }
        {
            final JsonObject currentTimestamp = new JsonObject();
            currentTimestamp.addProperty("name", "currentTimestampField");
            currentTimestamp.addProperty("func", "current_timestamp");
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
        Assert.assertTrue(outputSchema.hasField("concatField"));
        Assert.assertEquals(Schema.FieldType.INT64.getTypeName(), outputSchema.getField("longField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.INT32.getTypeName(), outputSchema.getField("renameIntField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.STRING.getTypeName(), outputSchema.getField("constantStringField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.DOUBLE.getTypeName(), outputSchema.getField("expressionField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.STRING.getTypeName(), outputSchema.getField("hashField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.STRING.getTypeName(), outputSchema.getField("hashArrayField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.DATETIME.getTypeName(), outputSchema.getField("currentTimestampField").getType().getTypeName());
        Assert.assertEquals(Schema.FieldType.STRING.getTypeName(), outputSchema.getField("concatField").getType().getTypeName());

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
                .build();

        selectFunctions.forEach(SelectFunction::setup);

        final Map<String, Object> values = SelectFunction.apply(selectFunctions, row, DataType.ROW, DataType.ROW);
        final Row output = RowSchemaUtil.create(outputSchema, values);

        Assert.assertEquals((Long) 10L, output.getInt64("longField"));
        Assert.assertEquals((Integer) 32, output.getInt32("renameIntField"));
        Assert.assertEquals("constantStringValue", output.getString("constantStringField"));
        Assert.assertEquals((Double)32.32, output.getDouble("expressionField"));
        Assert.assertEquals("3f6f120a3879bfbcb45bfb78a0ce80a0d53de6c8b4e5b0b9b94f282a6b0a2b63", output.getString("hashField"));
        Assert.assertEquals("13e90672ae28e846323e1d0ea8c0918f8bd49b8adc9cc6eaeb1d56c4ef4e26ea", output.getString("hashArrayField"));
        Assert.assertNotNull(output.getDateTime("currentTimestampField"));
        Assert.assertEquals("stringValue 32 10", output.getString("concatField"));

    }

}
