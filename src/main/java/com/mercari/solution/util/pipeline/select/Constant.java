package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Constant implements SelectFunction {

    private final String name;
    private final String type;
    private final String valueString;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient JsonElement value;

    Constant(String name, String type, String valueString, boolean ignore) {
        this.name = name;
        this.type = type;
        this.valueString = valueString;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = createOutputFieldType(name, type);
        this.ignore = ignore;
    }

    public static Constant of(String name, JsonObject jsonObject, boolean ignore) {
        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField constant: " + name + " requires field parameter");
        }
        final String type = jsonObject.get("type").getAsString();

        if(!jsonObject.has("value")) {
            throw new IllegalArgumentException("SelectField constant: " + name + " requires field parameter");
        }
        final String value = jsonObject.get("value").toString();

        return new Constant(name, type, value, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {
        this.value = new Gson().fromJson(valueString, JsonElement.class);
    }

    @Override
    public Object apply(Map<String, Object> input) {
        if(value == null || value.isJsonNull()) {
            return null;
        }
        return switch (type) {
            case "bool", "boolean" -> value.getAsBoolean();
            case "int", "int32", "integer" -> value.getAsInt();
            case "long", "int64" -> value.getAsLong();
            case "float", "float32" -> value.getAsFloat();
            case "double", "float64" -> value.getAsDouble();
            case "string" -> value.getAsString();
            case "date" -> Long.valueOf(DateTimeUtil.toLocalDate(value.getAsString()).toEpochDay()).intValue();
            case "time" -> DateTimeUtil.toLocalTime(value.getAsString()).toNanoOfDay() / 1000L;
            case "timestamp" -> DateTimeUtil.toJodaInstant(value.getAsString()).getMillis() * 1000L;
            default ->
                    throw new IllegalArgumentException("SelectField constant: " + name + ".type[" + type + "] is not supported");
        };
    }

    private static Schema.FieldType createOutputFieldType(final String name, final String type) {
        final Schema.FieldType fieldType = switch (type) {
            case "bool", "boolean" -> Schema.FieldType.BOOLEAN;
            case "string" -> Schema.FieldType.STRING;
            case "int", "int32", "integer" -> Schema.FieldType.INT32;
            case "long", "int64" -> Schema.FieldType.INT64;
            case "float", "float32" -> Schema.FieldType.FLOAT;
            case "double", "float64" -> Schema.FieldType.DOUBLE;
            case "date" -> CalciteUtils.DATE;
            case "time" -> CalciteUtils.TIME;
            case "timestamp" -> Schema.FieldType.DATETIME;
            default ->
                    throw new IllegalArgumentException("SelectField constant: " + name + ".type[" + type + "] is not supported");
        };

        return fieldType.withNullable(true);
    }

}
