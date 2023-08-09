package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;

import java.util.Map;

public class Constant implements SelectFunction {

    private final String name;
    private final String type;
    private final String valueString;

    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private transient JsonElement value;

    Constant(String name, String type, String valueString, boolean ignore) {
        this.name = name;
        this.type = type;
        this.valueString = valueString;
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
        switch (type) {
            case "int":
            case "int32":
            case "integer":
                return value.getAsInt();
            case "long":
            case "int64":
                return value.getAsLong();
            case "string":
                return value.getAsString();
            case "date":
                return Long.valueOf(DateTimeUtil.toLocalDate(value.getAsString()).toEpochDay()).intValue();
            case "time":
                return DateTimeUtil.toLocalTime(value.getAsString()).toNanoOfDay() / 1000L;
            case "timestamp":
                return DateTimeUtil.toJodaInstant(value.getAsString()).getMillis() * 1000L;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Schema.FieldType createOutputFieldType(final String name, final String type) {
        final Schema.FieldType fieldType;
        switch (type) {
            case "int":
            case "int32":
            case "integer":
                fieldType = Schema.FieldType.INT32;
                break;
            case "long":
            case "int64":
                fieldType = Schema.FieldType.INT64;
                break;
            case "string":
                fieldType = Schema.FieldType.STRING;
                break;
            case "date":
                fieldType = CalciteUtils.DATE;
                break;
            case "time":
                fieldType = CalciteUtils.TIME;
                break;
            case "timestamp":
                fieldType = Schema.FieldType.DATETIME;
                break;
            default:
                throw new IllegalArgumentException("SelectField constant: " + name + ".type[" + type + "] is not supported");
        }

        return fieldType.withNullable(true);
    }

}
