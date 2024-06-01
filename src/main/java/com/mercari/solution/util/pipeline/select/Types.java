package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;

public class Types {

    public static Schema.FieldType createOutputFieldType(final String type) {
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
            default -> throw new IllegalArgumentException("SelectField type[" + type + "] is not supported");
        };
        return fieldType.withNullable(true);
    }

    public static Object getValue(String type, JsonElement value) {
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
            default -> throw new IllegalArgumentException("SelectField type[" + type + "] is not supported");
        };
    }

}
