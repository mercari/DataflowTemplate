package com.mercari.solution.util.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class JsonToRowConverter {
    
    public static Row convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonObject jsonObject = new Gson().fromJson(text, JsonObject.class);
        return convert(schema, jsonObject);
    }

    public static Row convert( final Schema schema, final JsonObject jsonObject) {
        final Row.Builder builder = Row.withSchema(schema);
        for(final Schema.Field field : schema.getFields()) {
            if(field.getType().getTypeName().equals(Schema.TypeName.ARRAY)) {
                builder.addArray(convertValue(field.getType(), jsonObject.get(field.getName())));
            } else {
                builder.addValue(convertValue(field.getType(), jsonObject.get(field.getName())));
            }
        }
        return builder.build();
    }

    private static Object convertValue(final Schema.FieldType fieldType, final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            if(org.apache.avro.Schema.Type.ARRAY.equals(fieldType)) {
                return new ArrayList<>();
            }
            return null;
        }
        switch (fieldType.getTypeName()) {
            case STRING:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
            case BYTES:
                return jsonElement.isJsonPrimitive() ? Base64.getUrlDecoder().decode(jsonElement.getAsString()) : null;
            case INT16:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsShort() : null;
            case INT32:
                return jsonElement.isJsonPrimitive() ? Integer.valueOf(jsonElement.getAsString()) : null;
            case INT64:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsLong() : null;
            case FLOAT:
                return jsonElement.isJsonPrimitive() ? Float.valueOf(jsonElement.getAsString()) : null;
            case DOUBLE:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsDouble() : null;
            case BOOLEAN:
                return jsonElement.isJsonPrimitive() ? jsonElement.getAsBoolean() : null;
            case DATETIME:
                return jsonElement.isJsonPrimitive() ? Instant.parse(jsonElement.getAsString()) : null;
            case LOGICAL_TYPE: {
                if(RowSchemaUtil.isLogicalTypeDate(fieldType)) {
                    return LocalDate.parse(jsonElement.getAsString());
                } else if(RowSchemaUtil.isLogicalTypeTime(fieldType)) {
                    return LocalTime.parse(jsonElement.getAsString());
                } else if(RowSchemaUtil.isLogicalTypeTimestamp(fieldType)) {
                    return Instant.parse(jsonElement.getAsString());
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported Beam logical type: " + fieldType.getLogicalType().getIdentifier());
                }
            }
            case ROW:
                if(!jsonElement.isJsonObject()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                return convert(fieldType.getRowSchema(), jsonElement.getAsJsonObject());
            case MAP: {
                if(!jsonElement.isJsonObject()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is map, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                return jsonElement.getAsJsonObject().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> convertValue(fieldType.getMapKeyType(), new JsonPrimitive(e.getKey())),
                                e -> convertValue(fieldType.getMapValueType(), e.getValue())));
            }
            case ITERABLE:
            case ARRAY:
                if(!jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is %s",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                // for maprecord
                {
                    final Schema.FieldType elementFieldType = fieldType.getCollectionElementType();
                    final Schema.Options options = elementFieldType.getRowSchema().getOptions();
                    if(options.hasOption("extension") && options.getValue("extension").equals("maprecord")) {
                        final Schema schema = Schema.builder()
                                .addField("key", elementFieldType.getMapKeyType())
                                .addField("value", elementFieldType.getMapValueType())
                                .build();
                        return jsonElement.getAsJsonObject().entrySet().stream()
                                .map(e -> Row.withSchema(schema)
                                            .withFieldValue("key", convertValue(elementFieldType.getMapKeyType(), new JsonPrimitive(e.getKey())))
                                            .withFieldValue("value", convertValue(elementFieldType.getMapValueType(), e.getValue()))
                                            .build())
                                .collect(Collectors.toList());
                    }
                }

                final List<Object> childValues = new ArrayList<>();
                for(final JsonElement childJsonElement : jsonElement.getAsJsonArray()) {
                    if(childJsonElement.isJsonArray()) {
                        throw new IllegalArgumentException("");
                    }
                    final Object arrayValue = convertValue(fieldType.getCollectionElementType(), childJsonElement);
                    if(arrayValue != null) {
                        childValues.add(arrayValue);
                    }
                }
                return childValues;
            case BYTE:
            case DECIMAL:
            default:
                return null;
        }
    }

}
