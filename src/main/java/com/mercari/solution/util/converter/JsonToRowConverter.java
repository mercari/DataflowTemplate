package com.mercari.solution.util.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

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
            case ROW:
                if(!jsonElement.isJsonObject()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is",
                            fieldType.getTypeName(), jsonElement.toString()));
                }
                return convert( fieldType.getRowSchema(), jsonElement.getAsJsonObject());
            case ITERABLE:
            case ARRAY:
                if(!jsonElement.isJsonArray()) {
                    throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is",
                            fieldType.getTypeName(), jsonElement.toString()));
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
            case BYTES:
            case MAP:
            default:
                return null;
        }
    }

}
