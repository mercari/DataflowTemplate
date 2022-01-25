package com.mercari.solution.util.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class JsonToRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToRowConverter.class);

    public static Row convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonObject jsonObject = new Gson().fromJson(text, JsonObject.class);
        return convert(schema, jsonObject);
    }

    public static Row convert(final Schema schema, final JsonObject jsonObject) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            final String fieldName;
            if(field.getOptions().hasOption(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)) {
                fieldName = field.getOptions().getValue(SourceConfig.OPTION_ORIGINAL_FIELD_NAME);
            } else {
                fieldName = field.getName();
            }
            builder.withFieldValue(field.getName(), convertValue(field.getType(), jsonObject.get(fieldName)));
        }
        return builder.build();
    }

    public static boolean validateSchema(final Schema schema, final String json) {
        if(json == null || json.trim().length() < 2) {
            return false;
        }
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        return validateSchema(schema, jsonObject);
    }

    public static boolean validateSchema(final Schema schema, final JsonObject jsonObject) {
        for(final Map.Entry<String,JsonElement> entry : jsonObject.entrySet()) {
            if(!schema.hasField(entry.getKey())) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is not present in schema.");
                return false;
            }
            final Schema.Field field = schema.getField(entry.getKey());
            final JsonElement element = entry.getValue();
            if(field.getType().getNullable() != null && !field.getType().getNullable() && element.isJsonNull()) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is null but schema is not nullable");
                return false;
            }
            if(element.isJsonNull()) {
                continue;
            }
            switch (field.getType().getTypeName()) {
                case ROW: {
                    if(!element.isJsonObject()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonObject. element: " + element);
                        return false;
                    }
                    if(!validateSchema(field.getType().getRowSchema(), element.getAsJsonObject())) {
                        return false;
                    }
                    break;
                }
                case ITERABLE:
                case ARRAY: {
                    if(!element.isJsonArray()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonArray. element: " + element);
                        return false;
                    }
                    break;
                }
                default: {
                    if(!element.isJsonPrimitive()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonPrimitive. element: " + element);
                        return false;
                    }
                    break;
                }
            }
        }
        return true;
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
            case DECIMAL: {
                if(!jsonElement.isJsonPrimitive()) {
                    throw new IllegalStateException("Can not convert Decimal type from jsonElement: " + jsonElement.toString());
                }
                final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                if(jsonPrimitive.isString()) {
                    return new BigDecimal(jsonPrimitive.getAsString());
                } else if(jsonPrimitive.isNumber()) {
                    return new BigDecimal(jsonPrimitive.getAsString());
                } else {
                    throw new IllegalStateException("Can not convert Decimal type from jsonElement: " + jsonElement.toString());
                }
            }
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
                if(fieldType.getCollectionElementType().getTypeName().equals(Schema.TypeName.ROW)) {
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
            default:
                return null;
        }
    }

}
