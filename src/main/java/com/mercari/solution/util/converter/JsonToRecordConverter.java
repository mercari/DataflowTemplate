package com.mercari.solution.util.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JsonToRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToRecordConverter.class);

    public static GenericRecord convert(final Schema schema, final String text) {
        if(text == null || text.trim().length() < 2) {
            return null;
        }
        final JsonObject jsonObject = new Gson().fromJson(text, JsonObject.class);
        return convert(schema, jsonObject);
    }

    public static GenericRecord convert(final Schema schema, final JsonObject jsonObject) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            final String fieldName = Optional.ofNullable(field.getProp(SourceConfig.OPTION_ORIGINAL_FIELD_NAME)).orElse(field.name());
            builder.set(field.name(), convertValue(field.schema(), jsonObject.get(fieldName)));
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
            if(!schema.getFields().stream().anyMatch(f -> f.name().equals(entry.getKey()))) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is not present in schema.");
                return false;
            }
            final Schema.Field field = schema.getField(entry.getKey());
            final JsonElement element = entry.getValue();
            if(!AvroSchemaUtil.isNullable(field.schema()) && element.isJsonNull()) {
                LOG.error("Validation error: json field: " + entry.getKey() + " is null but schema is not nullable");
                return false;
            }
            if(element.isJsonNull()) {
                continue;
            }
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            switch (fieldSchema.getType()) {
                case RECORD: {
                    if(!element.isJsonObject()) {
                        LOG.error("Validation error: json field: " + entry.getKey() + " is not JsonObject. element: " + element);
                        return false;
                    }
                    if(!validateSchema(fieldSchema, element.getAsJsonObject())) {
                        return false;
                    }
                    break;
                }
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

    private static Object convertValue(final Schema schema, final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            if(Schema.Type.ARRAY.equals(schema.getType())) {
                return new ArrayList<>();
            }
            return null;
        }
        try {
            switch (schema.getType()) {
                case ENUM:
                case STRING:
                    return jsonElement.isJsonPrimitive() ? jsonElement.getAsString() : jsonElement.toString();
                case FIXED:
                case BYTES:
                    if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                        final LogicalTypes.Decimal decimalType = AvroSchemaUtil.getLogicalTypeDecimal(schema);
                        if(jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if(jsonPrimitive.isString()) {
                                final BigDecimal decimal = new BigDecimal(jsonPrimitive.getAsString(), new MathContext(decimalType.getPrecision()));
                                return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
                            } else if(jsonPrimitive.isNumber()) {
                                final BigDecimal decimal = jsonPrimitive.getAsBigDecimal();
                                return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
                            } else {
                                throw new IllegalStateException();
                            }
                        }
                    }
                    return ByteBuffer.wrap(jsonElement.isJsonPrimitive() ? jsonElement.getAsString().getBytes() : jsonElement.toString().getBytes());
                case INT: {
                    if (LogicalTypes.date().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalDate localDate = DateTimeUtil.toLocalDate(jsonPrimitive.getAsString());
                                return localDate != null ? Long.valueOf(localDate.toEpochDay()).intValue() : null;
                            } else if (jsonPrimitive.isNumber()) {
                                return jsonPrimitive.getAsInt();
                            } else {
                                return null;
                            }
                        } else {
                            return null;
                        }
                    } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalTime localTime = DateTimeUtil.toLocalTime(jsonPrimitive.getAsString());
                                return DateTimeUtil.toMilliOfDay(localTime);
                            } else if (jsonPrimitive.isNumber()) {
                                return jsonPrimitive.getAsInt();
                            } else {
                                return null;
                            }
                        } else {
                            return null;
                        }
                    }
                    return jsonElement.isJsonPrimitive() ? Integer.valueOf(jsonElement.getAsString()) : null;
                }
                case LONG: {
                    if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                            || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final String pattern = schema.getProp("patternTimestamp");
                                final Instant instant;
                                if (pattern != null) {
                                    instant = Instant.parse(jsonPrimitive.getAsString(), DateTimeFormat.forPattern(pattern));
                                } else {
                                    instant = Instant.parse(jsonPrimitive.getAsString());
                                }
                                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                                    return instant.getMillis();
                                } else {
                                    return instant.getMillis() * 1000;
                                }
                            } else if (jsonPrimitive.isNumber()) {
                                return jsonPrimitive.getAsLong();
                            } else {
                                return null;
                            }
                        } else {
                            return null;
                        }
                    } else if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                        if (jsonElement.isJsonPrimitive()) {
                            final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                            if (jsonPrimitive.isString()) {
                                final LocalTime localTime = DateTimeUtil.toLocalTime(jsonPrimitive.getAsString());
                                return DateTimeUtil.toMicroOfDay(localTime);
                            } else if (jsonPrimitive.isNumber()) {
                                return jsonPrimitive.getAsLong();
                            } else {
                                return null;
                            }
                        } else {
                            return null;
                        }
                    }
                    return jsonElement.isJsonPrimitive() ? jsonElement.getAsLong() : null;
                }
                case FLOAT:
                    return jsonElement.isJsonPrimitive() ? jsonElement.getAsFloat() : null;
                case DOUBLE:
                    return jsonElement.isJsonPrimitive() ? jsonElement.getAsDouble() : null;
                case BOOLEAN:
                    return jsonElement.isJsonPrimitive() ? jsonElement.getAsBoolean() : null;
                case RECORD: {
                    if (!jsonElement.isJsonObject()) {
                        throw new IllegalStateException(String.format("FieldType: %s's type is record, but jsonElement is",
                                schema.getType(), jsonElement.toString()));
                    }
                    return convert(schema, jsonElement.getAsJsonObject());
                }
                case ARRAY: {
                    if (!jsonElement.isJsonArray()) {
                        throw new IllegalStateException(String.format("FieldType: %s's type is array, but jsonElement is",
                                schema.getType(), jsonElement.toString()));
                    }
                    final List<Object> childValues = new ArrayList<>();
                    for (final JsonElement childJsonElement : jsonElement.getAsJsonArray()) {
                        if (childJsonElement.isJsonArray()) {
                            throw new IllegalArgumentException("JsonElement is not JsonArray: " + childJsonElement.toString());
                        }
                        final Object arrayValue = convertValue(schema.getElementType(), childJsonElement);
                        if (arrayValue != null) {
                            childValues.add(arrayValue);
                        }
                    }
                    return childValues;
                }
                case UNION: {
                    final Schema childSchema = AvroSchemaUtil.unnestUnion(schema);
                    return convertValue(childSchema, jsonElement);
                }
                case MAP:
                case NULL:
                default:
                    return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

}
