package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class MutationToJsonConverter {

    public static JsonObject convert(final Mutation mutation) {
        if(mutation == null) {
            return null;
        }
        if(Mutation.Op.DELETE.equals(mutation.getOperation())) {
            final JsonArray keys = new JsonArray();
            for(final Key key : mutation.getKeySet().getKeys()) {
                keys.add(key.toString());
            }
            final JsonObject jsonObject = new JsonObject();
            jsonObject.add("keys", keys);
            return jsonObject;
        } else {
            return convertJsonObject(mutation.asMap());
        }
    }

    private static JsonObject convertJsonObject(final Map<String, Value> values) {
        final JsonObject jsonObject = new JsonObject();
        for(Map.Entry<String, Value> entry : values.entrySet()) {
            final Value value = entry.getValue();
            if(value.isNull()) {
                jsonObject.add(entry.getKey(), null);
                continue;
            }
            switch (entry.getValue().getType().getCode()) {
                case BOOL:
                    jsonObject.addProperty(entry.getKey(), value.getBool());
                    break;
                case STRING:
                    jsonObject.addProperty(entry.getKey(), value.getString());
                    break;
                case BYTES:
                    jsonObject.addProperty(entry.getKey(), value.getBytes().toBase64());
                    break;
                case JSON:
                    jsonObject.addProperty(entry.getKey(), value.getJson());
                    break;
                case INT64:
                    jsonObject.addProperty(entry.getKey(), value.getInt64());
                    break;
                case FLOAT64:
                    jsonObject.addProperty(entry.getKey(), value.getFloat64());
                    break;
                case NUMERIC:
                case PG_NUMERIC:
                    jsonObject.addProperty(entry.getKey(), value.getNumeric());
                    break;
                case PG_JSONB:
                    jsonObject.addProperty(entry.getKey(), value.getPgJsonb());
                    break;
                case DATE:
                    jsonObject.addProperty(entry.getKey(), value.getDate().toString());
                    break;
                case TIMESTAMP: {
                    if(value.isCommitTimestamp()) {
                        jsonObject.addProperty(entry.getKey(), "COMMIT_TIMESTAMP");
                    } else {
                        jsonObject.addProperty(entry.getKey(), value.getTimestamp().toString());
                    }
                    break;
                }
                case STRUCT: {
                    final Struct childStruct = value.getStruct();
                    final Map<String, Value> childValues = toMap(childStruct);
                    final JsonObject child = convertJsonObject(childValues);
                    jsonObject.add(entry.getKey(), child);
                    break;
                }
                case ARRAY: {
                    final JsonArray jsonArray = new JsonArray();
                    switch (value.getType().getArrayElementType().getCode()) {
                        case BOOL:
                            value.getBoolArray().forEach(jsonArray::add);
                            break;
                        case STRING:
                            value.getStringArray().forEach(jsonArray::add);
                            break;
                        case BYTES:
                            value.getBytesArray().stream().map(ByteArray::toBase64).forEach(jsonArray::add);
                            break;
                        case JSON:
                            value.getJsonArray().forEach(jsonArray::add);
                            break;
                        case INT64:
                            value.getInt64Array().forEach(jsonArray::add);
                            break;
                        case FLOAT64:
                            value.getFloat64Array().forEach(jsonArray::add);
                            break;
                        case NUMERIC:
                        case PG_NUMERIC:
                            value.getNumericArray().forEach(jsonArray::add);
                            break;
                        case PG_JSONB:
                            value.getPgJsonbArray().forEach(jsonArray::add);
                            break;
                        case DATE:
                            value.getDateArray().stream().map(Date::toString).forEach(jsonArray::add);
                            break;
                        case TIMESTAMP:
                            value.getTimestampArray().stream().map(Timestamp::toString).forEach(jsonArray::add);
                            break;
                        case STRUCT:
                            value.getStructArray().stream()
                                    .map(MutationToJsonConverter::toMap)
                                    .map(MutationToJsonConverter::convertJsonObject)
                                    .forEach(jsonArray::add);
                            break;
                        case ARRAY: {
                            throw new IllegalStateException();
                        }
                        case UNRECOGNIZED:
                        default:
                            throw new IllegalStateException();
                    }
                    jsonObject.add(entry.getKey(), jsonArray);
                    break;
                }
                case UNRECOGNIZED:
                    throw new IllegalStateException();
            }
        }
        return jsonObject;
    }

    private static Map<String, Value> toMap(final Struct struct) {
        final Map<String, Value> map = new HashMap<>();
        if(struct == null) {
            return map;
        }
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final Value value = struct.getValue(field.getName());
            map.put(field.getName(), value);
        }
        return map;
    }

}
