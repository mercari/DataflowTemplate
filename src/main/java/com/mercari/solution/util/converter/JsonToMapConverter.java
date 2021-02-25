package com.mercari.solution.util.converter;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonToMapConverter {

    public static Map<String, Object> convert(final JsonElement json) {
        final Map<String, Object> map = new HashMap<>();
        if(json == null) {
            return map;
        } else if(json.isJsonNull()) {
            return map;
        } else if(json.isJsonPrimitive()) {
            map.put("value", json.getAsJsonPrimitive());
            return map;
        } else if(json.isJsonObject()) {
            for(final Map.Entry<String, JsonElement> entry :json.getAsJsonObject().entrySet()) {
                map.put(entry.getKey(), getValue(entry.getValue()));
            }
        } else if(json.isJsonArray()) {
            final List<Object> values = new ArrayList<>();
            for(final JsonElement element : json.getAsJsonArray()) {
                values.add(getValue(element));
            }
            map.put("values", values);
        }

        return map;
    }

    private static Object getValue(JsonElement element) {
        if(element == null) {
            return null;
        } else if(element.isJsonNull()) {
            return null;
        } else if(element.isJsonPrimitive()) {
            final JsonPrimitive primitive = element.getAsJsonPrimitive();
            if(primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else if(primitive.isString()) {
                return primitive.getAsString();
            } else if(primitive.isNumber()) {
                if(NumberUtils.isDigits(primitive.getAsString())) {
                    return primitive.getAsLong();
                } else {
                    return primitive.getAsDouble();
                }
            } else {
                return primitive.getAsString();
            }
        } else if(element.isJsonObject()) {
            return convert(element.getAsJsonObject());
        } else if(element.isJsonArray()) {
            final List<Object> values = new ArrayList<>();
            for(final JsonElement arrayElement : element.getAsJsonArray()) {
                values.add(getValue(arrayElement));
            }
            return values;
        }
        return null;
    }

}
