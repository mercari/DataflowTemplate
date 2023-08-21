package com.mercari.solution.util.pipeline;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;

public class ParameterUtil {

    public static KV<List<String>, Boolean> getSingleMultiAttribute(
            final JsonObject params, final String singleName, final String multiName) {

        final List<String> list = new ArrayList<>();
        final Boolean isSingle;
        if(params.has(singleName) || params.has(multiName)) {
            if(params.has(singleName)) {
                if(!params.get(singleName).isJsonPrimitive()) {
                    throw new IllegalArgumentException();
                }
                list.add(params.get(singleName).getAsString());
                isSingle = true;
            } else {
                if(!params.get(multiName).isJsonArray()) {
                    throw new IllegalArgumentException();
                }
                for(final JsonElement field : params.get(multiName).getAsJsonArray()) {
                    if(!field.isJsonPrimitive()) {
                        throw new IllegalArgumentException();
                    }
                    list.add(field.getAsString());
                }
                isSingle = false;
            }
        } else {
            throw new IllegalArgumentException("Illegal parameter: " + params + " not contains both " + singleName + " and " + multiName);
        }
        return KV.of(list, isSingle);
    }
}
