package com.mercari.solution.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.apache.beam.sdk.values.KV;

import java.util.*;

public class JsonUtil {

    private static final Configuration.Defaults CONF_DEFAULT = new Configuration.Defaults() {
        private final JsonProvider jsonProvider = new JacksonJsonProvider();
        private final MappingProvider mappingProvider = new JacksonMappingProvider();

        @Override
        public JsonProvider jsonProvider() {
            return jsonProvider;
        }

        @Override
        public MappingProvider mappingProvider() {
            return mappingProvider;
        }

        @Override
        public Set<Option> options() {
            return EnumSet.noneOf(Option.class);
        }
    };

    private static final Configuration CONF_PATH = Configuration.builder()
            .jsonProvider(CONF_DEFAULT.jsonProvider())
            .mappingProvider(CONF_DEFAULT.mappingProvider())
            .options(Option.AS_PATH_LIST)
            .build();

    public static JsonElement fromJson(final String json) {
        return new Gson().fromJson(json, JsonElement.class);
    }

    public static List<KV<String, Object>> read(JsonObject json, String jsonPath) {
        return read(json.toString(), jsonPath);
    }

    public static List<KV<String, Object>> read(String json, String jsonPath) {

        Configuration.setDefaults(CONF_DEFAULT);

        final DocumentContext dc = JsonPath
                .using(CONF_PATH)
                .parse(json);

        final List<String> paths = dc.read(jsonPath);
        var values = new ArrayList<KV<String, Object>>();
        for(var path : paths) {
            final Object value = JsonPath.read(json, path);
            values.add(KV.of(path, value));
        }
        return values;
    }

    public static JsonObject set(JsonObject json, List<KV<String, Object>> pathAndValues) {
        final String result = set(json.toString(), pathAndValues);
        return new Gson().fromJson(result, JsonObject.class);
    }

    public static String set(String json, List<KV<String, Object>> pathAndValues) {
        Configuration.setDefaults(CONF_DEFAULT);

        final DocumentContext dc = JsonPath.parse(json);

        for(var pathAndValue : pathAndValues) {
            dc.set(pathAndValue.getKey(), pathAndValue.getValue());
        }

        return dc.jsonString();
    }

}
