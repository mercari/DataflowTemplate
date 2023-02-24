package com.mercari.solution.util.domain.sequence;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Filter implements Sequencer {

    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    private final String condition;

    private transient com.mercari.solution.util.Filter.ConditionNode conditionNode;


    Filter(final JsonObject params) {
        if(params.has("condition") && !params.get("condition").isJsonNull() && !params.get("condition").isJsonPrimitive()) {
            this.condition = params.get("condition").toString();
        } else {
            this.condition = null;
        }

    }

    @Override
    public Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        return new HashMap<>();
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        return new HashMap<>();
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.condition == null) {
            errorMessages.add("sequencer:Filter.condition must not be null.");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.condition != null) {
            this.conditionNode = com.mercari.solution.util.Filter.parse(new Gson().fromJson(condition, JsonElement.class));
        } else {
            this.conditionNode = null;
        }
    }

    @Override
    public <T> boolean suspend(final T input,
                               final Map<String, Object> updateValues,
                               final SchemaUtil.ValueGetter<T> valueGetter) {

        return !com.mercari.solution.util.Filter.filter(input, valueGetter::getValue, this.conditionNode, updateValues);
    }

    @Override
    public <T> void execute(
            final T input,
            final T state,
            final Map<String, Object> updateValues,
            final Instant timestamp,
            final SchemaUtil.ValueGetter<T> valueGetter,
            final SchemaUtil.TimestampConverter timestampConverter) {

    }

}
