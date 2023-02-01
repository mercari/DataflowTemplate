package com.mercari.solution.util.domain.sequence;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;

public class Count implements Sequencer {

    private final String name;
    private final String field;
    private final String condition;

    private transient com.mercari.solution.util.Filter.ConditionNode conditionNode;

    Count(final JsonObject params) {
        if(params.has("name")) {
            this.name = params.get("name").getAsString();
        } else {
            this.name = null;
        }

        if(params.has("field")) {
            this.field = params.get("field").getAsString();
        } else {
            this.field = null;
        }

        if(params.has("condition")) {
            this.condition = params.get("condition").toString();
        } else {
            this.condition = null;
        }
    }

    @Override
    public Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        return stateTypes(inputSchema, stateTypes);
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, Schema.FieldType.INT64);
        return outputTypes;
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("sequencer:Count.name must not be null.");
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
        return false;
    }

    @Override
    public <T> void execute(
            final T input,
            final T state,
            final Map<String, Object> updateValues,
            final Instant timestamp,
            final SchemaUtil.ValueGetter<T> valueGetter,
            final SchemaUtil.TimestampConverter timestampConverter) {

        if(conditionNode != null) {
            if(!Filter.filter(input, valueGetter::getValue, this.conditionNode, updateValues)) {
                return;
            }
        }

        final int prevCount = (Integer)Optional.ofNullable(Sequencer.getValue(input, state, updateValues, valueGetter, name)).orElse(0);
        if(this.field == null) {
            updateValues.put(name, prevCount + 1);
        } else {
            final Object fieldValue = Sequencer.getValue(input, state, updateValues, valueGetter, name);
            if(fieldValue != null) {
                updateValues.put(name, prevCount + 1);
            }
        }
    }
}
