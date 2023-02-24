package com.mercari.solution.util.domain.sequence;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;

public class First implements Sequencer {

    private static final String STATE_TIMESTAMP_SUFFIX = "_ts" + STATE_SUFFIX;

    private final String name;
    private final String field;

    private final String timestampStateName;
    private final String condition;

    private transient Filter.ConditionNode conditionNode;


    First(final JsonObject params) {
        if(params.has("name")) {
            this.name = params.get("name").getAsString();
            this.timestampStateName = name + STATE_TIMESTAMP_SUFFIX;
        } else {
            this.name = null;
            this.timestampStateName = null;
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
        final Schema.FieldType outputType;
        if(inputSchema.hasField(field)) {
            outputType = inputSchema.getField(field).getType();
        } else if(stateTypes.containsKey(field)) {
            outputType = stateTypes.get(field);
        } else {
            throw new IllegalArgumentException();
        }

        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, outputType.withNullable(true));
        return outputTypes;
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> outputTypes = outputTypes(inputSchema, stateTypes);
        outputTypes.put(timestampStateName, Schema.FieldType.INT64.withNullable(true));
        return outputTypes;
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("sequencer:First.name must not be null.");
        }
        if(this.field == null) {
            errorMessages.add("sequencer:First.field must not be null.");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(condition, JsonElement.class));
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

        final Object fieldValue = Sequencer.getValue(input, state, updateValues, valueGetter, field);
        final Long prevEpochMillis = (Long) Sequencer.getValue(input, state, updateValues, valueGetter, timestampStateName);

        if(prevEpochMillis == null || timestamp.getMillis() < prevEpochMillis) {
            updateValues.put(timestampStateName, timestamp.getMillis());
            updateValues.put(name, fieldValue);
        }
    }
}
