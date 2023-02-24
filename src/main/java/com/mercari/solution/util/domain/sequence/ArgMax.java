package com.mercari.solution.util.domain.sequence;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;

public class ArgMax implements Sequencer {

    private static final String STATE_ARGMAX_SUFFIX = "_argmax" + STATE_SUFFIX;

    private final String name;
    private final String field;
    private final String comparingField;

    private final String comparingStateName;
    private final String condition;

    private transient com.mercari.solution.util.Filter.ConditionNode conditionNode;


    ArgMax(final JsonObject params) {
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

        if(params.has("comparingField")) {
            this.comparingField = params.get("comparingField").getAsString();
            this.comparingStateName = name + STATE_ARGMAX_SUFFIX;
        } else {
            this.comparingField = null;
            this.comparingStateName = null;
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
            throw new IllegalArgumentException("");
        }

        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, outputType.withNullable(true));
        return outputTypes;
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Schema.FieldType comparingFieldType;
        if(inputSchema.hasField(field)) {
            comparingFieldType = inputSchema.getField(field).getType();
        } else if(stateTypes.containsKey(field)) {
            comparingFieldType = stateTypes.get(field);
        } else {
            throw new IllegalArgumentException("");
        }

        final Map<String, Schema.FieldType> outputTypes = outputTypes(inputSchema, stateTypes);
        outputTypes.put(comparingStateName, comparingFieldType);
        return outputTypes;
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("sequencer:ArgMax.name must not be null.");
        }
        if(this.field == null) {
            errorMessages.add(String.format("sequencer:ArgMax[%s].field must not be null.", name));
        }
        if(this.comparingField == null) {
            errorMessages.add(String.format("sequencer:ArgMax[%s].comparingField must not be null.", name));
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

        final Comparable comparingFieldValue_ = (Comparable) Sequencer.getValue(input, state, updateValues, valueGetter, comparingField);
        final Comparable comparingStateValue_ = (Comparable) Sequencer.getValue(input, state, updateValues, valueGetter, comparingStateName);

        if((comparingFieldValue_ != null && comparingStateValue_ == null)
                || (comparingFieldValue_ != null && comparingFieldValue_.compareTo(comparingStateValue_) > 0)) {
            updateValues.put(comparingStateName, comparingFieldValue_);
            final Object fieldValue = Sequencer.getValue(input, state, updateValues, valueGetter, field);
            updateValues.put(name, fieldValue);
        }

    }
}
