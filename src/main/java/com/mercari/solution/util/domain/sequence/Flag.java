package com.mercari.solution.util.domain.sequence;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.*;

public class Flag implements Sequencer {

    private static final String STATE_PREV_FLAG_SUFFIX = "_flag" + STATE_SUFFIX;

    private final String name;
    private final String changeFlagField;
    private final String prevFlagStateName;
    private final String condition;

    private transient com.mercari.solution.util.Filter.ConditionNode conditionNode;

    Flag(final JsonObject params) {
        if(params.has("name")) {
            this.name = params.get("name").getAsString();
        } else {
            this.name = null;
        }

        if(params.has("changeFlagField")) {
            this.changeFlagField = params.get("changeFlagField").getAsString();
            this.prevFlagStateName = name + STATE_PREV_FLAG_SUFFIX;
        } else {
            this.changeFlagField = null;
            this.prevFlagStateName = null;
        }

        if(params.has("condition")) {
            this.condition = params.get("condition").toString();
        } else {
            this.condition = null;
        }
    }

    @Override
    public Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, Schema.FieldType.BOOLEAN.withNullable(true));
        if(this.changeFlagField != null) {
            outputTypes.put(changeFlagField, Schema.FieldType.BOOLEAN.withNullable(true));
        }
        return outputTypes;
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> types = new HashMap<>();
        if(this.changeFlagField != null) {
            types.put(this.prevFlagStateName, Schema.FieldType.BOOLEAN.withNullable(true));
        }
        return types;
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("sequencer:Flag.name must not be null.");
        }
        if(this.condition == null) {
            errorMessages.add("sequencer:Flag.condition must not be null.");
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

        final boolean flag = Filter.filter(input, valueGetter::getValue, this.conditionNode, updateValues);
        updateValues.put(name, flag);
        if(this.changeFlagField != null) {
            final boolean prevFlag = (Boolean) Optional.ofNullable(Sequencer.getValue(input, state, updateValues, valueGetter, prevFlagStateName)).orElse(false);
            final Boolean changeFlag = prevFlag && flag;
            updateValues.put(changeFlagField, changeFlag);
            updateValues.put(prevFlagStateName, flag);
        }

    }
}
