package com.mercari.solution.util.domain.sequence;

import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

public class TimestampDiff implements Sequencer {

    private final String name;
    private final String leftField;
    private final String rightField;
    private final Part part;

    private enum Part implements Serializable {
        second,
        minute,
        hour,
        day,
        week
    }


    TimestampDiff(final JsonObject params) {
        if(params.has("name")) {
            this.name = params.get("name").getAsString();
        } else {
            this.name = null;
        }

        if(params.has("leftField")) {
            this.leftField = params.get("leftField").getAsString();
        } else {
            this.leftField = null;
        }
        if(params.has("rightField")) {
            this.rightField = params.get("rightField").getAsString();
        } else {
            this.rightField = null;
        }

        if(params.has("part")) {
            this.part = Part.valueOf(params.get("part").getAsString());
        } else {
            this.part = null;
        }

    }

    @Override
    public Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, Schema.FieldType.INT64.withNullable(true));
        return outputTypes;
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        return new HashMap<>();
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.part == null) {
            errorMessages.add(String.format("sequencer:TimestampDiff[%s].part must not be null.", this.name));
        }
        return errorMessages;
    }

    @Override
    public void setup() {

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

        final Instant leftInstant, rightInstant;
        if(this.leftField == null) {
            leftInstant = timestamp;
        } else {
            final Object leftTimestampValue = Sequencer.getValue(input, state, updateValues, valueGetter, leftField);
            leftInstant = timestampConverter.toInstant(leftTimestampValue);
        }
        if(this.rightField == null) {
            rightInstant = timestamp;
        } else {
            final Object rightTimestampValue = Sequencer.getValue(input, state, updateValues, valueGetter, rightField);
            rightInstant = timestampConverter.toInstant(rightTimestampValue);
        }
        if(leftInstant == null || rightInstant == null) {
            updateValues.put(this.name, null);
        } else {
            final Long durationMillis = leftInstant.getMillis() - rightInstant.getMillis();
            switch (this.part) {
                case second:
                    updateValues.put(this.name, durationMillis / 1000L);
                    break;
                case minute:
                    updateValues.put(this.name, durationMillis / (1000L * 60));
                    break;
                case hour:
                    updateValues.put(this.name, durationMillis / (1000L * 60 * 60));
                    break;
                case day:
                    updateValues.put(this.name, durationMillis / (1000L * 60 * 60 * 24));
                    break;
                case week:
                    updateValues.put(this.name, durationMillis / (1000L * 60 * 60 * 24 * 7));
                    break;
                default:
                    updateValues.put(this.name, null);
                    break;
            }
        }

    }

}
