package com.mercari.solution.util.domain.sequence;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Sequencer extends Serializable {

    enum Op implements Serializable {
        min,
        max,
        first,
        last,
        first_timestamp,
        last_timestamp,
        count,
        sum,
        avg,
        argmax,
        argmin,
        flag,
        expression,
        timestamp_diff,

        filter,
        clear
    }

    String STATE_SUFFIX = "_state_";


    List<String> validate();

    void setup();

    <T> boolean suspend(final T input, final Map<String, Object> updateValues, final SchemaUtil.ValueGetter<T> valueGetter);

    Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes);

    Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes);

    <T> void execute(
            final T input,
            final T state,
            final Map<String, Object> updateValues,
            final Instant timestamp,
            final SchemaUtil.ValueGetter<T> valueGetter,
            final SchemaUtil.TimestampConverter timestampConverter);


    static Sequencer convert(final JsonElement element) {
        if(element == null || element.isJsonNull() || !element.isJsonObject()) {
            return null;
        }

        final JsonObject params = element.getAsJsonObject();
        if(!params.has("op")) {
            throw new IllegalArgumentException();
        }
        final Op op = Op.valueOf(params.get("op").getAsString());
        switch (op) {
            case min:
                return new Min(params);
            case max:
                return new Max(params);
            case first:
                return new First(params);
            case last:
                return new Last(params);
            case count:
                return new Count(params);
            case sum:
                return new Sum(params);
            case argmax:
                return new ArgMax(params);
            case flag:
                return new Flag(params);
            case expression:
                return new Expression(params);
            case timestamp_diff:
                return new TimestampDiff(params);
            case filter:
                return new Filter(params);
            default:
                throw new IllegalArgumentException("Not supported op: " + op);
        }

    }

    static <T> Object getValue(final T input, final T state, final Map<String, Object> updatingState,
                               final SchemaUtil.ValueGetter<T> valueGetter, final String field) {

        if(updatingState.containsKey(field)) {
            return updatingState.get(field);
        }
        return Optional
                .ofNullable(valueGetter.getValue(state, field))
                .orElseGet(() -> valueGetter.getValue(input, field));
    }
}
