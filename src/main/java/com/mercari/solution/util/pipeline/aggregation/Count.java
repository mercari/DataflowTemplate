package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;

import java.util.*;

public class Count implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private String condition;

    private Boolean ignore;

    private transient Filter.ConditionNode conditionNode;

    public Count() {

    }

    public Aggregator.Op getOp() {
        return Aggregator.Op.count;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Boolean getIgnore() {
        return this.ignore;
    }

    @Override
    public Boolean filter(final UnionValue unionValue) {
        return Aggregator.filter(conditionNode, unionValue);
    }


    public static Count of(final String name, final String condition, final Boolean ignore) {

        final Count count = new Count();
        count.name = name;
        count.condition = condition;
        count.ignore = ignore;

        count.outputFields = new ArrayList<>();
        count.outputFields.add(Schema.Field.of(name, Schema.FieldType.INT64.withNullable(true)));

        return count;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if (name == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].name must not be null");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if (this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public List<Schema.Field> getOutputFields() {
        return this.outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accum, final UnionValue input, final SchemaUtil.PrimitiveValueGetter valueGetter) {
        final Long countPrev = accum.getLong(name);
        accum.putLong(name, Optional.ofNullable(countPrev).orElse(0L) + 1L);
        return accum;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator accum) {

        final Long stateValue = base.getLong(name);
        final Long accumValue = accum.getLong(name);

        final Long count = Optional.ofNullable(stateValue).orElse(0L) + Optional.ofNullable(accumValue).orElse(0L);
        base.putLong(name, count);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator, final Map<String, Object> values, final SchemaUtil.PrimitiveValueConverter converter) {
        final Long count = Optional
                .ofNullable(accumulator.getLong(name))
                .orElse(0L);
        values.put(name, count);
        return values;
    }

}