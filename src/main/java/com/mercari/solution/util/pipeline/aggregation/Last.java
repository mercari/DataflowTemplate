package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;


import java.util.*;

public class Last implements Aggregator {

    private List<Schema.Field> outputFields;

    private String name;
    private List<String> fields;
    private String condition;

    private Boolean ignore;

    private String separator;

    private Boolean expandOutputName;
    private Boolean opposite;


    private String timestampKeyName;


    private transient Filter.ConditionNode conditionNode;

    public Last() {

    }

    public Op getOp() {
        return this.opposite ? Op.argmin : Op.argmax;
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

    public static Last of(final String name,
                          final Schema inputSchema,
                          final String condition,
                          final Boolean ignore,
                          final String separator,
                          final JsonObject params) {

        return of(name, inputSchema, condition, ignore, separator, params, false);
    }

    public static Last of(final String name,
                          final Schema inputSchema,
                          final String condition,
                          final Boolean ignore,
                          final String separator,
                          final JsonObject params,
                          final boolean opposite) {

        final Last last = new Last();
        last.name = name;
        last.condition = condition;
        last.ignore = ignore;
        last.separator = separator;

        last.fields = new ArrayList<>();
        if(params.has("fields") && params.get("fields").isJsonArray()) {
            for(JsonElement element : params.get("fields").getAsJsonArray()) {
                last.fields.add(element.getAsString());
            }
            last.expandOutputName = true;
        } else if(params.has("field")) {
            last.fields.add(params.get("field").getAsString());
            last.expandOutputName = false;
        }

        last.opposite = opposite;

        last.outputFields = last.createOutputFields(inputSchema);
        last.timestampKeyName = name + ".timestamp";

        return last;
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].name must not be null");
        }
        if(this.fields == null && this.fields.size() == 0) {
            errorMessages.add("aggregations[" + parent + "].fields[" + index + "].fields size must not be zero");
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public List<Schema.Field> getOutputFields() {
        return outputFields;
    }

    @Override
    public Accumulator addInput(Accumulator accumulator, UnionValue unionValue, SchemaUtil.PrimitiveValueGetter valueGetter) {
        final Long currentMicros = unionValue.getEpochMillis() * 1000L;
        final Long prevMicros = accumulator.getLong(timestampKeyName);

        if(Aggregator.compare(currentMicros, prevMicros, opposite)) {
            for(final Schema.Field field : this.outputFields) {
                final String originalFieldName = Aggregator.getFieldOptionOriginalFieldKey(field);
                final Object fieldValue = valueGetter.getValue(unionValue.getValue(), field.getType(), originalFieldName);
                final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                accumulator.put(field.getType(), accumulatorKeyName, fieldValue);
            }
            accumulator.putLong(timestampKeyName, currentMicros);
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(Accumulator baseAccum, Accumulator inputAccum) {
        final Long timestamp = baseAccum.getLong(timestampKeyName);
        final Long timestampAccum = inputAccum.getLong(timestampKeyName);
        if(Aggregator.compare(timestampAccum, timestamp, opposite)) {
            for(final Schema.Field field : outputFields) {
                final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
                final Object fieldValue = inputAccum.get(field.getType(), accumulatorKeyName);
                baseAccum.put(field.getType(), accumulatorKeyName, fieldValue);
            }
            baseAccum.putLong(timestampKeyName, timestampAccum);
        }
        return baseAccum;
    }

    @Override
    public Map<String, Object> extractOutput(Accumulator accumulator, Map<String, Object> values, SchemaUtil.PrimitiveValueConverter converter) {
        for(final Schema.Field field : outputFields) {
            final String accumulatorKeyName = Aggregator.getFieldOptionAccumulatorKey(field);
            final Object fieldPrimitiveValue = accumulator.get(field.getType(), accumulatorKeyName);
            final Object fieldValue = converter.convertPrimitive(field.getType(), fieldPrimitiveValue);
            values.put(field.getName(), fieldValue);
        }
        return values;
    }

    private List<Schema.Field> createOutputFields(final Schema inputSchema) {
        final List<Schema.Field> outputFields = new ArrayList<>();
        for(final String field : fields) {
            if(!inputSchema.hasField(field)) {
                throw new IllegalArgumentException("field: " + field + " is not found in source schema: " + inputSchema);
            }
            final String fieldName = expandOutputName ? outputFieldName(field) : name;
            final String keyName = expandOutputName ? outputKeyName(field) : name;
            final Schema.Field schemaField = inputSchema.getField(field);
            outputFields.add(Schema.Field
                    .of(fieldName, schemaField.getType().withNullable(true))
                    .withOptions(Schema.Options.builder()
                            .setOption(FIELD_OPTION_ORIGINAL_FIELD, Schema.FieldType.STRING, field)
                            .build())
                    .withOptions(Schema.Options.builder()
                            .setOption(FIELD_OPTION_ACCUMULATOR_KEY, Schema.FieldType.STRING, keyName)
                            .build()));
        }
        return outputFields;
    }

    private String outputFieldName(String field) {
        return String.format("%s%s%s", name, separator, field);
    }

    private String outputKeyName(String field) {
        return String.format("%s.%s", name, field);
    }

}
