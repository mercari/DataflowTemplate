package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Aggregators implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Aggregators.class);

    private String input;
    private List<Schema.Field> commonFields;

    private List<Aggregator> aggregators;

    private SchemaUtil.PrimitiveValueGetter valueGetter;

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    public void setAggregators(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public static Aggregators of(final String input,
                                 final List<Schema.Field> commonFields,
                                 final DataType sourceType,
                                 final DataType targetType,
                                 final Schema inputSchema,
                                 final JsonArray fields) {

        final Aggregators aggregators = new Aggregators();
        aggregators.input = input;
        aggregators.commonFields = commonFields;
        aggregators.aggregators = new ArrayList<>();
        for(final JsonElement element : fields) {
            final Aggregator aggregator = Aggregator.of(element, inputSchema, targetType);
            aggregators.aggregators.add(aggregator);
        }
        switch (sourceType) {
            case ROW:
                aggregators.valueGetter = RowSchemaUtil::getAsPrimitive;
                break;
            case AVRO:
                aggregators.valueGetter = AvroSchemaUtil::getAsPrimitive;
                break;
            case STRUCT:
                aggregators.valueGetter = StructSchemaUtil::getAsPrimitive;
                break;
            case DOCUMENT:
                aggregators.valueGetter = DocumentSchemaUtil::getAsPrimitive;
                break;
            case ENTITY:
                aggregators.valueGetter = EntitySchemaUtil::getAsPrimitive;
                break;
            default:
                throw new IllegalStateException("Not supported aggregators sourceType: " + sourceType);
        }
        return aggregators;
    }

    public List<String> validate(int index) {
        final List<String> errorMessages = new ArrayList<>();

        if(this.input == null) {
            errorMessages.add("aggregation[" + index + "].input must not be null");
        }
        if(this.aggregators == null) {
            errorMessages.add("aggregation[" + index + "].fields must not be null");
        } else {
            for(int fieldIndex=0; fieldIndex<this.aggregators.size(); fieldIndex++) {
                errorMessages.addAll(aggregators.get(fieldIndex).validate(index, fieldIndex));
            }
        }
        return errorMessages;
    }

    public Aggregators setup() {
        for(final Aggregator aggregator : this.aggregators) {
            aggregator.setup();
        }
        return this;
    }

    public Accumulator addInput(Accumulator accumulator, UnionValue input) {
        for(final Schema.Field commonField : commonFields) {
            final Object primitiveValue = valueGetter.getValue(input.getValue(), commonField.getType(), commonField.getName());
            accumulator.put(commonField.getType(), commonField.getName(), primitiveValue);
        }
        for(final Aggregator aggregator : this.aggregators) {
            if(aggregator.getIgnore()) {
                continue;
            }
            if(!aggregator.filter(input)) {
                continue;
            }
            accumulator = aggregator.addInput(accumulator, input, valueGetter);
            accumulator.empty = false;
        }
        return accumulator;
    }

    public Accumulator mergeAccumulators(Accumulator base, final Iterable<Accumulator> accums) {
        boolean done = false;
        for (final Accumulator accum : accums) {
            if(accum.empty) {
                continue;
            }
            if(!done) {
                for(final Schema.Field commonField : commonFields) {
                    final Object primitiveValue = accum.get(commonField.getType(), commonField.getName());
                    base.put(commonField.getType(), commonField.getName(), primitiveValue);
                }
                done = true;
            }
            for(final Aggregator aggregator : aggregators) {
                if(aggregator.getIgnore()) {
                    continue;
                }
                base = aggregator.mergeAccumulator(base, accum);
            }
            base.empty = base.empty && accum.empty;
        }

        return base;
    }

    public Map<String, Object> extractOutput(
            final Accumulator accumulator,
            Map<String, Object> values,
            final SchemaUtil.PrimitiveValueConverter valueConverter) {

        for(final Aggregator aggregator : this.aggregators) {
            values = aggregator.extractOutput(accumulator, values, valueConverter);
        }
        return values;
    }

}