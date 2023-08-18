package com.mercari.solution.util.pipeline.processing.processor.window;

import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Instant;

import java.util.*;
import java.util.stream.Collectors;

public class Lag extends WindowProcessor {

    private final List<Integer> lags;

    private final Boolean isSingleLag;

    //
    private final Integer lagMax;
    private final Map<String, Integer> bufferSizes;


    public Lag(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        super(name, condition, ignore, params);

        // lag
        if((!params.has("lag") || params.get("lag").isJsonNull())
                && (!params.has("lags") || params.get("lags").isJsonNull())) {
            throw new IllegalArgumentException("Lag step: " + name + " requires lag or lags");
        }

        final KV<List<String>,Boolean> lagsAndIsSingle = Processor
                .getSingleMultiAttribute(params, "lag", "lags");
        this.lags = lagsAndIsSingle.getKey().stream().map(Integer::valueOf).collect(Collectors.toList());
        this.isSingleLag = lagsAndIsSingle.getValue();
        if(lags.size() == 0) {
            throw new IllegalArgumentException("Lag step: " + name + " requires lags over size zero");
        }

        this.lagMax = lags.stream().filter(Objects::nonNull).max(Integer::compareTo).orElse(0);
        this.bufferSizes = super.getBufferSizes();
    }

    public static Lag of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Lag(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.lag;
    }

    @Override
    public Integer getOffsetMax() {
        return lagMax;
    }

    @Override
    public SizeUnit getSizeUnit() {
        return SizeUnit.count;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final List<Schema.Field> outputFields = new ArrayList<>();
        if(fields.size() > 0) {
            for(final String field : fields) {
                for(final Integer lag : lags) {
                    final String outputFieldName = createOutputName(field, lag);
                    final Schema.FieldType fieldType = inputTypes.get(field);
                    if(fieldType == null) {
                        throw new IllegalArgumentException("Lag step: " + name + " input field: " + field + " not found");
                    }
                    outputFields.add(Schema.Field.of(outputFieldName, fieldType).withNullable(true));
                }
            }
        } else if(expressions.size() > 0) {
            for(int no=0; no<expressions.size(); no++) {
                for(final Integer lag : lags) {
                    final String outputFieldName = createOutputName(no, lag);
                    outputFields.add(Schema.Field.of(outputFieldName, Schema.FieldType.DOUBLE).withNullable(true));
                }
            }
        } else {
            throw new IllegalArgumentException();
        }
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        if(this.fields.size() > 0) {
            for(final String field : fields) {
                final Schema.FieldType fieldType = inputTypes.get(field);
                if(fieldType == null) {
                    throw new IllegalArgumentException("Lag step: " + name + " input field: " + field + " not found");
                }
                bufferTypes.put(field, fieldType);
            }
        } else {
            for(final String variable : bufferSizes.keySet()) {
                final Schema.FieldType fieldType = inputTypes.get(variable);
                if(fieldType == null) {
                    throw new IllegalArgumentException("Lag step: " + name + " input variable: " + variable + " not found");
                }
                bufferTypes.put(variable, fieldType);
            }
        }
        return bufferTypes;
    }

    @Override
    public void setup() {
        super.setup();
    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {

        final Map<String, Object> outputs = new HashMap<>();

        for(int lag : lags) {
            if(this.fields.size() > 0) {
                for(final String field : fields) {
                    final Object value = buffer.get(field, lag);
                    final String outputName = createOutputName(field, lag);
                    outputs.put(outputName, value);
                }
            } else {
                for(int e=0; e<expressions.size(); e++) {
                    final Set<String> variables = variablesList.get(e);
                    final Map<String, Double> values = new HashMap<>();
                    for(final String input : this.bufferSizes.keySet()) {
                        if(variables.contains(input)) {
                            final Double firstLagValue = buffer.getAsDouble(input, 0);
                            values.put(input, firstLagValue);
                        }
                        final int size = this.bufferSizes.get(input) - lagMax + 1;
                        for(int variableIndex=0; variableIndex<size; variableIndex++) {
                            final int index = lag + variableIndex;
                            final String variableName = Processor.createVariableName(input, DEFAULT_VARIABLE_NAME_SEPARATOR, variableIndex);
                            final Double variableValue = buffer.getAsDouble(input, index);
                            values.put(variableName, variableValue);
                        }
                    }

                    final String outputName = createOutputName(e, lag);
                    try {
                        final double output = expressionList.get(e).setVariables(values).evaluate();
                        if(Double.isNaN(output) || Double.isInfinite(output)) {
                            outputs.put(outputName, null);
                        } else {
                            outputs.put(outputName, output);
                        }
                    } catch (final NullPointerException | ArithmeticException ee) {
                        LOG.debug("Lag step: " + outputName + " failed evaluate expression: " + expressions.get(e) + " for variables: " + values + " cause: " + ee);
                        outputs.put(outputName, null);
                    }
                }

            }
        }

        return outputs;
    }

    private String createOutputName(final String field, final Integer lag) {
        if(isSingleField && isSingleLag) {
            return name;
        } else if(isSingleField) {
            return String.format("%s_lag%d", name, lag);
        } else if(isSingleLag) {
            return String.format("%s_%s", name, field);
        } else {
            return String.format("%s_%s_lag%d", name, field, lag);
        }
    }

    private String createOutputName(final Integer no, final Integer lag) {
        if(isSingleField && isSingleLag) {
            return name;
        } else if(isSingleField) {
            return String.format("%s_lag%d", name, lag);
        } else {
            final String expressionName;
            if(no < expressionNames.size()) {
                expressionName = expressionNames.get(no);
            } else {
                expressionName = "exp" + no;
            }
            if(isSingleLag) {
                return String.format("%s_%s", name, expressionName);
            } else {
                return String.format("%s_%s_lag%d", name, expressionName, lag);
            }
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        throw new NotImplementedException("");
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        throw new NotImplementedException("");
    }

}
