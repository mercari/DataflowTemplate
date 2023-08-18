package com.mercari.solution.util.pipeline.processing.processor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.feature.Binning;
import com.mercari.solution.util.pipeline.processing.processor.utility.Constant;
import com.mercari.solution.util.pipeline.processing.processor.utility.CurrentTimestamp;
import com.mercari.solution.util.pipeline.processing.processor.utility.Expression;
import com.mercari.solution.util.pipeline.processing.processor.learner.LinearRegression;
import com.mercari.solution.util.pipeline.processing.processor.utility.Hash;
import com.mercari.solution.util.pipeline.processing.processor.window.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public interface Processor extends Serializable {

    String DEFAULT_VARIABLE_NAME_SEPARATOR = "_";

    Logger LOG = LoggerFactory.getLogger(Processor.class);

    String getName();
    Op getOp();
    SizeUnit getSizeUnit();

    List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes);
    Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes);
    Map<String, Integer> getBufferSizes();
    void setup();
    Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp);
    Map<String, Object> process(Map<String, Object> input, Instant timestamp);
    List<Map<String, Object>> process(List<Map<String,Object>> inputs, Instant timestamp);
    boolean ignore();
    boolean filter(Map<String, Object> input);


    enum Op {
        // utility
        constant,
        expression,
        current_timestamp,
        hash,
        // feature
        binning,
        // window
        count,
        max,
        min,
        argmax,
        argmin,
        sum,
        avg,
        std,
        lag,
        // learner
        linear_regression,
        // signal
        channel_line
    }

    enum SizeUnit {
        count(0),
        second(1),
        minute(2),
        hour(3),
        day(4),
        none(99);

        public final int id;

        SizeUnit(int id) {
            this.id = id;
        }

        public static SizeUnit of(int id) {
            for(SizeUnit unit : values()) {
                if(unit.id == id) {
                    return unit;
                }
            }
            return none;
        }

        public static long getMicros(Integer unit, Integer size) {
            switch (SizeUnit.of(unit)) {
                case second:
                    return size * 1000_000L;
                case minute:
                    return size * 1000_000L * 60L;
                case hour:
                    return size * 1000_000L * 60L * 60L;
                case day:
                    return size * 1000_000L * 60L * 60L * 24L;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    static Processor of(final JsonElement element) {
        if(!element.isJsonObject()) {
            throw new IllegalArgumentException("Invalid processor config: " + element);
        }

        final JsonObject params = element.getAsJsonObject();
        if (!params.has("op")) {
            throw new IllegalArgumentException("Processor requires op parameter");
        }

        final String name;
        if(params.has("name")) {
            name = params.get("name").getAsString();
        } else {
            name = null;
        }

        final String condition;
        if(params.has("condition")) {
            condition = params.get("condition").toString();
        } else {
            condition = null;
        }

        final Boolean ignore;
        if(params.has("ignore")) {
            ignore = params.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        final Op op = Op.valueOf(params.get("op").getAsString());
        switch (op) {
            case constant:
                return Constant.of(name, condition, ignore, params);
            case expression:
                return Expression.of(name, condition, ignore, params);
            case current_timestamp:
                return CurrentTimestamp.of(name, condition, ignore, params);
            case hash:
                return Hash.of(name, condition, ignore, params);
            case binning:
                return Binning.of(name, condition, ignore, params);
            case count:
                return Count.of(name, condition, ignore, params);
            case max:
                return Max.of(name, condition, ignore, params);
            case min:
                return Max.of(name, condition, ignore, params, true);
            case argmax:
                return ArgMax.of(name, condition, ignore, params);
            case argmin:
                return ArgMax.of(name, condition, ignore, params, true);
            case sum:
                return Sum.of(name, condition, ignore, params);
            case avg:
                return Avg.of(name, condition, ignore, params);
            case std:
                return Std.of(name, condition, ignore, params);
            case lag:
                return Lag.of(name, condition, ignore, params);
            case linear_regression:
                return LinearRegression.of(name, condition, ignore, params);
            default:
                throw new IllegalArgumentException("Not supported processor: " + op);
        }

    }

    static Map<String, Integer> getBufferSizes(final List<Processor> processors) {
        final Map<String, Optional<Map.Entry<String,Integer>>> m = processors
                .stream()
                .filter(i -> !i.ignore())
                .flatMap(f -> f.getBufferSizes().entrySet().stream())
                .filter(e -> e.getValue() != null)
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.maxBy(Map.Entry.comparingByValue())));
        return m.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get().getValue()));
    }

    static Map<String, Schema.FieldType> getBufferTypes(final List<Processor> processors, final Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>(inputTypes);
        for(final Processor processor : processors) {
            if(processor.ignore()) {
                continue;
            }
            final Map<String, Schema.FieldType> processorBufferTypes = processor.getBufferTypes(bufferTypes);
            for(Map.Entry<String, Schema.FieldType> entry : processorBufferTypes.entrySet()) {
                // Validation
                if(bufferTypes.containsKey(entry.getKey())) {
                    final Schema.FieldType fieldType = bufferTypes.get(entry.getKey());
                    if(fieldType == null) {
                        throw new IllegalArgumentException("processor " + processor.getName() + " bufferTypes input[" + entry.getKey() + "] is null with inputTypes: " + bufferTypes);
                    } else {
                        if(entry.getValue() == null) {
                            throw new IllegalArgumentException("processor " + processor.getName() + " processors bufferType value input[" + entry.getKey() + "] is null");
                        }
                        if(!fieldType.getTypeName().equals(entry.getValue().getTypeName())) {
                            throw new IllegalArgumentException("processor " + processor.getName() + " not matched buffer input[" + entry.getKey() + "] type: " + fieldType.getTypeName() + " and " + entry.getValue().getTypeName());
                        }
                    }
                }
                bufferTypes.put(entry.getKey(), entry.getValue());
            }
            final List<Schema.Field> outputFields = processor.getOutputFields(bufferTypes);
            for(final Schema.Field outputField : outputFields) {
                bufferTypes.put(outputField.getName(), outputField.getType());
            }
        }
        return bufferTypes;
    }

    static Map<String, SizeUnit> getBufferUnits(final List<Processor> processors) {
        final Map<String, SizeUnit> units = new HashMap<>();
        for(final Processor processor : processors) {
            if(processor.ignore()) {
                continue;
            }
            for(final Map.Entry<String, Integer> size : processor.getBufferSizes().entrySet()) {
                units.put(size.getKey(), processor.getSizeUnit());
            }
        }
        return units;
    }

    static Map<String, Object> process(
            final List<Processor> processors,
            final ProcessingBuffer buffer,
            final ProcessingState state,
            final Instant timestamp) {

        final Map<String, Object> outputs = new HashMap<>();
        for(final Processor processor : processors) {
            if(processor.ignore()) {
                continue;
            }
            if(!processor.filter(buffer.getLatestValues())) {
                continue;
            }

            // put process results to outputs
            final Map<String, Object> output;
            try {
                output = processor.process(buffer, state, timestamp);
            } catch (final ProcessingInvalidException e) {
                LOG.warn(String.format("Failed to calculate processor: op=%s, name=%s, cause: %s",
                        processor.getOp(),
                        processor.getName(),
                        e.getMessage()));
                continue;
            }
            outputs.putAll(output);

            // put outputs to buffer
            for(final Map.Entry<String, Object> entry : output.entrySet()) {
                buffer.add(entry.getKey(), entry.getValue(), timestamp);
            }
        }

        return outputs;
    }

    static Map<String, Object> process(
            final List<Processor> processors,
            final Map<String, Object> values,
            final Instant timestamp) {

        final Map<String, Object> outputs = new HashMap<>(values);
        for(final Processor processor : processors) {
            if(processor.ignore()) {
                continue;
            }

            // put process results to outputs
            final Map<String, Object> output;
            try {
                output = processor.process(outputs, timestamp);
            } catch (final ProcessingInvalidException e) {
                LOG.warn(String.format("Failed to calculate processor: op=%s, name=%s, cause: %s",
                        processor.getOp(),
                        processor.getName(),
                        e.getMessage()));
                continue;
            }
            outputs.putAll(output);
        }

        return outputs;
    }

    static List<Map<String, Object>> process(
            final List<Processor> processors,
            final List<Map<String, Object>> valuesList,
            final Instant timestamp) {

        final List<Map<String, Object>> outputs = new ArrayList<>(valuesList);
        for(final Processor processor : processors) {
            if(processor.ignore()) {
                continue;
            }

            // put process results to outputs
            final List<Map<String, Object>> output;
            try {
                output = processor.process(outputs, timestamp);
            } catch (final ProcessingInvalidException e) {
                LOG.warn(String.format("Failed to calculate processor: op=%s, name=%s, cause: %s",
                        processor.getOp(),
                        processor.getName(),
                        e.getMessage()));
                continue;
            }

            for(int i=0; i<outputs.size(); i++) {
                if(output.size() <= i) {
                    continue;
                }
                outputs.get(i).putAll(output.get(i));
            }
        }

        return outputs;
    }

    static KV<List<String>, Boolean> getSingleMultiAttribute(
            final JsonObject params, final String singleName, final String multiName) {

        final List<String> list = new ArrayList<>();
        final Boolean isSingle;
        if(params.has(singleName) || params.has(multiName)) {
            if(params.has(singleName)) {
                if(!params.get(singleName).isJsonPrimitive()) {
                    throw new IllegalArgumentException();
                }
                list.add(params.get(singleName).getAsString());
                isSingle = true;
            } else {
                if(!params.get(multiName).isJsonArray()) {
                    throw new IllegalArgumentException();
                }
                for(final JsonElement field : params.get(multiName).getAsJsonArray()) {
                    if(!field.isJsonPrimitive()) {
                        throw new IllegalArgumentException();
                    }
                    list.add(field.getAsString());
                }
                isSingle = false;
            }
        } else {
            throw new IllegalArgumentException("Illegal processor parameter: " + params + " not contains both " + singleName + " and " + multiName);
        }
        return KV.of(list, isSingle);
    }

    static String createVariableName(final String input, final String separator, final Integer index) {
        return String.format("%s%s%d", input, separator, index);
    }

    class ProcessingInvalidException extends IllegalStateException {
        public ProcessingInvalidException(final String message) {
            super(message);
        }
    }
}
