package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;


public interface Aggregator extends Serializable {

    String DEFAULT_SEPARATOR = "_";

    String FIELD_OPTION_ORIGINAL_FIELD = "originalField";
    String FIELD_OPTION_ACCUMULATOR_KEY = "accumulatorKey";


    enum Op implements Serializable {
        count,
        max,
        min,
        argmax,
        argmin,
        last,
        first,
        sum,
        avg,
        std,
        any
    }

    Op getOp();
    String getName();
    Boolean getIgnore();
    Boolean filter(UnionValue input);

    List<String> validate(int parent, int index);

    void setup();

    List<Schema.Field> getOutputFields();

    Accumulator addInput(Accumulator accumulator, UnionValue input, SchemaUtil.PrimitiveValueGetter valueGetter);

    Accumulator mergeAccumulator(Accumulator base, Accumulator input);

    Map<String, Object> extractOutput(Accumulator accumulator, Map<String, Object> values, SchemaUtil.PrimitiveValueConverter converter);


    static Aggregator of(final JsonElement element, final Schema inputSchema) {
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            return null;
        }

        final JsonObject params = element.getAsJsonObject();
        if (!params.has("op")) {
            throw new IllegalArgumentException("Aggregator requires op parameter");
        }

        final String name;
        final String field;
        final String expression;
        final String condition;
        final Boolean ignore;
        final String separator;

        if(params.has("name")) {
            name = params.get("name").getAsString();
        } else {
            name = null;
        }

        if(params.has("field")) {
            field = params.get("field").getAsString();
        } else {
            field = null;
        }
        if(params.has("expression")) {
            expression = params.get("expression").getAsString();
        } else {
            expression = null;
        }

        if(params.has("condition")) {
            condition = params.get("condition").toString();
        } else {
            condition = null;
        }

        if(params.has("ignore")) {
            ignore = params.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        if(params.has("separator")) {
            separator = params.get("separator").getAsString();
        } else {
            separator = DEFAULT_SEPARATOR;
        }

        final Op op = Op.valueOf(params.get("op").getAsString());
        switch (op) {
            case count:
                return Count.of(name, condition, ignore);
            case max:
                return Max.of(name, inputSchema, field, expression, condition, ignore);
            case min:
                return Max.of(name, inputSchema, field, expression, condition, ignore, true);
            case last:
                return Last.of(name, inputSchema, condition, ignore, separator, params);
            case first:
                return Last.of(name, inputSchema, condition, ignore, separator, params, true);
            case argmax:
                return ArgMax.of(name, inputSchema, condition, ignore, separator, params);
            case argmin:
                return ArgMax.of(name, inputSchema, condition, ignore, separator, params, true);
            case sum:
                return Sum.of(name, inputSchema, field, expression, condition, ignore);
            case avg:
                return Avg.of(name, field, expression, condition, ignore, params);
            case std:
            case any:
            default:
                throw new IllegalArgumentException("Not supported op: " + op);
        }
    }

    static Boolean filter(final Filter.ConditionNode conditionNode, final UnionValue unionValue) {
        if(conditionNode == null) {
            return true;
        }
        final Map<String, Object> values = new HashMap<>();
        for(final String variable : conditionNode.getRequiredVariables()) {
            values.put(variable, UnionValue.getFieldValue(unionValue, variable));
        }
        return Filter.filter(conditionNode, values);
    }

    static Double eval(final Expression expression, final Set<String> variables, final UnionValue unionValue) {
        final Map<String, Double> values = new HashMap<>();
        for(final String variable : variables) {
            final Double value = unionValue.getAsDouble(variable);
            values.put(variable, Optional.ofNullable(value).orElse(Double.NaN));
        }
        double expResult = expression.setVariables(values).evaluate();
        return Double.isNaN(expResult) ? null : expResult;
    }

    static boolean compare(final Object v1, final Object v2) {
        return compare(v1, v2, false);
    }

    static boolean compare(final Object v1, final Object v2, final boolean opposite) {
        if(v1 == null) {
            return false;
        } else if(v2 == null) {
            return true;
        }

        final Comparable m = (Comparable) v1;
        final Comparable i = (Comparable) v2;
        final boolean result = m.compareTo(i) > 0;

        if(opposite) {
            return !result;
        } else {
            return result;
        }
    }

    static Object max(final Object v1, final Object v2) {
        return max(v1, v2, false);
    }

    static Object max(final Object v1, final Object v2, final boolean opposite) {
        if(v1 == null) {
            return v2;
        } else if(v2 == null) {
            return v1;
        }

        final Comparable m = (Comparable) v1;
        final Comparable i = (Comparable) v2;
        final boolean flag = m.compareTo(i) > 0;

        if(opposite) {
            return flag ? v2 : v1;
        } else {
            return flag ? v1 : v2;
        }
    }

    static Object sum(final Object v1, final Object v2) {
        if(v1 == null) {
            return v2;
        } else if(v2 == null) {
            return v1;
        }

        if(v1 instanceof Double) {
            return (Double) v1 + (Double) v2;
        } else if(v1 instanceof Long) {
            return (Long) v1 + (Long) v2;
        } else if(v1 instanceof Float) {
            return (Float) v1 + (Float) v2;
        } else if(v1 instanceof Integer) {
            return (Integer) v1 + (Integer) v2;
        } else if(v1 instanceof Short) {
            return (Short) v1 + (Short) v2;
        } else if(v1 instanceof BigDecimal) {
            return ((BigDecimal) v1).add((BigDecimal) v2);
        } else if(v1 instanceof Boolean) {
            return (Boolean) v1 || (Boolean) v2;
        } else if(v1 instanceof String) {
            return (String) v1 + (String) v2;
        }

        throw new IllegalArgumentException("Sum not supported type object: " + v1);
    }

    static String getFieldOptionAccumulatorKey(final Schema.Field field) {
        return field.getOptions().getValue(FIELD_OPTION_ACCUMULATOR_KEY, String.class);
    }

    static String getFieldOptionOriginalFieldKey(final Schema.Field field) {
        return field.getOptions().getValue(FIELD_OPTION_ORIGINAL_FIELD, String.class);
    }

    static Schema.Options createFieldOptionAccumulatorKey(final String key) {
        return Schema.Options.builder()
                .setOption(FIELD_OPTION_ACCUMULATOR_KEY, Schema.FieldType.STRING, key)
                .build();
    }

}
