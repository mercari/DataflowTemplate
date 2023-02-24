package com.mercari.solution.util.domain.sequence;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.domain.math.ExpressionUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

public class Expression implements Sequencer {

    private final String name;
    private final String expression;
    private final Set<String> variables;

    private transient net.objecthunter.exp4j.Expression exp;


    Expression(final JsonObject params) {
        if(params.has("name")) {
            this.name = params.get("name").getAsString();
        } else {
            this.name = null;
        }

        if(params.has("expression")) {
            this.expression = params.get("expression").getAsString();
        } else {
            this.expression = null;
        }

        if(!params.has("variables") || params.get("variables").isJsonNull()) {
            this.variables = ExpressionUtil.estimateVariables(expression);
        } else {
            this.variables = new HashSet<>();
            for(final JsonElement element : params.get("variables").getAsJsonArray()) {
                this.variables.add(element.getAsString());
            }
        }
    }

    @Override
    public Map<String, Schema.FieldType> outputTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        final Map<String, Schema.FieldType> outputTypes = new HashMap<>();
        outputTypes.put(name, Schema.FieldType.DOUBLE.withNullable(true));
        return outputTypes;
    }

    @Override
    public Map<String, Schema.FieldType> stateTypes(final Schema inputSchema, final Map<String, Schema.FieldType> stateTypes) {
        return new HashMap<>();
    }

    @Override
    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(this.name == null) {
            errorMessages.add("sequencer:Expression.name must not be null.");
        }
        if(this.expression == null) {
            errorMessages.add(String.format("sequencer:Expression[%s].expression must not be null.", this.name));
        }
        return errorMessages;
    }

    @Override
    public void setup() {
        this.exp = ExpressionUtil.createDefaultExpression(this.expression, this.variables);
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

        final Map<String, Double> values = new HashMap<>();
        for(final String variable : this.variables) {
            final Object value = Sequencer.getValue(input, state, updateValues, valueGetter, variable);
            final Double doubleValue = ExpressionUtil.getAsDouble(value);
            values.put(variable, doubleValue);
        }
        final double output = this.exp.setVariables(values).evaluate();
        updateValues.put(name, output);
    }

}
