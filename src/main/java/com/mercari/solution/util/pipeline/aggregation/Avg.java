package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.domain.math.ExpressionUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;

import java.util.*;

public class Avg implements Aggregator {

    private List<Schema.Field> outputFields;
    private String name;
    private String field;
    private String expression;
    private String weightField;
    private String weightExpression;
    private String condition;

    private Boolean ignore;

    private String weightKeyName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static Avg of(final String name,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore,
                         final JsonObject params) {

        final Avg avg = new Avg();
        avg.name = name;
        avg.field = field;
        avg.expression = expression;
        avg.condition = condition;
        avg.ignore = ignore;

        avg.outputFields = new ArrayList<>();
        avg.outputFields.add(Schema.Field.of(name, Schema.FieldType.DOUBLE.withNullable(true)));

        if(params.has("weightField")) {
            avg.weightField = params.get("weightField").getAsString();
        } else if(params.has("weightExpression")) {
            avg.weightExpression = params.get("weightExpression").getAsString();
        }

        avg.weightKeyName = name + ".weight";

        return avg;
    }

    @Override
    public Op getOp() {
        return Op.avg;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Boolean getIgnore() {
        return ignore;
    }

    @Override
    public Boolean filter(final UnionValue unionValue) {
        return Aggregator.filter(conditionNode, unionValue);
    }

    @Override
    public List<String> validate(int parent, int index) {
        final List<String> errorMessages = new ArrayList<>();
        return errorMessages;
    }

    @Override
    public void setup() {
        if(this.expression != null) {
            final Set<String> variables = ExpressionUtil.estimateVariables(this.expression);
            this.variables = variables;
            this.exp = ExpressionUtil.createDefaultExpression(this.expression, variables);
        }
        if(this.weightExpression != null) {
            final Set<String> weightVariables = ExpressionUtil.estimateVariables(this.weightExpression);
            this.weightVariables = weightVariables;
            this.weightExp = ExpressionUtil.createDefaultExpression(this.weightExpression, weightVariables);
        }
        if(this.condition != null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }
    }

    @Override
    public List<Schema.Field> getOutputFields() {
        return outputFields;
    }

    @Override
    public Accumulator addInput(final Accumulator accumulator, final UnionValue input, final SchemaUtil.PrimitiveValueGetter valueGetter) {
        final Double prevAvg = accumulator.getDouble(name);
        final Double prevWeight = Optional.ofNullable(accumulator.getDouble(weightKeyName)).orElse(0D);
        final Double inputValue;
        if(field != null) {
            inputValue = input.getAsDouble(field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }
        final Double inputWeight;
        if(weightField != null) {
            inputWeight = input.getAsDouble(weightField);
        } else if(weightExpression != null) {
            inputWeight = Aggregator.eval(this.weightExp, weightVariables, input);
        } else {
            inputWeight = 1D;
        }

        final Double avgNext = avg(prevAvg, prevWeight, inputValue, inputWeight);
        accumulator.putDouble(name, avgNext);
        if(inputValue != null) {
            accumulator.putDouble(weightKeyName, prevWeight + inputWeight);
        } else {
            accumulator.putDouble(weightKeyName, prevWeight);
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseAvg = base.getDouble(name);
        final Double baseWeight = base.getDouble(weightKeyName);
        final Double inputAvg = input.getDouble(name);
        final Double inputWeight = input.getDouble(weightKeyName);
        final Double avg = avg(baseAvg, baseWeight, inputAvg, inputWeight);
        final Double weight = Optional.ofNullable(baseWeight).orElse(0D) + Optional.ofNullable(inputWeight).orElse(0D);
        base.putDouble(name, avg);
        base.putDouble(weightKeyName, weight);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values,
                                            final SchemaUtil.PrimitiveValueConverter converter) {

        final Double avg = accumulator.getDouble(name);
        values.put(name, avg);
        return values;
    }

    private Double avg(final Double avg1, final Double count1, final Double avg2, final Double count2) {
        if(avg1 == null) {
            return avg2;
        } else if(avg2 == null) {
            return avg1;
        }

        if(count1 == null || count1 == 0) {
            return avg2;
        } else if(count2 == null || count2 == 0) {
            return avg1;
        }

        return (count1 * avg1 + count2 * avg2) / (count1 + count2);

    }

}
