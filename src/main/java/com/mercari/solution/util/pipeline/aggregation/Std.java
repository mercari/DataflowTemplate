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

public class Std implements Aggregator {

    private List<Schema.Field> outputFields;
    private String name;
    private String field;
    private String expression;
    private String weightField;
    private String weightExpression;
    private Integer ddof; // Delta Degree of Freedom
    private String condition;

    private Boolean ignore;

    private String accumKeyAvgName;
    private String accumKeyCountName;
    private String accumKeyWeightName;


    private transient Expression exp;
    private transient Set<String> variables;

    private transient Expression weightExp;
    private transient Set<String> weightVariables;

    private transient Filter.ConditionNode conditionNode;


    public static Std of(final String name,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore,
                         final JsonObject params) {

        final Std std = new Std();
        std.name = name;
        std.field = field;
        std.expression = expression;
        std.condition = condition;
        std.ignore = ignore;

        std.outputFields = new ArrayList<>();
        std.outputFields.add(Schema.Field.of(name, Schema.FieldType.DOUBLE.withNullable(true)));

        if(params.has("weightField")) {
            std.weightField = params.get("weightField").getAsString();
        } else if(params.has("weightExpression")) {
            std.weightExpression = params.get("weightExpression").getAsString();
        }

        if(params.has("ddof") && params.get("ddof").isJsonPrimitive()) {
            std.ddof = params.get("ddof").getAsInt();
        } else {
            std.ddof = 1;
        }

        std.accumKeyAvgName = name + ".avg";
        std.accumKeyCountName = name + ".count";
        std.accumKeyWeightName = name + ".weight";

        return std;
    }

    @Override
    public Op getOp() {
        return Op.std;
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
        final Double inputValue;
        if(field != null) {
            inputValue = input.getDouble(field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }
        if(inputValue == null || Double.isNaN(inputValue)) {
            return accumulator;
        }
        Double inputWeight;
        if(weightField != null) {
            inputWeight = input.getDouble(weightField);
        } else if(weightExpression != null) {
            inputWeight = Aggregator.eval(this.weightExp, weightVariables, input);
        } else {
            inputWeight = 1D;
        }
        if(inputWeight != null && !Double.isNaN(inputWeight) && inputWeight < 0) {
            inputWeight = Math.abs(inputWeight);
        }

        return add(accumulator, inputValue, inputWeight);
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Double baseCount = Optional.ofNullable(base.getDouble(accumKeyCountName)).orElse(0D);
        final Double inputCount = Optional.ofNullable(input.getDouble(accumKeyCountName)).orElse(0D);
        if(baseCount == 0) {
            return input;
        } else if(inputCount == 0) {
            return base;
        } else if(baseCount == 1 || inputCount == 1) {
            final Double inputValue;
            final Double inputWeight;
            if(inputCount == 1) {
                inputValue = input.getDouble(accumKeyAvgName);
                inputWeight = input.getDouble(accumKeyWeightName);
                return add(base, inputValue, inputWeight);
            } else {
                inputValue = base.getDouble(accumKeyAvgName);
                inputWeight = base.getDouble(accumKeyWeightName);
                return add(input, inputValue, inputWeight);
            }
        }

        final Double baseAvg = base.getDouble(accumKeyAvgName);
        final Double baseWeight = base.getDouble(accumKeyWeightName);
        final Double inputAvg = input.getDouble(accumKeyAvgName);
        final Double inputWeight = input.getDouble(accumKeyWeightName);
        final Double avg = Aggregator.avg(baseAvg, baseWeight, inputAvg, inputWeight);
        final Double count = baseCount + inputCount;
        final Double weight = Optional.ofNullable(baseWeight).orElse(0D) + Optional.ofNullable(inputWeight).orElse(0D);
        base.putDouble(accumKeyAvgName, avg);
        base.putDouble(accumKeyCountName, count);
        base.putDouble(accumKeyWeightName, weight);

        final Double baseVar = Optional.ofNullable(base.getDouble(name)).orElse(0D);
        final Double inputVar = Optional.ofNullable(input.getDouble(name)).orElse(0D);
        base.putDouble(name, baseVar + inputVar);

        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values,
                                            final SchemaUtil.PrimitiveValueConverter converter) {

        final Double var = accumulator.getDouble(name);
        final Double weight = Optional.ofNullable(accumulator.getDouble(accumKeyWeightName)).orElse(0D);
        if(var != null && weight != 0 && weight - ddof > 0) {
            values.put(name, Math.sqrt(var / (weight - ddof)));
        } else {
            values.put(name, null);
        }
        return values;
    }

    private Accumulator add(final Accumulator accumulator, final Double inputValue, final Double inputWeight) {
        if(inputValue == null || Double.isNaN(inputValue)) {
            return accumulator;
        }

        final Double prevAvg = accumulator.getDouble(accumKeyAvgName);
        final Double prevWeight = Optional.ofNullable(accumulator.getDouble(accumKeyWeightName)).orElse(0D);
        final Double nextAvg = Aggregator.avg(prevAvg, prevWeight, inputValue, inputWeight);
        final Double nextWeight = prevWeight + Optional.ofNullable(inputWeight).orElse(0D);
        accumulator.putDouble(accumKeyAvgName, nextAvg);
        accumulator.putDouble(accumKeyCountName, Optional.ofNullable(accumulator.getDouble(accumKeyCountName)).orElse(0D) + 1D);
        accumulator.putDouble(accumKeyWeightName, nextWeight);

        double deltaPrev = inputValue - Optional.ofNullable(prevAvg).orElse(0D);
        double deltaNext = inputValue - Optional.ofNullable(nextAvg).orElse(0D);
        final Double prevVar = Optional.ofNullable(accumulator.getDouble(name)).orElse(0D);
        final Double nextVar = prevVar + (deltaPrev * deltaNext);

        accumulator.putDouble(name, nextVar);

        return accumulator;
    }

}
