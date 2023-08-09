package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.domain.math.ExpressionUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.SchemaUtil;
import net.objecthunter.exp4j.Expression;
import org.apache.beam.sdk.schemas.Schema;

import java.util.*;

public class Max implements Aggregator {

    private List<Schema.Field> outputFields;
    private Schema.Field maxField;

    private String name;
    private String field;
    private String expression;
    private String condition;

    private Boolean opposite;
    private Boolean ignore;

    private transient Expression exp;
    private transient Set<String> variables;
    private transient Filter.ConditionNode conditionNode;

    public static Max of(final String name,
                         final Schema inputSchema,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore) {

        return of(name, inputSchema, field, expression, condition, ignore, false);
    }

    public static Max of(final String name,
                         final Schema inputSchema,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore,
                         final Boolean opposite) {

        if(field == null && expression == null) {
            throw new IllegalArgumentException("Aggregation max: " + name + " requires field or expression parameter");
        }

        final Max max = new Max();
        max.name = name;
        max.field = field;
        max.expression = expression;
        max.condition = condition;
        max.ignore = ignore;
        max.opposite = opposite;

        max.outputFields = new ArrayList<>();
        if(field != null) {
            final Schema.Field inputField = inputSchema.getField(field);
            max.maxField = Schema.Field.of(name, inputField.getType().withNullable(true));
            max.outputFields.add(max.maxField);
        } else {
            max.maxField = Schema.Field.of(name, Schema.FieldType.DOUBLE.withNullable(true));
            max.outputFields.add(max.maxField);
        }

        return max;
    }

    @Override
    public Op getOp() {
        return this.opposite ? Op.min : Op.max;
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
        final Object prevValue = accumulator.get(maxField.getType(), name);
        final Object inputValue;
        if(field != null) {
            inputValue = valueGetter.getValue(input.getValue(), maxField.getType(), field);//UnionValue.getFieldValue(input, field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }

        final Object maxNext = Aggregator.max(prevValue, inputValue, opposite);
        accumulator.put(maxField.getType(), name, maxNext);
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Object stateValue = base.get(maxField.getType(), name);
        final Object accumValue = input.get(maxField.getType(), name);
        final Object max = Aggregator.max(stateValue, accumValue, opposite);
        base.put(maxField.getType(), name, max);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values,
                                            final SchemaUtil.PrimitiveValueConverter converter) {

        final Object maxValue = accumulator.get(maxField.getType(), name);
        final Object fieldValue = converter.convertPrimitive(maxField.getType(), maxValue);
        values.put(name, fieldValue);
        return values;
    }

}
