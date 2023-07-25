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

public class Sum implements Aggregator {

    private List<Schema.Field> outputFields;
    private Schema.Field sumField;

    private String name;
    private String field;
    private String expression;
    private String condition;

    private Boolean ignore;

    private transient Expression exp;
    private transient Set<String> variables;
    private transient Filter.ConditionNode conditionNode;


    public static Sum of(final String name,
                         final Schema inputSchema,
                         final String field,
                         final String expression,
                         final String condition,
                         final Boolean ignore) {

        final Sum sum = new Sum();
        sum.name = name;
        sum.field = field;
        sum.expression = expression;
        sum.condition = condition;
        sum.ignore = ignore;

        sum.outputFields = new ArrayList<>();
        if(field != null) {
            final Schema.Field inputField = inputSchema.getField(field);
            sum.sumField = Schema.Field.of(name, inputField.getType().withNullable(true));
            sum.outputFields.add(sum.sumField);
        } else {
            sum.sumField = Schema.Field.of(name, Schema.FieldType.DOUBLE.withNullable(true));
            sum.outputFields.add(sum.sumField);
        }

        return sum;
    }

    @Override
    public Op getOp() {
        return Op.sum;
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
        final Object prevValue = accumulator.get(sumField.getType(), name);
        final Object inputValue;
        if(field != null) {
            inputValue = valueGetter.getValue(input.getValue(), sumField.getType(), field);
        } else {
            inputValue = Aggregator.eval(this.exp, variables, input);
        }

        final Object sumNext = Aggregator.sum(prevValue, inputValue);
        accumulator.put(sumField.getType(), name, sumNext);
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        final Object stateValue = base.get(sumField.getType(), name);
        final Object accumValue = input.get(sumField.getType(), name);
        final Object sum = Aggregator.sum(stateValue, accumValue);
        base.put(sumField.getType(), name, sum);
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values,
                                            final SchemaUtil.PrimitiveValueConverter converter) {

        final Object maxValue = accumulator.get(sumField.getType(), name);
        final Object fieldValue = converter.convertPrimitive(sumField.getType(), maxValue);
        if(fieldValue == null) {
            values.put(name, Accumulator.convertNumberValue(sumField.getType(), 0D));
        } else {
            values.put(name, fieldValue);
        }
        return values;
    }

}
