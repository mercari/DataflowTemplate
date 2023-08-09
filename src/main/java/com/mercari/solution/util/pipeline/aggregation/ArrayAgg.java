package com.mercari.solution.util.pipeline.aggregation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.ParameterUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.*;

public class ArrayAgg implements Aggregator {

    private List<Schema.Field> outputFields;
    private String name;
    private Order order;
    private Boolean flatten;
    private DataType outputType;
    private String condition;
    private Boolean ignore;
    private Boolean isSingleField;

    private List<Schema.Field> inputFields;

    private transient Filter.ConditionNode conditionNode;

    enum Order implements Serializable {
        ascending,
        descending,
        none
    }

    public static ArrayAgg of(final String name,
                              final Schema inputSchema,
                              final DataType outputType,
                              final String condition,
                              final Boolean ignore,
                              final JsonObject params) {

        final ArrayAgg arrayAgg = new ArrayAgg();
        arrayAgg.name = name;
        arrayAgg.outputType = outputType;
        arrayAgg.condition = condition;
        arrayAgg.ignore = ignore;

        final List<String> fields;
        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = ParameterUtil
                    .getSingleMultiAttribute(params, "field", "fields");
            fields = fieldsAndIsSingle.getKey();
            arrayAgg.isSingleField = fieldsAndIsSingle.getValue();
        } else {
            throw new IllegalArgumentException("");
        }

        if(params.has("order")) {
            arrayAgg.order = Order.valueOf(params.get("order").getAsString());
        } else {
            arrayAgg.order = Order.none;
        }

        if(params.has("flatten")) {
            arrayAgg.flatten = params.get("flatten").getAsBoolean();
        } else {
            arrayAgg.flatten = false;
        }

        arrayAgg.inputFields = new ArrayList<>();
        for(final String field : fields) {
            final Schema.Field inputField = inputSchema.getField(field);
            final Schema.Field outputField = Schema.Field.of(field, inputField.getType().withNullable(true));
            arrayAgg.inputFields.add(outputField);
        }

        arrayAgg.outputFields = new ArrayList<>();
        if(arrayAgg.isSingleField) {
            final Schema.Field inputField = inputSchema.getField(fields.get(0));
            final Schema.Field outputField = Schema.Field.of(name, Schema.FieldType.array(inputField.getType().withNullable(true)));
            arrayAgg.outputFields.add(outputField);
        } else {
            if(arrayAgg.flatten) {
                for(final String field : fields) {
                    final Schema.Field inputField = inputSchema.getField(field);
                    final Schema.Field outputField = Schema.Field.of(name + "_" + field, Schema.FieldType.array(inputField.getType().withNullable(true)));
                    arrayAgg.outputFields.add(outputField);
                }
            } else {
                Schema.Builder builder = Schema.builder();
                for(final String field : fields) {
                    final Schema.Field inputField = inputSchema.getField(field);
                    builder = builder.addField(field, inputField.getType().withNullable(true));
                }
                final Schema.Field outputField = Schema.Field.of(name, Schema.FieldType.array(Schema.FieldType.row(builder.build()).withNullable(true)));
                arrayAgg.outputFields.add(outputField);
            }
        }

        return arrayAgg;
    }

    @Override
    public Op getOp() {
        return Op.array_agg;
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
        for(final Schema.Field inputField : inputFields) {
            final String key = this.name + "." + inputField.getName();
            final Object value = valueGetter.getValue(input.getValue(), inputField.getType(), inputField.getName());
            accumulator.add(inputField.getType(), key, value);
        }

        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulator(final Accumulator base, final Accumulator input) {
        for(final Schema.Field inputField : inputFields) {
            final String key = this.name + "." + inputField.getName();
            final List baseList = Optional.ofNullable(base.list(inputField.getType(), key)).orElseGet(ArrayList::new);
            final List inputList = Optional.ofNullable(input.list(inputField.getType(), key)).orElseGet(ArrayList::new);
            for(Object value : inputList) {
                baseList.add(value);
            }
            base.put(inputField.getType(), name + "." + inputField.getName(), baseList);
        }
        return base;
    }

    @Override
    public Map<String,Object> extractOutput(final Accumulator accumulator,
                                            final Map<String, Object> values,
                                            final SchemaUtil.PrimitiveValueConverter converter) {

        if(isSingleField) {
            final Schema.Field inputField = inputFields.get(0);
            final String key = this.name + "." + inputField.getName();
            final List list = accumulator.list(inputField.getType(), key);
            final List output = new ArrayList();
            for(final Object primitiveValue : list) {
                final Object value = converter.convertPrimitive(inputField.getType(), primitiveValue);
                output.add(value);
            }
            values.put(name, output);
        } else {
            if(flatten) {
                for(final Schema.Field inputField : inputFields) {
                    final String key = this.name + "." + inputField.getName();
                    final List list = accumulator.list(inputField.getType(), key);
                    final List output = new ArrayList();
                    for(final Object primitiveValue : list) {
                        final Object value = converter.convertPrimitive(inputField.getType(), primitiveValue);
                        output.add(value);
                    }
                    values.put(name + "_" + inputField.getName(), output);
                }
            } else {
                final int size = accumulator.list(inputFields.get(0).getType(), this.name + "." + inputFields.get(0).getName()).size();
                final Schema outputSchema = outputFields.get(0).getType().getCollectionElementType().getRowSchema();
                final org.apache.avro.Schema outputAvroSchema = RowToRecordConverter.convertSchema(outputSchema);
                final List output = new ArrayList();
                for(int i=0; i<size; i++) {
                    final Map<String, Object> rowValues = new HashMap<>();
                    for(final Schema.Field inputField : inputFields) {
                        final String key = this.name + "." + inputField.getName();
                        final List list = accumulator.list(inputField.getType(), key);
                        final Object primitiveValue = list.get(i);
                        final Object value = converter.convertPrimitive(inputField.getType(), primitiveValue);
                        rowValues.put(inputField.getName(), value);
                    }

                    if(DataType.ROW.equals(outputType)) {
                        final Row row = Row.withSchema(outputSchema).withFieldValues(rowValues).build();
                        output.add(row);
                    } else if(DataType.AVRO.equals(outputType)) {
                        final GenericRecord record = AvroSchemaUtil.create(outputAvroSchema, rowValues);
                        output.add(record);
                    } else {
                        throw new IllegalArgumentException();
                    }
                }
                values.put(name, output);
            }
        }

        return values;
    }

}