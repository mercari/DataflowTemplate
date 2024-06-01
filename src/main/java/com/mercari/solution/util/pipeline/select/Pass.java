package com.mercari.solution.util.pipeline.select;

import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Pass implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Pass.class);

    private final String name;
    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Pass(String name, DataType outputType, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.outputType = outputType;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(Schema.Field.of(name, outputFieldType));
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Pass of(String name, DataType outputType, List<Schema.Field> inputFields, boolean ignore) {
        final Schema.FieldType outputFieldType = SelectFunction.getInputFieldType(name, inputFields);
        return new Pass(name, outputType, outputFieldType, ignore);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        final Object value = input.get(name);
        final Object output = switch (outputType) {
            case ROW -> RowSchemaUtil.getAsPrimitive(outputFieldType, value);
            case AVRO -> AvroSchemaUtil.getAsPrimitive(outputFieldType, value);
            case STRUCT -> StructSchemaUtil.getAsPrimitive(outputFieldType, value);
            case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(outputFieldType, value);
            case ENTITY -> EntitySchemaUtil.getAsPrimitive(outputFieldType, value);
            default -> throw new IllegalArgumentException("SelectField pass: " + name + " does not support output type: " + outputType);
        };
        return output;
    }

}