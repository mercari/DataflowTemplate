package com.mercari.solution.util.pipeline.select;

import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;

import java.util.List;
import java.util.Map;

public class Pass implements SelectFunction {

    private final String name;
    private final DataType outputType;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Pass(String name, DataType outputType, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.outputType = outputType;
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
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input) {
        final Object value = input.get(name);
        if(DataType.ROW.equals(outputType)) {
            return RowSchemaUtil.getAsPrimitive(outputFieldType, value);
        } else if(DataType.AVRO.equals(outputType)) {
            return AvroSchemaUtil.getAsPrimitive(outputFieldType, value);
        } else {
            throw new IllegalArgumentException("SelectField pass: " + name + " does not support output type: " + outputType);
        }
    }

}