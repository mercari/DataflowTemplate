package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Rename implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Rename.class);

    private final String name;
    private final String field;

    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Rename(String name, String field, DataType outputType, Schema.FieldType outputFieldType, boolean ignore) {
        this.name = name;
        this.field = field;
        this.outputType = outputType;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(Schema.Field.of(field, outputFieldType));
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Rename of(String name, JsonObject jsonObject, DataType outputType, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("field")) {
            throw new IllegalArgumentException("SelectField: " + name + " requires field parameter");
        }
        final String field = jsonObject.get("field").getAsString();

        final Schema.FieldType outputFieldType = SelectFunction.getInputFieldType(field, inputFields);
        return new Rename(name, field, outputType, outputFieldType, ignore);
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
        final Object value = input.get(field);
        final Object output = switch (outputType) {
            case ROW -> RowSchemaUtil.getAsPrimitive(outputFieldType, value);
            case AVRO -> AvroSchemaUtil.getAsPrimitive(outputFieldType, value);
            case STRUCT -> StructSchemaUtil.getAsPrimitive(outputFieldType, value);
            case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(outputFieldType, value);
            case ENTITY -> EntitySchemaUtil.getAsPrimitive(outputFieldType, value);
            default -> throw new IllegalArgumentException("SelectField rename: " + name + " does not support output type: " + outputType);
        };
        return output;
    }

}
