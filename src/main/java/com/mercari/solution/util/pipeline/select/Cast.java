package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Cast implements SelectFunction {

    private final String name;
    private final String field;
    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Cast(String name, String field, DataType outputType, Schema.FieldType outputFieldType, List<Schema.Field> inputFields, boolean ignore) {
        this.name = name;
        this.field = field;
        this.outputType = outputType;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static Cast of(String name, JsonObject jsonObject, DataType outputType, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("type")) {
            throw new IllegalArgumentException("SelectField cast: " + name + " requires type parameter");
        }
        final String type = jsonObject.get("type").getAsString();

        final String field;
        if(jsonObject.has("field")) {
            if(!jsonObject.get("field").isJsonPrimitive()) {
                throw new IllegalArgumentException("SelectField cast: " + name + ".field parameter must be string");
            }
            field = jsonObject.get("field").getAsString();
        } else {
            field = name;
        }

        final List<Schema.Field> fields = new ArrayList<>();
        final Schema.FieldType inputFieldType = SelectFunction.getInputFieldType(field, inputFields);
        if(inputFieldType == null) {
            throw new IllegalArgumentException("SelectField cast: " + name + " missing inputField: " + field);
        }
        fields.add(Schema.Field.of(field, inputFieldType));

        final Schema.FieldType outputFieldType = Types.createOutputFieldType(type);
        return new Cast(name, field, outputType, outputFieldType, fields, ignore);
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
        return switch (outputType) {
            case ROW -> RowSchemaUtil.getAsPrimitive(outputFieldType, value);
            case AVRO -> AvroSchemaUtil.getAsPrimitive(outputFieldType, value);
            case STRUCT -> StructSchemaUtil.getAsPrimitive(outputFieldType, value);
            case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(outputFieldType, value);
            case ENTITY -> EntitySchemaUtil.getAsPrimitive(outputFieldType, value);
            default -> throw new IllegalArgumentException("SelectField cast: " + name + " does not support output type: " + outputType);
        };
    }

}