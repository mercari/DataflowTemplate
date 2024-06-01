package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Concat implements SelectFunction {

    private final String name;
    private final List<Schema.Field> inputFields;
    private final String delimiter;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    Concat(String name, List<Schema.Field> inputFields, String delimiter, boolean ignore) {
        this.name = name;
        this.inputFields = inputFields;
        this.delimiter = delimiter;
        this.outputFieldType = Schema.FieldType.STRING.withNullable(true);
        this.ignore = ignore;
    }

    public static Concat of(String name, List<Schema.Field> inputFields, JsonObject jsonObject, boolean ignore) {

        if(!jsonObject.has("fields")) {
            throw new IllegalArgumentException();
        }
        final JsonElement fieldsElement = jsonObject.get("fields");
        if(!fieldsElement.isJsonArray()) {
            throw new IllegalArgumentException("SelectField concat: " + name + " fields parameter must be array. but: " + fieldsElement);
        }
        final List<Schema.Field> fields = new ArrayList<>();
        for(JsonElement element : fieldsElement.getAsJsonArray()) {
            if(element.isJsonPrimitive()) {
                final String inputFieldName = element.getAsString();
                final Schema.FieldType inputFieldType = SelectFunction.getInputFieldType(inputFieldName, inputFields);
                if(inputFieldType == null) {
                    throw new IllegalArgumentException("SelectField concat: " + name + " missing inputField: " + inputFieldName);
                }
                fields.add(Schema.Field.of(inputFieldName, inputFieldType));
            }
        }

        final String delimiter;
        if(jsonObject.has("delimiter")) {
            delimiter = jsonObject.get("delimiter").getAsString();
        } else {
            delimiter = "";
        }

        return new Concat(name, fields, delimiter, ignore);
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
        final List<String> strs = new ArrayList<>();
        for(Schema.Field inputField : inputFields) {
            final Object value = input.get(inputField.getName());
            final String str = value == null ? "" : value.toString();
            strs.add(str);
        }
        return String.join(delimiter, strs);
    }
}
