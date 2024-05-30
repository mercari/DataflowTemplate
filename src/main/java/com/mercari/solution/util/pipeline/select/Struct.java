package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Struct implements SelectFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Struct.class);

    private final String name;
    private final String selectFunctionsJson;

    private final DataType outputType;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final String eachField;
    private final boolean isArray;
    private final boolean ignore;


    private transient List<SelectFunction> selectFunctions;

    Struct(final String name,
           final String selectFunctionsJson,
           final DataType outputType,
           final Schema.FieldType outputFieldType,
           final List<Schema.Field> inputFields,
           final String eachField,
           final boolean isArray,
           final boolean ignore) {

        this.name = name;
        this.selectFunctionsJson = selectFunctionsJson;
        this.outputType = outputType;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.eachField = eachField;
        this.isArray = isArray;
        this.ignore = ignore;
    }

    public static Struct of(String name, JsonObject jsonObject, DataType outputType, List<Schema.Field> inputFields, boolean ignore) {
        if(!jsonObject.has("fields")) {
            throw new IllegalArgumentException("SelectField: " + name + " struct func requires fields parameter");
        } else if(!jsonObject.get("fields").isJsonArray()) {
            throw new IllegalArgumentException("SelectField: " + name + " struct func fields parameter must be array");
        }
        final JsonElement fieldsElement = jsonObject.get("fields");
        final List<SelectFunction> childFunctions = SelectFunction.of(fieldsElement.getAsJsonArray(), inputFields, outputType);
        final Schema outputSchema = SelectFunction.createSchema(childFunctions);
        final List<Schema.Field> nestedInputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : childFunctions) {
            nestedInputFields.addAll(selectFunction.getInputFields());
        }

        final String mode;
        if(jsonObject.has("mode") && jsonObject.get("mode").isJsonPrimitive()) {
            mode = jsonObject.get("mode").getAsString();
        } else {
            mode = "nullable";
        }

        final String eachField;
        if(jsonObject.has("each") && jsonObject.get("each").isJsonPrimitive()) {
            eachField = jsonObject.get("each").getAsString();
        } else {
            eachField = null;
        }

        final Schema.FieldType fieldType = Schema.FieldType.row(outputSchema);
        final Schema.FieldType outputFieldType = switch (mode) {
            case "required" -> fieldType;
            case "nullable" -> fieldType.withNullable(true);
            case "repeated" -> Schema.FieldType.array(fieldType).withNullable(true);
            default -> throw new IllegalArgumentException("illegal struct mode: " + mode);
        };

        final boolean isArray = Schema.TypeName.ARRAY.equals(outputFieldType.getTypeName());
        return new Struct(name, fieldsElement.toString(), outputType, outputFieldType, nestedInputFields, eachField, isArray, ignore);
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
        final JsonArray jsonArray = new Gson().fromJson(this.selectFunctionsJson, JsonArray.class);
        this.selectFunctions = SelectFunction.of(jsonArray, inputFields, outputType);
        for(final SelectFunction selectFunction : this.selectFunctions) {
            selectFunction.setup();
        }
    }

    @Override
    public Object apply(Map<String, Object> input, Instant timestamp) {
        if(isArray) {
            if(eachField == null || !input.containsKey(eachField)) {
                final Map<String, Object> newInput = new HashMap<>(input);
                final Map<String, Object> output = SelectFunction.apply(selectFunctions, newInput, outputType, timestamp);
                return List.of(output);
            }
            final List<Object> eachValues = (List) input.get(eachField);
            if(eachValues == null) {
                return new ArrayList<>();
            }
            final List<Map<String, Object>> outputs = new ArrayList<>();
            for(final Object value :eachValues) {
                final Map<String, Object> newInput = new HashMap<>(input);
                newInput.put("_", value);
                final Map<String, Object> output =  SelectFunction.apply(selectFunctions, newInput, outputType, timestamp);
                outputs.add(output);
            }
            return outputs;
        } else {
            final Map<String, Object> newInput = new HashMap<>(input);
            final Map<String, Object> output = SelectFunction.apply(selectFunctions, newInput, outputType, timestamp);
            return output;
        }
    }

}
