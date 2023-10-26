package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.*;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface SelectFunction extends Serializable {

    String getName();
    Object apply(Map<String, Object> input);
    void setup();
    List<Schema.Field> getInputFields();
    Schema.FieldType getOutputFieldType();
    boolean ignore();

    enum Func implements Serializable {
        pass,
        constant,
        rename,
        expression,
        concat,
        hash,
        current_timestamp
    }


    static List<SelectFunction> of(final JsonArray selects, final List<Schema.Field> inputFields, final DataType outputType) {
        final List<SelectFunction> selectFunctions = new ArrayList<>();
        if(selects == null || !selects.isJsonArray()) {
            return selectFunctions;
        }

        for(final JsonElement select : selects) {
            if(!select.isJsonObject()) {
                continue;
            }
            selectFunctions.add(SelectFunction.of(select.getAsJsonObject(), outputType, inputFields));
        }
        return selectFunctions;
    }

    static SelectFunction of(JsonObject jsonObject, DataType outputType, List<Schema.Field> inputFields) {

        if(!jsonObject.has("name")) {
            throw new IllegalArgumentException("selectField requires name parameter");
        }
        final String name = jsonObject.get("name").getAsString();

        final Func func;
        if(jsonObject.has("func")) {
            func = Func.valueOf(jsonObject.get("func").getAsString());
        } else {
            if(jsonObject.size() == 1) {
                func = Func.pass;
            } else if(jsonObject.has("field")) {
                func = Func.rename;
            } else if(jsonObject.has("value")) {
                if(jsonObject.has("type")) {
                    func = Func.constant;
                } else  {
                    throw new IllegalArgumentException("selectField value requires type parameter");
                }
            } else if(jsonObject.has("expression")) {
                func = Func.expression;
            } else {
                throw new IllegalArgumentException("selectField requires func parameter");
            }
        }

        final boolean ignore;
        if(jsonObject.has("ignore")) {
            ignore = jsonObject.get("ignore").getAsBoolean();
        } else {
            ignore = false;
        }

        return switch (func) {
            case pass -> Pass.of(name, outputType, inputFields, ignore);
            case rename -> Rename.of(name, jsonObject, outputType, inputFields, ignore);
            case constant -> Constant.of(name, jsonObject, ignore);
            case expression -> Expression.of(name, jsonObject, ignore);
            case concat -> Concat.of(name, inputFields, jsonObject, ignore);
            case hash -> Hash.of(name, jsonObject, ignore);
            case current_timestamp -> CurrentTimestamp.of(name, ignore);
        };
    }

    static Schema createSchema(final JsonArray select, List<Schema.Field> outputFields) {
        final List<SelectFunction> selectFunctions = SelectFunction.of(select, outputFields, null);
        return SelectFunction.createSchema(selectFunctions);
    }

    static Schema createSchema(List<SelectFunction> selectFunctions) {
        final List<Schema.Field> selectOutputFields = new ArrayList<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Schema.FieldType selectOutputFieldType = selectFunction.getOutputFieldType();

            selectOutputFields.add(Schema.Field.of(selectFunction.getName(), selectOutputFieldType));
        }
        return Schema.builder().addFields(selectOutputFields).build();
    }

    static Map<String, Object> apply(List<SelectFunction> selectFunctions, Object element, DataType inputType, DataType outputType) {
        final Map<String, Object> primitiveValues = new HashMap<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            for(final Schema.Field inputField : selectFunction.getInputFields()) {
                final Object primitiveValue = switch (inputType) {
                    case ROW -> RowSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                    case AVRO -> AvroSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                    case STRUCT -> StructSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                    case DOCUMENT -> DocumentSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                    case ENTITY -> EntitySchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                    default -> throw new IllegalArgumentException("SelectFunction not supported input data type: " + inputType);
                };
                primitiveValues.put(inputField.getName(), primitiveValue);
            }
        }
        return apply(selectFunctions, primitiveValues, outputType);
    }

    static Map<String, Object> apply(List<SelectFunction> selectFunctions, Map<String, Object> primitiveValues, DataType outputType) {
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Schema.FieldType fieldType = selectFunction.getOutputFieldType();
            final Object primitiveValue = selectFunction.apply(primitiveValues);
            final Object value = switch (outputType) {
                case ROW -> RowSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                case AVRO -> AvroSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                case STRUCT -> StructSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                case DOCUMENT -> DocumentSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                case ENTITY -> EntitySchemaUtil.convertPrimitive(fieldType, primitiveValue);
                default -> throw new IllegalArgumentException("SelectFunction not supported input data type: " + outputType);
            };
            primitiveValues.put(selectFunction.getName(), value);
        }
        return primitiveValues;
    }

    static Schema.FieldType getInputFieldType(String field, List<Schema.Field> inputFields) {
        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField.getType();
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                switch (parentFieldType.getTypeName()) {
                    case ROW -> {
                        return getInputFieldType(fields[1], parentFieldType.getRowSchema().getFields());
                    }
                    case ARRAY, ITERABLE -> {
                        if (!Schema.TypeName.ROW.equals(parentFieldType.getCollectionElementType().getTypeName())) {
                            throw new IllegalArgumentException();
                        }
                        return getInputFieldType(fields[1], parentFieldType.getCollectionElementType().getRowSchema().getFields());
                    }
                    default -> throw new IllegalArgumentException();
                }
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }


}
