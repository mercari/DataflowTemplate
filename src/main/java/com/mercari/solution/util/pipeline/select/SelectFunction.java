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

        final SelectFunction selectFunction;
        switch (func) {
            case pass:
                selectFunction = Pass.of(name, outputType, inputFields, ignore);
                break;
            case rename:
                selectFunction = Rename.of(name, jsonObject, outputType, inputFields, ignore);
                break;
            case constant:
                selectFunction = Constant.of(name, jsonObject, ignore);
                break;
            case expression:
                selectFunction = Expression.of(name, jsonObject, ignore);
                break;
            case hash:
                selectFunction = Hash.of(name, jsonObject, ignore);
                break;
            case current_timestamp:
                selectFunction = CurrentTimestamp.of(name, ignore);
                break;
            default:
                throw new IllegalArgumentException("Not supported select function: " + func);
        }

        return selectFunction;
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
        final Map<String, Object> values = new HashMap<>();
        for(final SelectFunction selectFunction : selectFunctions) {
            for(final Schema.Field inputField : selectFunction.getInputFields()) {
                final Object value;
                switch (inputType) {
                    case ROW:
                        value = RowSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                        break;
                    case AVRO:
                        value = AvroSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                        break;
                    case STRUCT:
                        value = StructSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                        break;
                    case DOCUMENT:
                        value = DocumentSchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                        break;
                    case ENTITY:
                        value = EntitySchemaUtil.getAsPrimitive(element, inputField.getType(), inputField.getName());
                        break;
                    default:
                        throw new IllegalArgumentException("SelectFunction not supported input data type: " + inputType);
                }
                values.put(inputField.getName(), value);
            }
        }
        return apply(selectFunctions, values, outputType);
    }

    static Map<String, Object> apply(List<SelectFunction> selectFunctions, Map<String, Object> values, DataType outputType) {
        for(final SelectFunction selectFunction : selectFunctions) {
            if(selectFunction.ignore()) {
                continue;
            }
            final Schema.FieldType fieldType = selectFunction.getOutputFieldType();
            final Object primitiveValue = selectFunction.apply(values);
            final Object value;
            switch (outputType) {
                case ROW:
                    value = RowSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                    break;
                case AVRO:
                    value = AvroSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                    break;
                case STRUCT:
                    value = StructSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                    break;
                case DOCUMENT:
                    value = DocumentSchemaUtil.convertPrimitive(fieldType, primitiveValue);
                    break;
                case ENTITY:
                    value = EntitySchemaUtil.convertPrimitive(fieldType, primitiveValue);
                    break;
                default:
                    throw new IllegalArgumentException("SelectFunction not supported input data type: " + outputType);
            }
            values.put(selectFunction.getName(), value);
        }
        return values;
    }

    static Schema.FieldType getInputFieldType(String field, List<Schema.Field> inputFields) {
        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField.getType();
            } else if(field.contains(".")) {
                final String[] fields = field.split("\\.", 2);
                final Schema.FieldType parentFieldType = getInputFieldType(fields[0], inputFields);
                switch (parentFieldType.getTypeName()) {
                    case ROW:
                        return getInputFieldType(fields[1], parentFieldType.getRowSchema().getFields());
                    case ARRAY:
                    case ITERABLE: {
                        if(!Schema.TypeName.ROW.equals(parentFieldType.getCollectionElementType().getTypeName())) {
                            throw new IllegalArgumentException();
                        }
                        return getInputFieldType(fields[1], parentFieldType.getCollectionElementType().getRowSchema().getFields());
                    }
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }


}
