package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.DataType;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface SelectFunction extends Serializable {

    String getName();
    Object apply(Map<String, Object> input);
    void setup();
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

    static Schema.FieldType getInputFieldType(String field, List<Schema.Field> inputFields) {
        for(final Schema.Field inputField : inputFields) {
            if(field.equals(inputField.getName())) {
                return inputField.getType();
            }
        }
        throw new IllegalArgumentException("Not found field: " + field + " in input fields: " + inputFields);
    }


}
