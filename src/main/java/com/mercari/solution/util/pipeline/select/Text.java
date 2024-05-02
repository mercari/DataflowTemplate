package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.TemplateUtil;
import freemarker.template.Template;
import org.apache.beam.sdk.schemas.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Text implements SelectFunction {

    private final String name;
    private final String text;
    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    private transient Template template;


    Text(String name, String text, boolean ignore) {
        this.name = name;
        this.text = text;
        this.inputFields = new ArrayList<>();
        this.outputFieldType = Schema.FieldType.STRING.withNullable(true);
        this.ignore = ignore;
    }

    public static Text of(String name, JsonObject jsonObject, boolean ignore) {

        if(!jsonObject.has("text") && !jsonObject.has("value")) {
            throw new IllegalArgumentException("Select function[" + name + "] text or value requires text parameter");
        }
        final JsonElement textElement;
        if(jsonObject.has("text")) {
            textElement = jsonObject.get("text");
        } else {
            textElement = jsonObject.get("value");
        }
        if(!textElement.isJsonPrimitive()) {
            throw new IllegalArgumentException("Select function[" + name + "].text or value parameter must be string. but: " + textElement);
        }

        final String text = textElement.getAsString();
        return new Text(name, text, ignore);
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
        this.template = TemplateUtil.createStrictTemplate(name, text);
    }

    @Override
    public Object apply(Map<String, Object> input) {
        final Map<String, Object> values = new HashMap<>(input);
        TemplateUtil.setFunctions(values);
        final String output = TemplateUtil.executeStrictTemplate(template, values);
        return output;
    }
}
