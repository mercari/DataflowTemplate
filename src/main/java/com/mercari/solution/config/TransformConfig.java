package com.mercari.solution.config;

import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.List;

public class TransformConfig implements Serializable {

    private String name;
    private String module;
    private List<String> inputs;
    private JsonObject parameters;
    private List<String> wait;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public void setParameters(JsonObject parameters) {
        this.parameters = parameters;
    }

    public List<String> getWait() {
        return wait;
    }

    public void setWait(List<String> wait) {
        this.wait = wait;
    }

}
