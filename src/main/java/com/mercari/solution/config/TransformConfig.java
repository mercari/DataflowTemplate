package com.mercari.solution.config;

import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TransformConfig implements Serializable {

    public enum Module {
        flatten,
        groupby,
        beamsql,
        window,
        setoperation,
        pdfextract,
        crypto,
        protobuf,
        feature,
        javascript,
        onnx,
        reshuffle,
        automl
    }

    private String name;
    private Module module;
    private List<String> inputs;
    private JsonObject parameters;
    private List<String> wait;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Module getModule() {
        return module;
    }

    public void setModule(Module module) {
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
