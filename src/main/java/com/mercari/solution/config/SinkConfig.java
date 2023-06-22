package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SinkConfig implements Serializable {

    private String name;
    private String module;
    private String input;
    private List<String> inputs;
    private List<String> wait;
    private String outputAvroSchema;
    private JsonObject parameters;
    private Boolean skip;

    private String description;

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

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public List<String> getWait() {
        return wait;
    }

    public void setWait(List<String> wait) {
        this.wait = wait;
    }

    public List<String> getInputs() {
        if(inputs != null && inputs.size() > 0) {
            return inputs;
        } else if(input != null) {
            final List<String> list = new ArrayList<>();
            list.add(input);
            return list;
        } else {
            throw new IllegalArgumentException("Sink module: " + name + " has not input");
        }
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public String getOutputAvroSchema() {
        return outputAvroSchema;
    }

    public void setOutputAvroSchema(String outputAvroSchema) {
        this.outputAvroSchema = outputAvroSchema;
    }

    public JsonObject getParameters() {
        return parameters;
    }

    public void setParameters(JsonObject parameters) {
        this.parameters = parameters;
    }

    public Boolean getSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void outputAvroSchema(final Schema schema) {
        if(outputAvroSchema == null) {
            return;
        }
        if(!outputAvroSchema.startsWith("gs://")) {
            throw new IllegalArgumentException("Parameter outputAvroSchema is illegal: " + outputAvroSchema);
        }
        if(schema == null) {
            throw new IllegalArgumentException("Required schema when outputAvroSchema specified: " + outputAvroSchema);
        }
        try {
            StorageUtil.writeString(outputAvroSchema, schema.toString(true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
