package com.mercari.solution.config;

import com.google.gson.JsonObject;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SinkConfig implements Serializable {

    public enum Module {
        storage,
        bigquery,
        spanner,
        bigtable,
        datastore,
        jdbc,
        pubsub
    }

    private String name;
    private Module module;
    private String input;
    private List<String> wait;
    private String outputAvroSchema;
    private JsonObject parameters;

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

    public void outputAvroSchema(final Schema schema) {
        if(outputAvroSchema == null) {
            return;
        }
        if(outputAvroSchema.startsWith("gs://")) {
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
