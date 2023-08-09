package com.mercari.solution.util.pipeline.select;

import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.util.Map;

public class CurrentTimestamp implements SelectFunction {

    private final String name;

    private final Schema.FieldType outputFieldType;
    private final boolean ignore;


    CurrentTimestamp(String name, boolean ignore) {
        this.name = name;
        this.outputFieldType = Schema.FieldType.DATETIME.withNullable(true);
        this.ignore = ignore;
    }

    public static CurrentTimestamp of(String name, boolean ignore) {
        return new CurrentTimestamp(name, ignore);
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
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(Map<String, Object> input) {
        return Instant.now().getMillis() * 1000L;
    }

}
