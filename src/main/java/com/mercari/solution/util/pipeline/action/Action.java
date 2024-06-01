package com.mercari.solution.util.pipeline.action;

import com.mercari.solution.util.pipeline.union.UnionValue;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;

public interface Action extends Serializable {

    void setup();
    UnionValue action();
    UnionValue action(UnionValue value);
    Schema getOutputSchema();

    enum Service {
        dataflow,
        bigquery;
    }

}
