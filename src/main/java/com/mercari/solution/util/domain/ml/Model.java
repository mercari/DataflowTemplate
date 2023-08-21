package com.mercari.solution.util.domain.ml;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface Model extends Serializable {

    void update(List<String> fields, Map<String, Double> values);
    List<Double> inference(List<String> fields, Map<String, Double> values);
    List<Double> inference(double[] x);

}
