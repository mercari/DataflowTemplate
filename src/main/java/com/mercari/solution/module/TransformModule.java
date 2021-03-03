package com.mercari.solution.module;

import com.mercari.solution.config.TransformConfig;

import java.util.List;
import java.util.Map;

public interface TransformModule extends Module {
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config);
}
