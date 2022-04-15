package com.mercari.solution.module;

import com.mercari.solution.config.SinkConfig;

import java.util.List;
import java.util.Map;

public interface SinkModule extends Module {
    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs);
}
