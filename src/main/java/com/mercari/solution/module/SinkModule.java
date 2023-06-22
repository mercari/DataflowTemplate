package com.mercari.solution.module;

import com.mercari.solution.config.SinkConfig;

import java.util.List;
import java.util.Map;

public interface SinkModule extends Module {
    Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits);
}
