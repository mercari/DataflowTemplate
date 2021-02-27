package com.mercari.solution.module;

import com.mercari.solution.config.SourceConfig;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.Map;

public interface SourceModule extends Module {
    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits);
}
