package com.mercari.solution.module;

import com.mercari.solution.module.sink.*;
import com.mercari.solution.module.source.*;
import com.mercari.solution.module.transform.*;

import java.util.HashMap;
import java.util.Map;

public class ModuleRegistry {
    private static final ModuleRegistry instance = new ModuleRegistry();

    private final Map<String, SourceModule> sources = new HashMap<>();
    private final Map<String, TransformModule> transforms = new HashMap<>();
    private final Map<String, SinkModule> sinks = new HashMap<>();

    public static ModuleRegistry getInstance() {
        return instance;
    }

    private ModuleRegistry() {
        registerBuiltinModules();
    }

    private void registerBuiltinModules() {
        registerSource(new BigQuerySource());
        registerSource(new DatastoreSource());
        registerSource(new JdbcSource());
        registerSource(new PubSubSource());
        registerSource(new SpannerBackupSource());
        registerSource(new SpannerSource());
        registerSource(new StorageSource());

        registerTransform(new AutoMLTablesTransform());
        registerTransform(new BeamSQLTransform());
        registerTransform(new FeatureTransform());
        registerTransform(new FlattenTransform());
        registerTransform(new GroupByTransform());
        registerTransform(new PDFExtractTransform());
        registerTransform(new ReshuffleTransform());
        registerTransform(new SetOperationTransform());
        registerTransform(new WindowTransform());

        registerSink(new BigQuerySink());
        registerSink(new BigtableSink());
        registerSink(new DatastoreSink());
        registerSink(new JdbcSink());
        registerSink(new SolrIndexSink());
        registerSink(new SpannerSink());
        registerSink(new StorageSink());
        registerSink(new TextSink());
    }

    public void registerSource(SourceModule module) {
        sources.put(module.getName(), module);
    }

    public void registerTransform(TransformModule module) {
        transforms.put(module.getName(), module);
    }

    public void registerSink(SinkModule module) {
        sinks.put(module.getName(), module);
    }

    public SourceModule getSource(String name) {
        return sources.get(name);
    }

    public TransformModule getTransform(String name) {
        return transforms.get(name);
    }

    public SinkModule getSink(String name) {
        return sinks.get(name);
    }
}
