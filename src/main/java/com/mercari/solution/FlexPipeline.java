package com.mercari.solution;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.mercari.solution.config.*;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.sink.*;
import com.mercari.solution.module.source.*;
import com.mercari.solution.module.transform.*;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class FlexPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FlexPipeline.class);

    public interface FlexPipelineOptions extends BeamSqlPipelineOptions, GcpOptions, AwsOptions, StreamingOptions {

        @Description("Config json text body or gcs path.")
        String getConfig();
        void setConfig(String config);

    }

    public interface FlexDataflowPipelineOptions extends FlexPipelineOptions, DataflowPipelineOptions { }

    public static void main(final String[] args) throws Exception {

        final FlexPipelineOptions localOptions = PipelineOptionsFactory
                .fromArgs(filterPipelineArgs(args))
                .as(FlexPipelineOptions.class);
        final Config config = getConfig(localOptions.getConfig(), args);
        setSettingsOptions(localOptions, config);

        if(isDirectRunner(args)) {
            LOG.info("DirectRunner mode.");
            final Pipeline pipeline = Pipeline.create(localOptions);
            run(pipeline, config);
            return;
        }

        LOG.info("DataflowRunner mode.");
        final FlexDataflowPipelineOptions flexOptions = localOptions.as(FlexDataflowPipelineOptions.class);
        if(flexOptions.isStreaming()) {
            flexOptions.setEnableStreamingEngine(true);
        } else {
            final List<String> experiments = new ArrayList<>();
            experiments.add("shuffle_mode=service");
            flexOptions.setExperiments(experiments);
        }
        final Pipeline pipeline = Pipeline.create(flexOptions);
        run(pipeline, config);
    }

    private static void run(final Pipeline pipeline, final Config config) {
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        final Set<String> executedModuleNames = new HashSet<>();
        final Set<String> moduleNames = moduleNames(config);

        final PCollection<Long> beats = pipeline.getOptions().as(StreamingOptions.class).isStreaming() && containsMicrobatch(config.getSources())
                ? pipeline
                .apply("MicrobatchBeats", GenerateSequence
                        .from(0)
                        .withRate(1, Duration.millis(1000L)))
                : null;

        final int size = moduleNames.size();
        int preOutputSize = 0;
        while(preOutputSize < size) {
            setSourceResult(pipeline.begin(), beats, config.getSources(), outputs, executedModuleNames);
            setTransformResult(config.getTransforms(), outputs, executedModuleNames);
            setSinkResult(config.getSinks(), outputs, executedModuleNames);
            if(preOutputSize == executedModuleNames.size()) {
                moduleNames.removeAll(executedModuleNames);
                final String message = String.format("No input for modules: %s",
                        String.join(",", moduleNames));
                throw new IllegalArgumentException(message);
            }
            preOutputSize = executedModuleNames.size();
        }

        pipeline.run();
    }

    private static <O extends FlexPipelineOptions> void setSettingsOptions(final O options, final Config config) {

        options.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");

        if(config.getSettings() != null) {
            final Settings settings = config.getSettings();

            if(settings.getStreaming() != null) {
                options.setStreaming(settings.getStreaming());
            }

            if(settings.getBeamsql() != null) {
                final Settings.BeamSQLSettings beamsql = settings.getBeamsql();
                options.setPlannerName(beamsql.getPlannerName());
                //options.setPlannerName("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner");
            }

            if(settings.getAws() != null) {
                final Settings.AWSSettings aws = settings.getAws();
                final BasicAWSCredentials credentials = new BasicAWSCredentials(
                        aws.getAccessKey(),
                        aws.getSecretKey()
                );
                options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(credentials));
                options.setAwsRegion(aws.getRegion());
            }
        }
    }

    private static boolean isStreaming(final String[] args) {
        return Arrays.stream(args)
                .filter(Objects::nonNull)
                .map(s -> s.contains("=") ? s.split("=")[0] : s)
                .map(String::trim)
                .map(String::toLowerCase)
                .anyMatch("--streaming"::equals);
    }

    private static boolean isDirectRunner(final String[] args) {
        return Arrays.stream(args)
                .filter(Objects::nonNull)
                .filter(s -> s.contains("="))
                .map(s -> s.split("="))
                .filter(s -> s.length > 1)
                .anyMatch(s -> s[0].toLowerCase().equals("--runner") && s[1].toLowerCase().equals("directrunner"));
    }

    private static String[] filterPipelineArgs(final String[] args) {
        final List<String> filteredArgs = Arrays.stream(args)
                .filter(s -> !s.contains("=") || !s.split("=")[0].contains("."))
                .collect(Collectors.toList());
        return filteredArgs.toArray(new String[filteredArgs.size()]);
    }

    private static Config getConfig(final String configParam, final String[] args) throws Exception {
        if(configParam == null) {
            throw new IllegalArgumentException("Parameter config must not be null!");
        }

        final String jsonText;
        if(configParam.startsWith("gs://")) {
            jsonText = StorageUtil.readString(configParam);
        } else if(Files.exists(Paths.get(configParam)) && !Files.isDirectory(Paths.get(configParam))) {
            jsonText = new String(Files.readAllBytes(Paths.get(configParam)));
        } else {
            jsonText = configParam;
        }
        return Config.parse(jsonText, args);
    }

    private static Set<String> moduleNames(final Config config) {
        final Set<String> moduleNames = new HashSet<>();
        moduleNames.addAll(config.getSources().stream()
                .filter(Objects::nonNull)
                .map(SourceConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getTransforms().stream()
                .filter(Objects::nonNull)
                .map(TransformConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getSinks().stream()
                .filter(Objects::nonNull)
                .map(SinkConfig::getName)
                .collect(Collectors.toSet()));
        return moduleNames;
    }

    private static void setSourceResult(
            final PBegin begin,
            final PCollection<Long> beats,
            final List<SourceConfig> sourceConfigs,
            final Map<String, FCollection<?>> outputs,
            final Set<String> executedModuleNames) {

        final boolean isStreaming = begin.getPipeline().getOptions().as(DataflowPipelineOptions.class).isStreaming();

        final List<SourceConfig> notDoneModules = new ArrayList<>();
        for (final SourceConfig sourceConfig : sourceConfigs) {
            if(sourceConfig == null) {
                continue;
            }

            if(executedModuleNames.contains(sourceConfig.getName())) {
                continue;
            }

            if(sourceConfig.getWait() != null && !outputs.keySet().containsAll(sourceConfig.getWait())) {
                notDoneModules.add(sourceConfig);
                continue;
            }

            final List<FCollection<?>> wait;
            if(sourceConfig.getWait() == null) {
                wait = null;
            } else {
                wait = sourceConfig.getWait().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            //
            if(!isStreaming && sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                throw new IllegalArgumentException("Config batch mode must be set batch mode for all inputs");
            }
            switch (sourceConfig.getModule()) {
                case storage: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        //inputs.put(sourceConfig.getName(), beats.apply(sourceConfig.getName(), StorageSource.microbatch(sourceConfig)));
                    } else {
                        outputs.put(sourceConfig.getName(), StorageSource.batch(begin, sourceConfig));
                    }
                    break;
                }
                case bigquery: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        //inputs.put(sourceConfig.getName(), beats.apply(sourceConfig.getName(), BigQuerySource.microbatch(sourceConfig)));
                    } else {
                        outputs.put(sourceConfig.getName(), BigQuerySource.batch(begin, sourceConfig, wait));
                    }
                    break;
                }
                case spanner: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        outputs.put(sourceConfig.getName(), SpannerSource.microbatch(beats, sourceConfig));
                    } else {
                        outputs.put(sourceConfig.getName(), SpannerSource.batch(begin, sourceConfig));
                    }
                    break;
                }
                case datastore: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        //inputs.put(sourceConfig.getName(), beats.apply(sourceConfig.getName(), SpannerSource.microbatch(sourceConfig)));
                    } else {
                        outputs.put(sourceConfig.getName(), DatastoreSource.batch(begin, sourceConfig));
                    }
                    break;
                }
                case jdbc: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        //inputs.put(sourceConfig.getName(), beats.apply(sourceConfig.getName(), SpannerSource.microbatch(sourceConfig)));
                    } else {
                        outputs.put(sourceConfig.getName(), JdbcSource.batch(begin, sourceConfig));
                    }
                    break;
                }
                case pubsub: {
                    if (isStreaming) {
                        outputs.put(sourceConfig.getName(), PubSubSource.stream(begin, sourceConfig));
                    } else {
                        throw new IllegalArgumentException("PubSubSource only support streaming mode.");
                    }
                    break;
                }
                case spannerBackup: {
                    if (sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                        throw new IllegalArgumentException("SpannerBackupSource does not support microbatch mode.");
                    }
                    outputs.putAll(SpannerBackupSource.batch(begin, sourceConfig));
                    break;
                }
                default:
                    throw new IllegalArgumentException("module " + sourceConfig.getModule() + " not supported !");
            }
            executedModuleNames.add(sourceConfig.getName());
        }

        if(notDoneModules.size() == 0) {
            return;
        }
        if(notDoneModules.size() == sourceConfigs.size()) {
            return;
        }
        setSourceResult(begin, beats, notDoneModules, outputs, executedModuleNames);
    }

    private static void setTransformResult(
            final List<TransformConfig> transformConfigs,
            final Map<String, FCollection<?>> outputs,
            final Set<String> executedModuleNames) {

        final List<TransformConfig> notDoneModules = new ArrayList<>();
        for(final TransformConfig transformConfig : transformConfigs) {
            // Skip null config(ketu comma)
            if(transformConfig == null) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(transformConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(transformConfig.getWait() != null && !outputs.keySet().containsAll(transformConfig.getWait())) {
                notDoneModules.add(transformConfig);
                continue;
            }

            final List<FCollection<?>> wait;
            if(transformConfig.getWait() == null) {
                wait = null;
            } else {
                wait = transformConfig.getWait().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            // Add queue if all input not done.
            if(!outputs.keySet().containsAll(transformConfig.getInputs())) {
                notDoneModules.add(transformConfig);
                continue;
            }

            final List<FCollection<?>> inputs = transformConfig.getInputs().stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());
            switch (transformConfig.getModule()) {
                case flatten:
                    outputs.put(transformConfig.getName(), FlattenTransform.transform(inputs, transformConfig));
                    break;
                case groupby:
                    outputs.put(transformConfig.getName(), GroupByTransform.transform(inputs, transformConfig));
                    break;
                case beamsql:
                    outputs.put(transformConfig.getName(), BeamSQLTransform.transform(inputs, transformConfig));
                    break;
                case window:
                    outputs.putAll(WindowTransform.transform(inputs, transformConfig));
                    break;
                case reshuffle:
                    outputs.putAll(ReshuffleTransform.transform(inputs, transformConfig));
                    break;
                case setoperation:
                    outputs.putAll(SetOperationTransform.transform(inputs, transformConfig));
                    break;
                case pdfextract:
                    outputs.putAll(PDFExtractTransform.transform(inputs, transformConfig));
                    break;
                case feature:
                    outputs.put(transformConfig.getName(), FeatureTransform.transform(inputs, transformConfig));
                    break;
                case automl:
                    if(transformConfig.getInputs().size() != 1) {
                        throw new IllegalArgumentException("module mlengine must not be multi input !");
                    }
                    //rows.put(transformName, rows.get(transformConfig.getInputs().get(0)).apply(AutoMLTablesTransform.process(transformConfig)));
                    break;
                case onnx:
                case javascript:
                default:
                    throw new UnsupportedOperationException("Module: " + transformConfig.getModule() + " is not supported !");
            }
            executedModuleNames.add(transformConfig.getName());
        }

        if(notDoneModules.size() == 0) {
            return;
        }
        if(notDoneModules.size() == transformConfigs.size()) {
            return;
        }
        setTransformResult(notDoneModules, outputs, executedModuleNames);
    }

    private static void setSinkResult(
            final List<SinkConfig> sinkConfigs,
            final Map<String, FCollection<?>> outputs,
            final Set<String> executedModuleNames) {

        final List<SinkConfig> notDoneModules = new ArrayList<>();
        for(final SinkConfig sinkConfig : sinkConfigs) {
            // Skip null config(ketu comma)
            if(sinkConfig == null) {
                continue;
            }

            // Skip already done module.
            if(executedModuleNames.contains(sinkConfig.getName())) {
                continue;
            }

            // Add queue if wait not done.
            if(sinkConfig.getWait() != null && !outputs.keySet().containsAll(sinkConfig.getWait())) {
                notDoneModules.add(sinkConfig);
                continue;
            }

            final List<FCollection<?>> wait;
            if(sinkConfig.getWait() == null) {
                wait = null;
            } else {
                wait = sinkConfig.getWait().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            // Add queue if input not done.
            if(!outputs.keySet().contains(sinkConfig.getInput())) {
                notDoneModules.add(sinkConfig);
                continue;
            }
            final FCollection<?> input = outputs.get(sinkConfig.getInput());

            switch (sinkConfig.getModule()) {
                case bigquery:
                    outputs.put(sinkConfig.getName(), BigQuerySink.write(input, sinkConfig, wait));
                    break;
                case spanner:
                    outputs.put(sinkConfig.getName(), SpannerSink.write(input, sinkConfig, wait));
                    break;
                case storage:
                    outputs.put(sinkConfig.getName(), StorageSink.write(input, sinkConfig, wait));
                    break;
                case bigtable:
                    outputs.put(sinkConfig.getName(), BigtableSink.write(input, sinkConfig, wait));
                    break;
                case datastore:
                    outputs.put(sinkConfig.getName(), DatastoreSink.write(input, sinkConfig, wait));
                    break;
                case jdbc:
                    outputs.put(sinkConfig.getName(), JdbcSink.write(input, sinkConfig, wait));
                    break;
                case solrindex:
                    outputs.put(sinkConfig.getName(), SolrIndexSink.write(input, sinkConfig, wait));
                    break;
                case pubsub:
                default:
                    throw new UnsupportedOperationException("Module: " + sinkConfig.getModule() + " is not supported !");
            }
            executedModuleNames.add(sinkConfig.getName());
        }

        if(notDoneModules.size() == 0) {
            return;
        }
        if(notDoneModules.size() == sinkConfigs.size()) {
            return;
        }
        setSinkResult(notDoneModules, outputs, executedModuleNames);
    }

    private static boolean containsMicrobatch(final List<SourceConfig> sources) {
        return sources.stream()
                .map(SourceConfig::getMicrobatch)
                .filter(Objects::nonNull)
                .anyMatch(s -> s);
    }

    public static void mergeSpannerOutput(final List<SinkConfig> sinks) {

        //INTERLEAVE
        final List<KV<String, String>> interleaves = sinks.stream()
                .filter(sink -> SinkConfig.Module.spanner.equals(sink.getModule()))
                .filter(sink -> sink.getParameters() != null)
                .filter(sink -> sink.getParameters().has("interleavedIn"))
                .filter(sink -> sink.getParameters().get("interleavedIn").isJsonPrimitive())
                .filter(sink -> sink.getParameters().getAsJsonPrimitive("interleavedIn").isString())
                .map(sink -> KV.of(sink.getName(), sink.getParameters().get("interleavedIn").getAsString()))
                .collect(Collectors.toList());

        int change = 1;
        while (change > 0) {
            final List<KV<String, String>> next = interleaves.stream()
                    .flatMap(p -> interleaves.stream()
                            .filter(pp -> pp.getKey().equals(p.getValue()))
                            .map(pp -> KV.of(p.getKey(), pp.getValue())))
                    .distinct()
                    .filter(p -> !interleaves.contains(p))
                    .collect(Collectors.toList());
            interleaves.addAll(next);
            change = next.size();
        }

        final Map<String, Set<KV<String, String>>> parentsMap = interleaves.stream()
                .collect(Collectors.groupingBy(KV::getKey, Collectors.toSet()));

        final Set<String> parentsNames = parentsMap
                .values()
                .stream()
                .flatMap(s -> s.stream().map(KV::getValue))
                .collect(Collectors.toSet());

        for(SinkConfig sink : sinks) {
            if(parentsNames.contains(sink.getName())) {
                continue;
            }
        }

    }

}
