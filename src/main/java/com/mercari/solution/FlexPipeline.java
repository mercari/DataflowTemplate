package com.mercari.solution;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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

        @Description("Dataflow tempLocation")
        String getTempLocation();
        void setTempLocation(String tempLocation);

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
        flexOptions.setAppName("Mercari Dataflow Template");
        if(config.getName() != null) {
            flexOptions.setJobName(config.getName().trim().replaceAll(" ", ""));
        }

        if(flexOptions.isStreaming()) {
            LOG.info("streaming");
            flexOptions.setEnableStreamingEngine(true);
        } else {
            LOG.info("batch");
            final List<String> experiments = Optional.ofNullable(flexOptions.getExperiments()).orElse(new ArrayList<>());
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
        //options.setPlannerName("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner");

        if(config.getSettings() != null) {
            final Settings settings = config.getSettings();

            if(settings.getStreaming() != null) {
                options.setStreaming(settings.getStreaming());
            }

            if(settings.getDataflow() != null) {
                final Settings.DataflowSettings dataflow = settings.getDataflow();
                if(dataflow.getAutoscalingAlgorithm() != null) {
                    options.as(DataflowPipelineOptions.class).setAutoscalingAlgorithm(dataflow.getAutoscalingAlgorithm());
                }
                if(dataflow.getLabels() != null && dataflow.getLabels().size() > 0) {
                    options.as(DataflowPipelineOptions.class).setLabels(dataflow.getLabels());
                }
                if(dataflow.getDataflowServiceOptions() != null && dataflow.getDataflowServiceOptions().size() > 0) {
                    options.as(DataflowPipelineOptions.class).setDataflowServiceOptions(dataflow.getDataflowServiceOptions());
                }
                if(dataflow.getNumberOfWorkerHarnessThreads() != null && dataflow.getNumberOfWorkerHarnessThreads() > 0) {
                    options.as(DataflowPipelineOptions.class).setNumberOfWorkerHarnessThreads(dataflow.getNumberOfWorkerHarnessThreads());
                }
                if(dataflow.getFlexRSGoal() != null) {
                    options.as(DataflowPipelineOptions.class).setFlexRSGoal(dataflow.getFlexRSGoal());
                }
                if(dataflow.getCreateFromSnapshot() != null) {
                    options.as(DataflowPipelineOptions.class).setCreateFromSnapshot(dataflow.getCreateFromSnapshot());
                }
                if(dataflow.getSdkContainerImage() != null) {
                    options.as(DataflowPipelineOptions.class).setSdkContainerImage(dataflow.getSdkContainerImage());
                }
                if(dataflow.getExperiments() != null && dataflow.getExperiments().size() > 0) {
                    options.as(DataflowPipelineOptions.class).setExperiments(dataflow.getExperiments());
                }
            }

            if(settings.getBeamsql() != null) {
                final Settings.BeamSQLSettings beamsql = settings.getBeamsql();
                if(beamsql.getPlannerName() != null) {
                    options.setPlannerName(beamsql.getPlannerName());
                }
                if(beamsql.getZetaSqlDefaultTimezone() != null) {
                    options.setZetaSqlDefaultTimezone(beamsql.getZetaSqlDefaultTimezone());
                }
                if(beamsql.getVerifyRowValues() != null) {
                    options.setVerifyRowValues(beamsql.getVerifyRowValues());
                }
            }

            if(settings.getAws() != null) {
                final Settings.AWSSettings aws = settings.getAws();
                if(aws.getAccessKey() != null && aws.getSecretKey() != null) {
                    final BasicAWSCredentials credentials = new BasicAWSCredentials(
                            aws.getAccessKey(),
                            aws.getSecretKey()
                    );
                    options.as(AwsOptions.class).setAwsCredentialsProvider(new AWSStaticCredentialsProvider(credentials));
                } else {
                    LOG.warn("settings.aws not contains accessKey and secretKey");
                }
                if(aws.getRegion() != null) {
                    options.as(AwsOptions.class).setAwsRegion(aws.getRegion());
                }
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
            jsonText = Files.readString(Paths.get(configParam), StandardCharsets.UTF_8);
        } else {
            jsonText = configParam;
        }
        return Config.parse(jsonText, args);
    }

    private static Set<String> moduleNames(final Config config) {
        final Set<String> moduleNames = new HashSet<>();
        moduleNames.addAll(config.getSources().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getSkip() == null || !c.getSkip())
                .map(SourceConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getTransforms().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getSkip() == null || !c.getSkip())
                .map(TransformConfig::getName)
                .collect(Collectors.toSet()));
        moduleNames.addAll(config.getSinks().stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getSkip() == null || !c.getSkip())
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
            // Skip null config(ketu comma)
            if(sourceConfig == null) {
                continue;
            }

            // Skip if parameter skip is true
            if(sourceConfig.getSkip() != null && sourceConfig.getSkip()) {
                continue;
            }

            // Skip already done module.
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

            if(!isStreaming && sourceConfig.getMicrobatch() != null && sourceConfig.getMicrobatch()) {
                throw new IllegalArgumentException("Config batch mode must be set batch mode for all inputs");
            }
            SourceModule module = ModuleRegistry.getInstance().getSource(sourceConfig.getModule());
            if (module != null) {
                outputs.putAll(module.expand(begin, sourceConfig, beats, wait));
            } else {
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

            // Skip if parameter skip is true
            if(transformConfig.getSkip() != null && transformConfig.getSkip()) {
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
            TransformModule module = ModuleRegistry.getInstance().getTransform(transformConfig.getModule());
            if (module != null) {
                outputs.putAll(module.expand(inputs, transformConfig));
            } else {
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

            // Skip if parameter skip is true
            if(sinkConfig.getSkip() != null && sinkConfig.getSkip()) {
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

            // Add waits
            final List<FCollection<?>> wait;
            if(sinkConfig.getWait() == null) {
                wait = null;
            } else {
                wait = sinkConfig.getWait().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            // Add sideInputs
            final List<FCollection<?>> sideInputs;
            if(sinkConfig.getSideInputs() == null) {
                sideInputs = new ArrayList<>();
            } else {
                sideInputs = sinkConfig.getSideInputs().stream()
                        .map(outputs::get)
                        .collect(Collectors.toList());
            }

            // Add queue if input not done.
            if(!outputs.keySet().contains(sinkConfig.getInput())) {
                notDoneModules.add(sinkConfig);
                continue;
            }
            final FCollection<?> input = outputs.get(sinkConfig.getInput());

            SinkModule module = ModuleRegistry.getInstance().getSink(sinkConfig.getModule());
            if (module != null) {
                outputs.putAll(module.expand(input, sinkConfig, wait, sideInputs));
            } else {
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

}
