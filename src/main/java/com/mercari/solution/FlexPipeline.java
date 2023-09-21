package com.mercari.solution;

import com.mercari.solution.config.*;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.io.GenerateSequence;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class FlexPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FlexPipeline.class);

    public interface FlexPipelineOptions extends BeamSqlPipelineOptions, GcpOptions, StreamingOptions {

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
        setDefaults(localOptions, args);

        if(isDirectRunner(args)) {
            LOG.info("DirectRunner mode.");
            final Pipeline pipeline = Pipeline.create(localOptions);
            run(pipeline, config);
        } else {
            LOG.info("DataflowRunner mode.");
            final FlexDataflowPipelineOptions flexOptions = localOptions.as(FlexDataflowPipelineOptions.class);
            flexOptions.setAppName("Mercari Dataflow Template");

            final Pipeline pipeline = Pipeline.create(flexOptions);
            run(pipeline, config);
        }

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
                if(dataflow.getJobName() != null) {
                    options.as(DataflowPipelineOptions.class).setJobName(dataflow.getJobName());
                }
                if(dataflow.getTempLocation() != null) {
                    options.as(DataflowPipelineOptions.class).setTempLocation(dataflow.getTempLocation());
                }
                if(dataflow.getStagingLocation() != null) {
                    options.as(DataflowPipelineOptions.class).setStagingLocation(dataflow.getStagingLocation());
                }
                if(dataflow.getLabels() != null && dataflow.getLabels().size() > 0) {
                    options.as(DataflowPipelineOptions.class).setLabels(dataflow.getLabels());
                }
                if(dataflow.getAutoscalingAlgorithm() != null) {
                    options.as(DataflowPipelineOptions.class).setAutoscalingAlgorithm(dataflow.getAutoscalingAlgorithm());
                }
                if(dataflow.getFlexRSGoal() != null) {
                    options.as(DataflowPipelineOptions.class).setFlexRSGoal(dataflow.getFlexRSGoal());
                }
                if(dataflow.getNumWorkers() != null && dataflow.getNumWorkers() > 0) {
                    options.as(DataflowPipelineOptions.class).setNumWorkers(dataflow.getNumWorkers());
                }
                if(dataflow.getMaxNumWorkers() != null && dataflow.getMaxNumWorkers() > 0) {
                    options.as(DataflowPipelineOptions.class).setMaxNumWorkers(dataflow.getMaxNumWorkers());
                }
                if(dataflow.getNumberOfWorkerHarnessThreads() != null && dataflow.getNumberOfWorkerHarnessThreads() > 0) {
                    options.as(DataflowPipelineOptions.class).setNumberOfWorkerHarnessThreads(dataflow.getNumberOfWorkerHarnessThreads());
                }
                if(dataflow.getWorkerMachineType() != null) {
                    options.as(DataflowPipelineOptions.class).setWorkerMachineType(dataflow.getWorkerMachineType());
                }
                if(dataflow.getWorkerRegion() != null) {
                    options.as(DataflowPipelineOptions.class).setWorkerRegion(dataflow.getWorkerRegion());
                }
                if(dataflow.getWorkerZone() != null) {
                    options.as(DataflowPipelineOptions.class).setWorkerZone(dataflow.getWorkerZone());
                }
                if(dataflow.getDiskSizeGb() != null) {
                    options.as(DataflowPipelineOptions.class).setDiskSizeGb(dataflow.getDiskSizeGb());
                }
                if(dataflow.getWorkerDiskType() != null) {
                    options.as(DataflowPipelineOptions.class).setWorkerDiskType(dataflow.getWorkerDiskType());
                }
                if(dataflow.getServiceAccount() != null) {
                    options.as(DataflowPipelineOptions.class).setServiceAccount(dataflow.getServiceAccount());
                }
                if(dataflow.getImpersonateServiceAccount() != null) {
                    options.as(DataflowPipelineOptions.class).setImpersonateServiceAccount(dataflow.getImpersonateServiceAccount());
                }
                if(dataflow.getNetwork() != null) {
                    options.as(DataflowPipelineOptions.class).setNetwork(dataflow.getNetwork());
                }
                if(dataflow.getSubnetwork() != null) {
                    options.as(DataflowPipelineOptions.class).setSubnetwork(dataflow.getSubnetwork());
                }
                if(dataflow.getUsePublicIps() != null) {
                    options.as(DataflowPipelineOptions.class).setUsePublicIps(dataflow.getUsePublicIps());
                }
                if(dataflow.getWorkerCacheMb() != null && dataflow.getWorkerCacheMb() > 0) {
                    options.as(DataflowPipelineOptions.class).setWorkerCacheMb(dataflow.getWorkerCacheMb());
                }
                if(dataflow.getCreateFromSnapshot() != null) {
                    options.as(DataflowPipelineOptions.class).setCreateFromSnapshot(dataflow.getCreateFromSnapshot());
                }
                if(dataflow.getSdkContainerImage() != null) {
                    options.as(DataflowPipelineOptions.class).setSdkContainerImage(dataflow.getSdkContainerImage());
                }
                if(options.isStreaming()) {
                    if(dataflow.getEnableStreamingEngine() != null) {
                        options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(dataflow.getEnableStreamingEngine());
                    } else {
                        options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
                    }
                }
                if(dataflow.getDataflowServiceOptions() != null && dataflow.getDataflowServiceOptions().size() > 0) {
                    final List<String> existingDataflowServiceOptions = Optional
                            .ofNullable(options.as(DataflowPipelineOptions.class).getDataflowServiceOptions())
                            .orElseGet(ArrayList::new);
                    existingDataflowServiceOptions.addAll(dataflow.getDataflowServiceOptions());
                    options.as(DataflowPipelineOptions.class).setDataflowServiceOptions(existingDataflowServiceOptions.stream().distinct().toList());
                }
                if(dataflow.getExperiments() != null && dataflow.getExperiments().size() > 0) {
                    final List<String> existingExperiments = Optional
                            .ofNullable(options.as(DataflowPipelineOptions.class).getExperiments())
                            .orElseGet(ArrayList::new);
                    existingExperiments.addAll(dataflow.getExperiments());
                    options.as(DataflowPipelineOptions.class).setExperiments(existingExperiments.stream().distinct().toList());
                }
            } else {
                if(options.isStreaming()) {
                    options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
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
        } else {
            if(options.isStreaming()) {
                options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
            }
        }
    }

    private static void setDefaults(FlexPipelineOptions options, String[] args) {
        if(options.isStreaming()) {
            if(hasEnableStreamingEngine(args) && !options.isEnableStreamingEngine()) {
                options.as(DataflowPipelineOptions.class).setEnableStreamingEngine(true);
            }
        }
    }

    private static boolean hasStreaming(final String[] args) {
        return hasOption(args, "--streaming");
    }

    private static boolean hasEnableStreamingEngine(final String[] args) {
        return hasOption(args, "--enableStreamingEngine");
    }

    private static boolean isDirectRunner(final String[] args) {
        return Arrays.stream(args)
                .filter(Objects::nonNull)
                .filter(s -> s.contains("="))
                .map(s -> s.split("="))
                .filter(s -> s.length > 1)
                .anyMatch(s -> s[0].equalsIgnoreCase("--runner") && s[1].equalsIgnoreCase("directrunner"));
    }

    private static boolean hasOption(final String[] args, final String optionName) {
        return Arrays.stream(args)
                .filter(Objects::nonNull)
                .map(s -> s.contains("=") ? s.split("=")[0] : s)
                .map(String::trim)
                .anyMatch(optionName::equalsIgnoreCase);
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
            LOG.info("config parameter is GCS path: " + configParam);
            jsonText = StorageUtil.readString(configParam);
        } else  {
            Path path;
            try {
                path = Paths.get(configParam);
            } catch (Throwable e) {
                path = null;
            }
            if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                LOG.info("config parameter is local file path: " + configParam);
                jsonText = Files.readString(path, StandardCharsets.UTF_8);
            } else {
                LOG.info("config parameter is json body: " + configParam);
                jsonText = configParam;
            }
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

            // Add queue if input not done.
            if(!outputs.keySet().containsAll(sinkConfig.getInputs())) {
                notDoneModules.add(sinkConfig);
                continue;
            }

            final List<FCollection<?>> inputs = sinkConfig.getInputs().stream()
                    .map(outputs::get)
                    .collect(Collectors.toList());
            final SinkModule module = ModuleRegistry.getInstance().getSink(sinkConfig.getModule());
            if (module != null) {
                outputs.putAll(module.expand(inputs, sinkConfig, wait));
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
