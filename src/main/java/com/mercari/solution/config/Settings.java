package com.mercari.solution.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;

import java.util.List;
import java.util.Map;

public class Settings {

    private Boolean streaming;
    private DataflowSettings dataflow;
    private AWSSettings aws;
    private BeamSQLSettings beamsql;

    public Boolean getStreaming() {
        return streaming;
    }

    public void setStreaming(Boolean streaming) {
        this.streaming = streaming;
    }

    public DataflowSettings getDataflow() {
        return dataflow;
    }

    public void setDataflow(DataflowSettings dataflow) {
        this.dataflow = dataflow;
    }

    public AWSSettings getAws() {
        return aws;
    }

    public void setAws(AWSSettings aws) {
        this.aws = aws;
    }

    public BeamSQLSettings getBeamsql() {
        return beamsql;
    }

    public void setBeamsql(BeamSQLSettings beamsql) {
        this.beamsql = beamsql;
    }

    public class AWSSettings {

        private String accessKey;
        private String secretKey;
        private String region;

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

    }

    public class DataflowSettings {

        private DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType autoscalingAlgorithm;
        private Map<String, String> labels;
        private List<String> dataflowServiceOptions;
        private Integer numberOfWorkerHarnessThreads;
        private DataflowPipelineOptions.FlexResourceSchedulingGoal flexRSGoal;
        private String createFromSnapshot;
        private String sdkContainerImage;
        private List<String> experiments;

        public DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType getAutoscalingAlgorithm() {
            return autoscalingAlgorithm;
        }

        public void setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType autoscalingAlgorithm) {
            this.autoscalingAlgorithm = autoscalingAlgorithm;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        public void setLabels(Map<String, String> labels) {
            this.labels = labels;
        }

        public List<String> getDataflowServiceOptions() {
            return dataflowServiceOptions;
        }

        public void setDataflowServiceOptions(List<String> dataflowServiceOptions) {
            this.dataflowServiceOptions = dataflowServiceOptions;
        }

        public Integer getNumberOfWorkerHarnessThreads() {
            return numberOfWorkerHarnessThreads;
        }

        public void setNumberOfWorkerHarnessThreads(Integer numberOfWorkerHarnessThreads) {
            this.numberOfWorkerHarnessThreads = numberOfWorkerHarnessThreads;
        }

        public DataflowPipelineOptions.FlexResourceSchedulingGoal getFlexRSGoal() {
            return flexRSGoal;
        }

        public void setFlexRSGoal(DataflowPipelineOptions.FlexResourceSchedulingGoal flexRSGoal) {
            this.flexRSGoal = flexRSGoal;
        }

        public String getCreateFromSnapshot() {
            return createFromSnapshot;
        }

        public void setCreateFromSnapshot(String createFromSnapshot) {
            this.createFromSnapshot = createFromSnapshot;
        }

        public String getSdkContainerImage() {
            return sdkContainerImage;
        }

        public void setSdkContainerImage(String sdkContainerImage) {
            this.sdkContainerImage = sdkContainerImage;
        }

        public List<String> getExperiments() {
            return experiments;
        }

        public void setExperiments(List<String> experiments) {
            this.experiments = experiments;
        }
    }

    public class BeamSQLSettings {

        private String plannerName;
        private String zetaSqlDefaultTimezone;
        private Boolean verifyRowValues;

        public String getPlannerName() {
            return plannerName;
        }

        public void setPlannerName(String plannerName) {
            this.plannerName = plannerName;
        }

        public String getZetaSqlDefaultTimezone() {
            return zetaSqlDefaultTimezone;
        }

        public void setZetaSqlDefaultTimezone(String zetaSqlDefaultTimezone) {
            this.zetaSqlDefaultTimezone = zetaSqlDefaultTimezone;
        }

        public Boolean getVerifyRowValues() {
            return verifyRowValues;
        }

        public void setVerifyRowValues(Boolean verifyRowValues) {
            this.verifyRowValues = verifyRowValues;
        }
    }

}
