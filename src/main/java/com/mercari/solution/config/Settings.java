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

        private String jobName;
        private String tempLocation;
        private String stagingLocation;
        private Map<String, String> labels;
        private DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType autoscalingAlgorithm;
        private DataflowPipelineOptions.FlexResourceSchedulingGoal flexRSGoal;
        private Integer numWorkers;
        private Integer maxNumWorkers;
        private String workerMachineType;
        private Integer diskSizeGb;
        private String workerDiskType;
        private String workerRegion;
        private String workerZone;
        private String serviceAccount;
        private String impersonateServiceAccount;
        private String network;
        private String subnetwork;
        private Boolean usePublicIps;
        private Integer numberOfWorkerHarnessThreads;
        private Integer workerCacheMb;
        private String createFromSnapshot;
        private String sdkContainerImage;
        private Boolean enableStreamingEngine;
        private List<String> dataflowServiceOptions;
        private List<String> experiments;


        public String getJobName() {
            return jobName;
        }

        public String getTempLocation() {
            return tempLocation;
        }

        public String getStagingLocation() {
            return stagingLocation;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        public DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType getAutoscalingAlgorithm() {
            return autoscalingAlgorithm;
        }

        public DataflowPipelineOptions.FlexResourceSchedulingGoal getFlexRSGoal() {
            return flexRSGoal;
        }

        public Integer getNumWorkers() {
            return numWorkers;
        }

        public Integer getMaxNumWorkers() {
            return maxNumWorkers;
        }

        public String getWorkerMachineType() {
            return workerMachineType;
        }

        public String getWorkerRegion() {
            return workerRegion;
        }

        public String getWorkerZone() {
            return workerZone;
        }

        public Integer getDiskSizeGb() {
            return diskSizeGb;
        }

        public String getWorkerDiskType() {
            return workerDiskType;
        }

        public String getServiceAccount() {
            return serviceAccount;
        }

        public String getImpersonateServiceAccount() {
            return impersonateServiceAccount;
        }

        public String getNetwork() {
            return network;
        }

        public String getSubnetwork() {
            return subnetwork;
        }

        public Boolean getUsePublicIps() {
            return usePublicIps;
        }

        public Integer getNumberOfWorkerHarnessThreads() {
            return numberOfWorkerHarnessThreads;
        }

        public Integer getWorkerCacheMb() {
            return workerCacheMb;
        }

        public String getCreateFromSnapshot() {
            return createFromSnapshot;
        }

        public String getSdkContainerImage() {
            return sdkContainerImage;
        }

        public Boolean getEnableStreamingEngine() {
            return enableStreamingEngine;
        }

        public List<String> getDataflowServiceOptions() {
            return dataflowServiceOptions;
        }

        public List<String> getExperiments() {
            return experiments;
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
