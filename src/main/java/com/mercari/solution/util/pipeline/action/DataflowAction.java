package com.mercari.solution.util.pipeline.action;

import com.google.api.services.dataflow.model.FlexTemplateRuntimeEnvironment;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.mercari.solution.util.gcp.DataflowUtil;
import com.mercari.solution.util.pipeline.union.UnionValue;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;

public class DataflowAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(DataflowAction.class);

    public static class Parameters implements Serializable {

        private Op op;

        private String project;
        private String region;
        private String jobName;
        private String runner;
        private String templateLocation;
        private String tempLocation;
        private String stagingLocation;
        private String serviceAccount;
        private String subnetwork;
        private String workerMachineType;
        private Integer maxNumWorkers;
        private Map<String, String> parameters;
        private Map<String, String> launchOptions;
        private Boolean update;
        private Boolean validateOnly;


        public Op getOp() {
            return op;
        }

        public String getProject() {
            return project;
        }

        public String getRegion() {
            return region;
        }

        public String getJobName() {
            return jobName;
        }

        public String getRunner() {
            return runner;
        }

        public String getTemplateLocation() {
            return templateLocation;
        }

        public String getTempLocation() {
            return tempLocation;
        }

        public String getStagingLocation() {
            return stagingLocation;
        }

        public String getServiceAccount() {
            return serviceAccount;
        }

        public String getSubnetwork() {
            return subnetwork;
        }

        public String getWorkerMachineType() {
            return workerMachineType;
        }

        public Integer getMaxNumWorkers() {
            return maxNumWorkers;
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public Map<String, String> getLaunchOptions() {
            return launchOptions;
        }


        public Boolean getUpdate() {
            return update;
        }

        public Boolean getValidateOnly() {
            return validateOnly;
        }

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.op == null) {
                errorMessages.add("action[" + name + "].dataflow.op must not be null");
            } else {
                switch (this.op) {
                    case launchTemplate, launchFlexTemplate -> {

                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults(final DataflowPipelineOptions options, final String config) {
            if(this.project == null) {
                this.project = options.getProject();
            }
            if(this.region == null) {
                this.region = options.getRegion();
            }
            if(this.templateLocation == null) {
                this.templateLocation = options.getTemplateLocation();
            }
            if(this.tempLocation == null) {
                this.tempLocation = options.getTempLocation();
            }
            if(this.stagingLocation == null) {
                this.stagingLocation = options.getStagingLocation();
            }
            if(this.workerMachineType == null) {
                this.workerMachineType = options.getWorkerMachineType();
            }
            if(this.maxNumWorkers == null) {
                this.maxNumWorkers = options.getMaxNumWorkers();
            }
            if(this.subnetwork == null) {
                this.subnetwork = options.getSubnetwork();
            }
            if(this.serviceAccount == null) {
                this.serviceAccount = options.getServiceAccount();
            }
            if(this.parameters == null && config != null) {
                this.parameters = new HashMap<>();
                this.parameters.put("config", config);
            }
            if(this.validateOnly == null) {
                this.validateOnly = false;
            }
        }

    }

    enum Op {
        launchTemplate,
        launchFlexTemplate;
    }

    private final Parameters parameters;
    private final Map<String, String> labels;


    public static DataflowAction of(final Parameters parameters, final Map<String, String> labels) {
        return new DataflowAction(parameters, labels);
    }

    public DataflowAction(final Parameters parameters, final Map<String, String> labels) {
        this.parameters = parameters;
        this.labels = labels;
    }

    @Override
    public Schema getOutputSchema() {
        return switch (this.parameters.getOp()) {
            case launchTemplate -> throw new RuntimeException();
            case launchFlexTemplate -> Schema.builder()
                    .addField(Schema.Field.of("jobId", Schema.FieldType.STRING.withNullable(true)))
                    .build();
        };
    }

    @Override
    public void setup() {

    }

    @Override
    public UnionValue action() {
        return action(parameters, labels);
    }


    @Override
    public UnionValue action(final UnionValue unionValue) {
        return action(parameters, labels);
    }

    private static UnionValue action(final Parameters parameters, final Map<String, String> labels) {
        try {
            switch (parameters.getOp()) {
                case launchTemplate -> dataflowLaunchTemplate(parameters);
                case launchFlexTemplate -> dataflowLaunchFlexTemplate(parameters, labels);
                default -> throw new IllegalArgumentException();
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dataflowLaunchTemplate(final Parameters parameters) throws IOException {
        throw new NotImplementedException("dataflowLaunchTemplate");
    }

    private static void dataflowLaunchFlexTemplate(final Parameters dataflowParameters, final Map<String, String> labels) throws IOException {

        final LaunchFlexTemplateParameter parameter = new LaunchFlexTemplateParameter();
        parameter.setContainerSpecGcsPath(dataflowParameters.getTemplateLocation());
        parameter.setParameters(dataflowParameters.getParameters());

        final String jobName = Optional
                .ofNullable(dataflowParameters.getJobName())
                .orElseGet(() -> "dataflow-flex-template-job-" + Instant.now().toEpochMilli());
        parameter.setJobName(jobName);

        if(dataflowParameters.getUpdate() != null) {
            parameter.setUpdate(dataflowParameters.getUpdate());
        }
        if(dataflowParameters.getLaunchOptions() != null) {
            parameter.setLaunchOptions(dataflowParameters.getLaunchOptions());
        }

        final FlexTemplateRuntimeEnvironment environment = new FlexTemplateRuntimeEnvironment();
        if(dataflowParameters.getTempLocation() != null) {
            environment.setTempLocation(dataflowParameters.getTempLocation());
        }
        if(dataflowParameters.getStagingLocation() != null) {
            environment.setStagingLocation(dataflowParameters.getStagingLocation());
        }
        if(dataflowParameters.getSubnetwork() != null) {
            environment.setSubnetwork(dataflowParameters.getSubnetwork());
        }
        if(dataflowParameters.getServiceAccount() != null) {
            environment.setServiceAccountEmail(dataflowParameters.getServiceAccount());
        }
        if(dataflowParameters.getWorkerMachineType() != null) {
            environment.setMachineType(dataflowParameters.getWorkerMachineType());
        }
        if(dataflowParameters.getMaxNumWorkers() != null) {
            environment.setMaxWorkers(dataflowParameters.getMaxNumWorkers());
        }
        if(labels != null) {
            environment.setAdditionalUserLabels(labels);
        }

        if(!environment.isEmpty()) {
            parameter.setEnvironment(environment);
        }

        final LaunchFlexTemplateResponse response = DataflowUtil.launchFlexTemplate(
                dataflowParameters.getProject(),
                dataflowParameters.getRegion(),
                parameter,
                dataflowParameters.getValidateOnly());
        LOG.info("LaunchFlexTemplate: {}", response.toString());
    }

}
