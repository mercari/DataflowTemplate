package com.mercari.solution.module.source;

import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.gcp.DatastoreUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DatastoreSource {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSource.class);

    private class DatastoreSourceParameters {

        private String projectId;
        private String gql;
        private String kind;
        private String namespace;
        private Integer numQuerySplits;
        private Boolean emulator;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getGql() {
            return gql;
        }

        public void setGql(String gql) {
            this.gql = gql;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public Integer getNumQuerySplits() {
            return numQuerySplits;
        }

        public void setNumQuerySplits(Integer numQuerySplits) {
            this.numQuerySplits = numQuerySplits;
        }

        public Boolean getEmulator() {
            return emulator;
        }

        public void setEmulator(Boolean emulator) {
            this.emulator = emulator;
        }
    }

    public static FCollection<Entity> batch(final PBegin begin, final SourceConfig config) {
        final DatastoreBatchSource source = new DatastoreBatchSource(config);
        final PCollection<Entity> output = begin.apply(config.getName(), source);
        final Schema schema;
        if(config.getSchema() != null) {
            schema = SourceConfig.convertSchema(config.getSchema());
        } else {
            final DatastoreSourceParameters parameters = new Gson().fromJson(config.getParameters(), DatastoreSourceParameters.class);
            if(parameters.getKind() == null) {
                throw new IllegalArgumentException("Datastore auto schema detection requires kind parameter!");
            }
            schema = DatastoreUtil.getSchema(
                    begin.getPipeline().getOptions(),
                    parameters.getProjectId(),
                    parameters.getKind());
        }
        return FCollection.of(config.getName(), output, DataType.ENTITY, schema);
    }


    public static class DatastoreBatchSource extends PTransform<PBegin, PCollection<Entity>> {

        private final DatastoreSourceParameters parameters;
        private final String timestampAttribute;
        private final String timestampDefault;

        public DatastoreSourceParameters getParameters() {
            return parameters;
        }

        private DatastoreBatchSource(final SourceConfig config) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.parameters = new Gson().fromJson(config.getParameters(), DatastoreSourceParameters.class);
        }

        @Override
        public PCollection<Entity> expand(final PBegin begin) {

            validateParameters(parameters);
            setDefaultParameters(parameters);

            final String execEnvProject = begin.getPipeline().getOptions().as(GcpOptions.class).getProject();

            DatastoreV1.Read read = DatastoreIO.v1().read()
                    .withProjectId(parameters.getProjectId() == null ? execEnvProject : parameters.getProjectId())
                    .withLiteralGqlQuery(parameters.getGql());

            if(parameters.getNamespace() != null) {
                read = read.withNamespace(parameters.getNamespace());
            }

            if(parameters.getNumQuerySplits() != null) {
                read = read.withNumQuerySplits(parameters.getNumQuerySplits());
            }

            final PCollection<Entity> entities = begin.apply("QueryToDatastore", read);

            if(timestampAttribute == null) {
                return entities;
            } else {
                final String timestampField = timestampAttribute;
                try {
                    final Instant timestampDefault = Instant.parse(this.timestampDefault);
                    return entities.apply("WithTimestamp", WithTimestamps.of(e -> DatastoreUtil.getTimestamp(e, timestampField, timestampDefault)));
                } catch (Exception exception) {
                    LOG.error("timestampDefault is illegal value: " + timestampDefault);
                    final Instant timestampDefault = Instant.ofEpochSecond(0L);
                    return entities.apply("WithTimestamp", WithTimestamps.of(e -> DatastoreUtil.getTimestamp(e, timestampField, timestampDefault)));
                }
            }
        }

        private void validateParameters(final DatastoreSourceParameters parameters) {
            if(parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getGql() == null) {
                errorMessages.add("Parameter must contain gql");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters(final DatastoreSourceParameters parameters) {

        }

    }

}
