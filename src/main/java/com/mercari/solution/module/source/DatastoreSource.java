package com.mercari.solution.module.source;

import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.EntityToRowConverter;
import com.mercari.solution.util.gcp.DatastoreUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class DatastoreSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSource.class);

    private static class DatastoreSourceParameters {

        private String projectId;
        private String gql;
        private String kind;
        private String namespace;
        private Integer numQuerySplits;
        private Boolean withKey;
        private Boolean emulator;

        public String getProjectId() {
            return projectId;
        }

        public String getGql() {
            return gql;
        }

        public String getKind() {
            return kind;
        }

        public String getNamespace() {
            return namespace;
        }

        public Integer getNumQuerySplits() {
            return numQuerySplits;
        }

        public Boolean getWithKey() {
            return withKey;
        }

        public Boolean getEmulator() {
            return emulator;
        }


        private void validate() {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(gql == null) {
                errorMessages.add("Parameter must contain gql");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {
            if (withKey == null) {
                withKey = false;
            }
        }
    }

    public String getName() { return "datastore"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(config.getName(), DatastoreSource.batch(begin, config));
        }
    }

    public static FCollection<Entity> batch(final PBegin begin, final SourceConfig config) {

        final DatastoreSourceParameters parameters = new Gson().fromJson(config.getParameters(), DatastoreSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("datastore source module parameters must not empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final DatastoreBatchSource source = new DatastoreBatchSource(config, parameters);
        final PCollection<Entity> output = begin.apply(config.getName(), source);
        final Schema schema;
        if(config.getSchema() != null) {
            final Schema configSchema = SourceConfig.convertSchema(config.getSchema());
            if(parameters.getWithKey()) {
                schema = EntityToRowConverter.addKeyToSchema(configSchema);
            } else {
                schema = configSchema;
            }
        } else {
            if(parameters.getKind() == null) {
                throw new IllegalArgumentException("Datastore auto schema detection requires kind parameter!");
            }
            final Schema kindSchema = DatastoreUtil.getSchema(
                    begin.getPipeline().getOptions(),
                    parameters.getProjectId(),
                    parameters.getKind());
            if (parameters.getWithKey()) {
                schema = kindSchema;
            } else {
                if(kindSchema.getField("__key__") != null) {
                    schema = RowSchemaUtil.removeFields(kindSchema, Arrays.asList("__key__"));
                } else {
                    schema = kindSchema;
                }
            }
        }
        return FCollection.of(config.getName(), output, DataType.ENTITY, schema);
    }

    public static class DatastoreBatchSource extends PTransform<PBegin, PCollection<Entity>> {

        private final DatastoreSourceParameters parameters;
        private final String timestampAttribute;
        private final String timestampDefault;

        private DatastoreBatchSource(final SourceConfig config, final DatastoreSourceParameters parameters) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.parameters = parameters;
        }

        @Override
        public PCollection<Entity> expand(final PBegin begin) {

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
                return entities.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.ENTITY, timestampAttribute, timestampDefault));
            }
        }

    }

}
