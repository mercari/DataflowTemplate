package com.mercari.solution.module.sink;

import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.List;
import java.util.Optional;

public class FirestoreSink {

    private class FirestoreSinkParameters {

        private String projectId;
        private String kind;
        private List<String> keyFields;
        private Boolean delete;

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields;
        }

        public Boolean getDelete() {
            return delete;
        }

        public void setDelete(Boolean delete) {
            this.delete = delete;
        }
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waitCollections, final List<FCollection<?>> sideInputs) {
        final FirestoreSinkParameters parameters = new Gson().fromJson(config.getParameters(), FirestoreSinkParameters.class);
        final FirestoreWrite write = new FirestoreWrite(collection, parameters, waitCollections);
        final PDone output = collection.getCollection().apply(config.getName(), write);
        return FCollection.update(collection, config.getName(), (PCollection) collection.getCollection());
    }

    public static class FirestoreWrite extends PTransform<PCollection<?>, PDone> {

        private final FCollection<?> inputCollection;
        private final FirestoreSinkParameters parameters;
        private final List<FCollection<?>> waitCollections;

        public FCollection<?> getCollection() {
            return inputCollection;
        }

        private FirestoreWrite(final FCollection<?> inputCollection,
                               final FirestoreSinkParameters parameters,
                               final List<FCollection<?>> waitCollections) {

            this.inputCollection = inputCollection;
            this.parameters = parameters;
            this.waitCollections = waitCollections;
        }

        public PDone expand(final PCollection<?> input) {
            validateParameters();
            setDefaultParameters();

            final String execEnvProject = input.getPipeline().getOptions().as(GcpOptions.class).getProject();

            final PCollection<Entity> entities = input
                    .apply(DataTypeTransform.datastoreEntity(inputCollection, parameters.getKind(), parameters.getKeyFields()));

            if(parameters.getDelete()) {
                return entities.apply("DeleteEntity", DatastoreIO.v1().deleteEntity()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject)));
            } else {
                return entities.apply("WriteEntity", DatastoreIO.v1().write()
                        .withProjectId(Optional.ofNullable(parameters.getProjectId()).orElse(execEnvProject)));
            }
        }

        private void validateParameters() {
            if(this.parameters.getProjectId() == null) {
                throw new IllegalArgumentException("Datastore output module requires projectId parameter!");
            }
            if(this.parameters.getKind() == null) {
                throw new IllegalArgumentException("Datastore output module requires kind parameter!");
            }
        }

        private void setDefaultParameters() {
            if(parameters.getDelete() == null) {
                parameters.setDelete(false);
            }
        }

    }

}
