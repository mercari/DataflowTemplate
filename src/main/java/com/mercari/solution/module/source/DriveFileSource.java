package com.mercari.solution.module.source;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.gcp.DriveUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class DriveFileSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(DriveFileSource.class);

    private static final String DEFAULT_FIELDS = "files(id,driveId,name,size,description,version,originalFilename,kind,mimeType,fileExtension,parents,createdTime,modifiedTime),nextPageToken";

    private class DriveMetaSourceParameters implements Serializable {

        private String query;
        private String user;
        private String driveId;
        private String folderId;
        private Boolean recursive;
        private String fields;

        private OutputType outputType;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getDriveId() {
            return driveId;
        }

        public void setDriveId(String driveId) {
            this.driveId = driveId;
        }

        public String getFolderId() {
            return folderId;
        }

        public void setFolderId(String folderId) {
            this.folderId = folderId;
        }

        public Boolean getRecursive() {
            return recursive;
        }

        public void setRecursive(Boolean recursive) {
            this.recursive = recursive;
        }

        public String getFields() {
            return fields;
        }

        public void setFields(String fields) {
            this.fields = fields;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }
    }

    public String getName() { return "drivefile"; }

    private enum OutputType implements Serializable {
        row,
        avro
    }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(config.getName(), batch(begin, config));
        }
    }

    public FCollection batch(final PBegin begin, final SourceConfig config) {
        final DriveMetaSourceParameters parameters = new Gson().fromJson(config.getParameters(), DriveMetaSourceParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters, begin.getPipeline().getOptions());
        switch (parameters.getOutputType()) {
            case row: {
                final Schema outputSchema = DriveUtil.createFileSchema(parameters.getFields());
                final BatchSource<Row> source = (new BatchSource<>(parameters));
                final PCollection<Row> rows = begin
                        .apply(config.getName(), source)
                        .setCoder(RowCoder.of(outputSchema));
                return FCollection.of(config.getName(), rows, DataType.ROW, outputSchema);
            }
            case avro: {
                final org.apache.avro.Schema outputSchema = DriveUtil.createAvroFileSchema(parameters.getFields());
                final BatchSource<GenericRecord> source = new BatchSource<>(parameters);
                final PCollection<GenericRecord> rows = begin
                        .apply(config.getName(), source)
                        .setCoder(AvroCoder.of(outputSchema));
                return FCollection.of(config.getName(), rows, DataType.AVRO, outputSchema);
            }
            default:
                throw new IllegalArgumentException("DriveFile module not support format: " + parameters.getOutputType());
        }
    }

    private static void validateParameters(final DriveMetaSourceParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("DriveFile config parameters must not be empty!");
        }

        // check required parameters filled
        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getQuery() == null) {
            errorMessages.add("DriveFile source module requires query parameter.");
        }

        if(parameters.getFields() != null) {
            if(parameters.getFields().equals("*")) {
                errorMessages.add("DriveFile source module not support '*' for fields parameter.");
            } else if(!parameters.getFields().contains("files(")) {
                errorMessages.add("DriveFile source module fields parameter must be format such as 'files(id,kind,...)'.");
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
        }
    }

    private static void setDefaultParameters(
            final DriveMetaSourceParameters parameters, final PipelineOptions options) {

        if(parameters.getUser() == null) {
            final String serviceAccount = options.as(DataflowPipelineOptions.class).getServiceAccount();
            parameters.setUser(serviceAccount);
        }
        if(parameters.getOutputType() == null) {
            parameters.setOutputType(OutputType.row);
        }
        if(parameters.getRecursive() == null) {
            parameters.setRecursive(true);
        }
        if(parameters.getFields() == null) {
            parameters.setFields(DEFAULT_FIELDS);
        } else if(!parameters.getFields().contains("nextPageToken")) {
            parameters.setFields(parameters.getFields() + ",nextPageToken");
        }
    }

    public static class BatchSource<T> extends PTransform<PBegin, PCollection<T>> {

        private final DriveMetaSourceParameters parameters;

        private BatchSource(final DriveMetaSourceParameters parameters) {
            this.parameters = parameters;
        }

        public PCollection<T> expand(final PBegin begin) {

            final PCollection<?> output;

            switch (parameters.getOutputType()) {
                case row: {
                    final DriveFileReadDoFn<Row> dofn = new DriveFileReadRowDoFn(
                            parameters.getUser(), parameters.getDriveId(), parameters.getFolderId(), parameters.getFields(), parameters.getRecursive());
                    output = begin
                            .apply("InjectQuery", Create.of(parameters.getQuery()))
                            .apply("ReadDriveFiles", ParDo.of(dofn));
                    break;
                }
                case avro: {
                    final DriveFileReadDoFn<GenericRecord> dofn = new DriveFileReadRecordDoFn(
                            parameters.getUser(), parameters.getDriveId(), parameters.getFolderId(), parameters.getFields(), parameters.getRecursive());
                    output = begin
                            .apply("InjectQuery", Create.of(parameters.getQuery()))
                            .apply("ReadDriveFiles", ParDo.of(dofn));
                    break;
                }
                default: {
                    throw new IllegalStateException("Not supported outputType: " + parameters.getOutputType());
                }
            }
            return (PCollection<T>)output;
        }
    }

    private static abstract class DriveFileReadDoFn<T> extends DoFn<String, T> {

        private final String user;
        private final String driveId;
        private final String folderId;
        protected final String fields;
        private final boolean recursive;

        private transient Drive service;

        DriveFileReadDoFn(final String user, final String driveId, final String folderId, final String fields, final boolean recursive) {
            this.user = user;
            this.driveId = driveId;
            this.folderId = folderId;
            this.fields = fields;
            this.recursive = recursive;
        }

        public void setup() {
            this.service = DriveUtil.drive(user, DriveScopes.DRIVE_READONLY);
        }

        @ProcessElement
        public void processElement(final ProcessContext c,
                                   final DoFn.OutputReceiver<T> receiver) throws IOException {

            final String query = c.element();
            final List<String> path = new ArrayList<>();
            search(query, folderId, path, receiver);
        }

        private void search(final String query,
                            final String parentFolderId,
                            final List<String> path,
                            final DoFn.OutputReceiver<T> receiver) throws IOException {

            final String q;
            if(folderId == null) {
                q = query;
            } else {
                q = "'" + parentFolderId + "' in parents and (" + query + " or " + "mimeType='" + DriveUtil.MIMETYPE_APPS_FOLDER + "')";
            }

            Drive.Files.List list = this.service.files().list()
                    .setPageSize(1000)
                    .setQ(q)
                    .setFields(fields);

            if(driveId != null) {
                list = list.setDriveId(driveId);
            }

            FileList fileList = list.execute();
            for(final File file : fileList.getFiles()) {
                if(DriveUtil.isFolder(file)) {
                    if(recursive) {
                        final List<String> childPath = new ArrayList<>(path);
                        childPath.add(file.getName());
                        search(query, file.getId(), childPath, receiver);
                    }
                } else {
                    final T output = createOutput(file, path);
                    receiver.output(output);
                }
            }

            while(fileList.getNextPageToken() != null) {
                fileList = list.setPageToken(fileList.getNextPageToken()).execute();
                for(final File file : fileList.getFiles()) {
                    if(DriveUtil.isFolder(file)) {
                        if(recursive) {
                            final List<String> childPath = new ArrayList<>(path);
                            childPath.add(file.getName());
                            search(query, file.getId(), childPath, receiver);
                        }
                    } else {
                        final T output = createOutput(file, path);
                        receiver.output(output);
                    }
                }
            }

        }

        abstract T createOutput(final File file, final List<String> path);

    }

    private static class DriveFileReadRowDoFn extends DriveFileReadDoFn<Row> {

        private transient Schema schema;

        DriveFileReadRowDoFn(final String user, final String driveId, final String folderId, final String fields, final boolean recursive) {
            super(user, driveId, folderId, fields, recursive);
        }

        @Setup
        public void setup() {
            super.setup();
            this.schema = DriveUtil.createFileSchema(super.fields);
        }

        @Override
        Row createOutput(final File file, final List<String> path) {
            return DriveUtil.convertToRow(schema, file);
        }

    }

    private static class DriveFileReadRecordDoFn extends DriveFileReadDoFn<GenericRecord> {

        private transient org.apache.avro.Schema schema;

        DriveFileReadRecordDoFn(final String user, final String driveId, final String folderId, final String fields, final boolean recursive) {
            super(user, driveId, folderId, fields, recursive);
        }

        @Setup
        public void setup() {
            this.schema = DriveUtil.createAvroFileSchema(super.fields);
            super.setup();
        }

        @Override
        GenericRecord createOutput(final File file, final List<String> path) {
            return DriveUtil.convertToRecord(schema, file);
        }

    }

}
