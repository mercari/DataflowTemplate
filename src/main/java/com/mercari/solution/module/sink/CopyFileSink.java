package com.mercari.solution.module.sink;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.aws.S3Util;
import com.mercari.solution.util.converter.EntityToMapConverter;
import com.mercari.solution.util.converter.RecordToMapConverter;
import com.mercari.solution.util.converter.RowToMapConverter;
import com.mercari.solution.util.converter.StructToMapConverter;
import com.mercari.solution.util.gcp.DriveUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.*;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.util.*;


public class CopyFileSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFileSink.class);

    private static class CopyFileSinkParameters implements Serializable {

        private StorageService sourceService;
        private StorageService destinationService;

        private String source;
        private String destination;
        private Map<String, String> attributes;

        private S3Parameters s3;
        private DriveParameters drive;

        private Boolean failFast;

        public StorageService getSourceService() {
            return sourceService;
        }

        public StorageService getDestinationService() {
            return destinationService;
        }

        public String getSource() {
            return source;
        }

        public String getDestination() {
            return destination;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public S3Parameters getS3() {
            return s3;
        }

        public DriveParameters getDrive() {
            return drive;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(sourceService == null) {
                errorMessages.add("sourceService parameter is required for CopyFileSink config.");
            }
            if(destinationService == null) {
                errorMessages.add("destinationService parameter is required for CopyFileSink config.");
            }
            if(destination == null) {
                errorMessages.add("destination parameter is required for CopyFileSink config.");
            }

            if(StorageService.s3.equals(sourceService) || StorageService.s3.equals(destinationService)) {
                if(s3 == null) {
                    errorMessages.add("s3 parameter is required for CopyFileSink config when using s3.");
                } else {
                    if(s3.getAccessKey() == null) {
                        errorMessages.add("s3.accessKey parameter is required for CopyFileSink config when using s3.");
                    }
                    if(s3.getSecretKey() == null) {
                        errorMessages.add("s3.secretKey parameter is required for CopyFileSink config when using s3.");
                    }
                    if(s3.getRegion() == null) {
                        errorMessages.add("s3.region parameter is required for CopyFileSink config when using s3.");
                    }
                }
            }

            if(StorageService.drive.equals(sourceService) || StorageService.drive.equals(destinationService)) {
                if(drive == null) {
                    errorMessages.add("drive parameter is required for CopyFileSink config when using drive.");
                } else {
                    if(drive.getUser() == null) {
                        errorMessages.add("drive.user parameter is required for CopyFileSink config when using drive.");
                    }
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        private void setDefaults() {
            if(attributes == null) {
                attributes = new HashMap<>();
            }
            if(failFast == null) {
                failFast = true;
            }
        }
    }

    private static class DriveParameters implements Serializable {

        private String user;

        public String getUser() {
            return user;
        }

    }

    private static class S3Parameters implements Serializable {

        private String accessKey;
        private String secretKey;
        private String region;

        public String getAccessKey() {
            return accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public String getRegion() {
            return region;
        }

    }

    private enum StorageService implements Serializable {
        s3,
        gcs,
        drive,
        field
    }

    public String getName() {
        return "copyfile";
    }


    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("copyFile sink module requires input parameter");
        }
        final FCollection<?> input = inputs.get(0);

        final CopyFileSinkParameters parameters = new Gson().fromJson(config.getParameters(), CopyFileSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("CopyFileSink config parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final Map<String, FCollection<?>> results = new HashMap<>();
        final Coder coder = input.getCollection().getCoder();
        switch (input.getDataType()) {
            case ROW: {
                final FCollection<Row> inputCollection = (FCollection<Row>) input;
                final Transform<Row> transform = new Transform<>(
                        parameters,
                        RowSchemaUtil::getBytes,
                        RowToMapConverter::convert);
                PCollectionTuple output = inputCollection.getCollection().apply(config.getName(), transform);
                results.put(config.getName(), FCollection.of(config.getName(), output.get("output").setCoder(coder), DataType.ROW, input.getSchema()));
                results.put(config.getName() + ".failures", FCollection.of(config.getName(), output.get("failures").setCoder(coder), DataType.ROW, input.getSchema()));
                return results;
            }
            case AVRO: {
                final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                final Transform<GenericRecord> transform = new Transform<>(
                        parameters,
                        AvroSchemaUtil::getBytes,
                        RecordToMapConverter::convert);
                PCollectionTuple output = inputCollection.getCollection().apply(config.getName(), transform);
                results.put(config.getName(), FCollection.of(config.getName(), output.get("output").setCoder(coder), DataType.AVRO, input.getAvroSchema()));
                results.put(config.getName() + ".failures", FCollection.of(config.getName(), output.get("failures").setCoder(coder), DataType.AVRO, input.getAvroSchema()));
                return results;
            }
            case STRUCT: {
                final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                final Transform<Struct> transform = new Transform<>(
                        parameters,
                        StructSchemaUtil::getBytes,
                        StructToMapConverter::convert);
                PCollectionTuple output = inputCollection.getCollection().apply(config.getName(), transform);
                results.put(config.getName(), FCollection.of(config.getName(), output.get("output").setCoder(coder), DataType.STRUCT, input.getSpannerType()));
                results.put(config.getName() + ".failures", FCollection.of(config.getName(), output.get("failures").setCoder(coder), DataType.STRUCT, input.getSpannerType()));
                return results;
            }
            case ENTITY: {
                final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                final Transform<Entity> transform = new Transform<>(
                        parameters,
                        EntitySchemaUtil::getBytes,
                        EntityToMapConverter::convert);
                PCollectionTuple output = inputCollection.getCollection().apply(config.getName(), transform);
                results.put(config.getName(), FCollection.of(config.getName(), output.get("output").setCoder(coder), DataType.ENTITY, input.getSchema()));
                results.put(config.getName() + ".failures", FCollection.of(config.getName(), output.get("failures").setCoder(coder), DataType.ENTITY, input.getSchema()));
                return results;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    public static class Transform<T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final TupleTag<T> outputTag;
        private final TupleTag<T> failureTag;

        private final CopyFileSinkParameters parameters;

        private final SchemaUtil.BytesGetter<T> bytesGetter;
        private final SchemaUtil.MapConverter<T> mapConverter;

        private Transform(final CopyFileSinkParameters parameters,
                          final SchemaUtil.BytesGetter<T> bytesGetter,
                          final SchemaUtil.MapConverter<T> mapConverter) {

            this.parameters = parameters;
            this.bytesGetter = bytesGetter;
            this.mapConverter = mapConverter;

            this.outputTag = new TupleTag<>("output"){};
            this.failureTag = new TupleTag<>("failures"){};
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {
            return input
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("CopyFile", ParDo
                            .of(new CopyDoFn<>(parameters, bytesGetter, mapConverter, failureTag))
                            .withOutputTags(outputTag, TupleTagList.of(failureTag)));
        }


        private static class CopyDoFn<T> extends DoFn<T, T> {

            private final TupleTag<T> failureTag;

            private final StorageService sourceService;
            private final StorageService destinationService;

            private final String source;
            private final String destination;
            private final Map<String, String> attributes;

            private final SchemaUtil.BytesGetter<T> bytesGetter;
            private final SchemaUtil.MapConverter<T> mapConverter;

            private final DriveParameters driveParameters;
            private final S3Parameters s3Parameters;

            private final Boolean failFast;

            private transient Template templateSource;
            private transient Template templateDestination;
            private transient Map<String,Template> templateAttributes;

            private transient S3Client s3;
            private transient Storage storage;
            private transient Drive drive;


            CopyDoFn(final CopyFileSinkParameters parameters,
                     final SchemaUtil.BytesGetter<T> bytesGetter,
                     final SchemaUtil.MapConverter<T> mapConverter,
                     final TupleTag<T> failureTag) {

                this.failureTag = failureTag;

                this.sourceService = parameters.getSourceService();
                this.destinationService = parameters.getDestinationService();

                this.source = parameters.getSource();
                this.destination = parameters.getDestination();
                this.attributes = parameters.getAttributes();

                this.bytesGetter = bytesGetter;
                this.mapConverter = mapConverter;

                this.driveParameters = parameters.getDrive();
                this.s3Parameters = parameters.getS3();

                this.failFast = parameters.getFailFast();
            }

            @Setup
            public void setup() {
                // Setup template engine
                this.templateSource = TemplateUtil.createStrictTemplate("source", source);
                this.templateDestination = TemplateUtil.createStrictTemplate("destination", destination);
                this.templateAttributes = new HashMap<>();
                for(final Map.Entry<String, String> entry : attributes.entrySet()) {
                    this.templateAttributes.put(entry.getKey(), TemplateUtil.createStrictTemplate(entry.getKey(), entry.getValue()));
                }

                // Setup storage service client
                if(StorageService.gcs.equals(sourceService) || StorageService.gcs.equals(destinationService)) {
                    this.storage = StorageUtil.storage();
                }
                if(StorageService.s3.equals(sourceService) || StorageService.s3.equals(destinationService)) {
                    this.s3 = S3Util.storage(this.s3Parameters.getAccessKey(), this.s3Parameters.getSecretKey(), this.s3Parameters.getRegion());
                }
                if(StorageService.drive.equals(destinationService)) {
                    this.drive = DriveUtil.drive(driveParameters.getUser(), DriveScopes.DRIVE_FILE, DriveScopes.DRIVE_READONLY);
                } else if(StorageService.drive.equals(sourceService)) {
                    this.drive = DriveUtil.drive(driveParameters.getUser(), DriveScopes.DRIVE_READONLY);
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws Exception {
                final T element = c.element();
                final Map<String, Object> map = mapConverter.convert(element);
                final String destinationPath = TemplateUtil.executeStrictTemplate(this.templateDestination, map);

                try {
                    if (StorageService.field.equals(sourceService)) {
                        LOG.info("Copy field value to " + destinationService + ": " + destinationPath);
                        final byte[] bytes = this.bytesGetter.getAsBytes(element, source);
                        switch (destinationService) {
                            case s3 -> writeS3(destinationPath, bytes, map);
                            case gcs -> writeGcs(destinationPath, bytes, map);
                            case drive -> writeDrive(destinationPath, bytes, map);
                        }
                    } else {
                        final String sourcePath = TemplateUtil.executeStrictTemplate(this.templateSource, map);
                        LOG.info("Copy file from " + sourceService + ": " + sourcePath + " to " + destinationService + ": " + destinationPath);
                        switch (sourceService) {
                            case s3 -> {
                                switch (destinationService) {
                                    case s3 -> {
                                        final Map<String, Object> attributes = new HashMap<>();
                                        for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                            final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), map);
                                            attributes.put(entry.getKey(), value);
                                        }
                                        S3Util.copy(s3, sourcePath, destinationPath, attributes);
                                    }
                                    case gcs -> {
                                        final byte[] bytes = S3Util.readBytes(s3, sourcePath);
                                        writeGcs(destinationPath, bytes, map);
                                    }
                                    case drive -> {
                                        final byte[] bytes = S3Util.readBytes(s3, sourcePath);
                                        writeDrive(destinationPath, bytes, map);
                                    }
                                }
                            }
                            case gcs -> {
                                switch (destinationService) {
                                    case s3 -> {
                                        final byte[] bytes = StorageUtil.readBytes(storage, sourcePath);
                                        writeS3(destinationPath, bytes, map);
                                    }
                                    case gcs -> {
                                        final Map<String, Object> attributes = new HashMap<>();
                                        for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                            final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), map);
                                            attributes.put(entry.getKey(), value);
                                        }
                                        StorageUtil.copy(storage, sourcePath, destinationPath, attributes);
                                    }
                                    case drive -> {
                                        final byte[] bytes = StorageUtil.readBytes(storage, sourcePath);
                                        writeDrive(destinationPath, bytes, map);
                                    }
                                }
                            }
                            case drive -> {
                                switch (destinationService) {
                                    case s3 -> {
                                        final byte[] bytes = DriveUtil.download(drive, sourcePath);
                                        writeS3(destinationPath, bytes, map);
                                    }
                                    case gcs -> {
                                        final byte[] bytes = DriveUtil.download(drive, sourcePath);
                                        writeGcs(destinationPath, bytes, map);
                                    }
                                    case drive -> {
                                        final Map<String, Object> attributes = new HashMap<>();
                                        for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                                            final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), map);
                                            attributes.put(entry.getKey(), value);
                                        }
                                        DriveUtil.copy(drive, sourcePath, destinationPath, attributes);
                                    }
                                }
                            }
                        }
                    }

                    c.output(c.element());

                } catch (final Exception e) {
                    final String message = "Failed to copy file to " + destinationService + ": " + destinationPath + ", cause: " + e.getMessage();
                    LOG.error(message);
                    if(failFast) {
                        throw new IllegalStateException(message, e);
                    }
                    c.output(failureTag, c.element());
                }

            }

            private void writeGcs(final String gcsDestinationPath, final byte[] bytes, final Map<String, Object> record) {
                final StorageObject object = new StorageObject();
                final String[] gcsPaths = StorageUtil.parseGcsPath(gcsDestinationPath);
                object.setBucket(gcsPaths[0]);
                object.setName(gcsPaths[1]);
                for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                    final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), record);
                    object.set(entry.getKey(), value);
                }
                if(object.getContentType() == null) {
                    object.setContentType("application/octet-stream");
                }

                StorageUtil.writeObject(storage, object, bytes);
            }

            private void writeS3(final String s3DestinationPath, final byte[] bytes, final Map<String, Object> record) {
                final Map<String, Object> attributes = new HashMap<>();
                for(final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                    final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), record);
                    attributes.put(entry.getKey(), value);
                }
                final String contentType;
                if(templateAttributes.containsKey("contentType")) {
                    contentType = TemplateUtil.executeStrictTemplate(templateAttributes.get("contentType"), record);
                } else {
                    contentType = "application/octet-stream";
                }
                S3Util.writeBytes(s3, s3DestinationPath, bytes, contentType, attributes, new HashMap<>());
            }

            private void writeDrive(final String parent, final byte[] bytes, final Map<String, Object> record) {
                final File file = new File();
                file.setParents(Arrays.asList(parent));
                for (final Map.Entry<String, Template> entry : templateAttributes.entrySet()) {
                    final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), record);
                    file.set(entry.getKey(), value);
                }
                if(file.getMimeType() == null) {
                    file.setMimeType("application/octet-stream");
                }
                DriveUtil.createFile(drive, file, bytes);
            }

        }

    }

}