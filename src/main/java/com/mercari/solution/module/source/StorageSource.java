package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.aws.S3Util;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.Serializable;
import java.util.*;


public class StorageSource implements SourceModule {

    private static class StorageSourceParameters implements Serializable {

        private String input;
        private Format format;
        private String compression;
        private String filterPrefix;
        private String targetFormat;

        // for AWS S3
        private String s3AccessKey;
        private String s3SecretKey;
        private String s3Region;

        public String getInput() {
            return input;
        }

        public Format getFormat() {
            return format;
        }

        public String getCompression() {
            return compression;
        }

        public String getFilterPrefix() {
            return filterPrefix;
        }

        public String getTargetFormat() {
            return targetFormat;
        }

        public String getS3AccessKey() {
            return s3AccessKey;
        }

        public String getS3SecretKey() {
            return s3SecretKey;
        }

        public String getS3Region() {
            return s3Region;
        }

        public void validate(String name) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.format == null) {
                errorMessages.add("Storage module: " + name + " requires parameter format");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        public void setDefaults() {

        }
    }

    public enum Format implements Serializable {
        avro,
        parquet,
        csv,
        json
    }

    public String getName() { return "storage"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            //inputs.put(config.getName(), beats.apply(config.getName(), StorageSource.microbatch(config)));
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(config.getName(), StorageSource.batch(begin, config));
        }
    }

    public static FCollection batch(final PBegin begin, final SourceConfig config) {
        final StorageSourceParameters parameters = new Gson().fromJson(config.getParameters(), StorageSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("Storage source module: " + config.getName() + " parameter must not be empty!");
        }
        parameters.validate(config.getName());
        parameters.setDefaults();

        switch (parameters.getFormat()) {
            case avro:
            case parquet: {
                final StorageAvroBatchSource sourceAvro = new StorageAvroBatchSource(config, parameters);
                final PCollection<GenericRecord> outputAvro = begin.apply(config.getName(), sourceAvro);
                return FCollection.of(config.getName(), outputAvro, DataType.AVRO, sourceAvro.schema);
            }
            case csv:
            case json: {
                if(parameters.getTargetFormat() != null && "row".equalsIgnoreCase(parameters.getTargetFormat().trim())) {
                    final StorageTextRowBatchSource sourceText = new StorageTextRowBatchSource(config);
                    final PCollection<Row> outputText = begin.apply(config.getName(), sourceText);
                    return FCollection.of(config.getName(), outputText, DataType.ROW, sourceText.schema);
                } else {
                    final StorageTextAvroBatchSource sourceText = new StorageTextAvroBatchSource(config);
                    final PCollection<GenericRecord> outputText = begin.apply(config.getName(), sourceText);
                    return FCollection.of(config.getName(), outputText, DataType.AVRO, sourceText.schema);
                }
            }
            default:
                throw new IllegalArgumentException("Storage module not support format: " + parameters.getFormat());
        }
    }

    public static StorageMicrobatchRead microbatch(final SourceConfig config) {
        return new StorageMicrobatchRead(config);
    }

    public static class StorageAvroBatchSource
            extends PTransform<PBegin, PCollection<GenericRecord>> {

        private org.apache.avro.Schema schema;

        private final StorageSourceParameters parameters;
        private final SourceConfig.InputSchema inputSchema;
        private final String timestampAttribute;
        private final String timestampDefault;

        public StorageSourceParameters getParameters() {
            return parameters;
        }

        private StorageAvroBatchSource(final SourceConfig config, final StorageSourceParameters parameters) {
            this.inputSchema = config.getSchema();
            this.parameters = parameters;
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
        }

        public PCollection<GenericRecord> expand(final PBegin begin) {

            final String input = parameters.getInput();
            final Format format = parameters.getFormat();

            final PCollection<GenericRecord> records;
            switch (format) {
                case avro -> {
                    this.schema = getAvroSchema(parameters.getInput(), inputSchema,
                            parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region());
                    records = begin
                            .apply("ReadAvro", AvroIO
                                    .readGenericRecords(this.schema)
                                    .from(input));
                }
                case parquet -> {
                    this.schema = getParquetSchema(parameters.getInput(), inputSchema,
                            parameters.getS3AccessKey(), parameters.getS3SecretKey(), parameters.getS3Region());
                    records = begin
                            .apply("ReadParquet", ParquetIO
                                    .read(this.schema)
                                    .from(input));
                }
                default -> throw new IllegalArgumentException("Storage module not support format: " + format);
            }

            if(timestampAttribute == null) {
                return records;
            } else {
                return records.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.AVRO, timestampAttribute, timestampDefault));
            }
        }
    }

    public static class StorageTextAvroBatchSource
            extends PTransform<PBegin, PCollection<GenericRecord>> {

        private org.apache.avro.Schema schema;

        private final StorageSourceParameters parameters;
        private final SourceConfig.InputSchema inputSchema;
        private final String timestampAttribute;
        private final String timestampDefault;

        public StorageSourceParameters getParameters() {
            return parameters;
        }

        private StorageTextAvroBatchSource(final SourceConfig config) {
            this.inputSchema = config.getSchema();
            this.parameters = new Gson().fromJson(config.getParameters(), StorageSourceParameters.class);
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
        }

        public PCollection<GenericRecord> expand(final PBegin begin) {

            final Format format = parameters.getFormat();

            final PCollection<GenericRecord> records;
            switch (format) {
                case csv:
                case json: {
                    if (this.inputSchema == null || (inputSchema.getAvroSchema() == null && inputSchema.getFields() == null)) {
                        this.schema = TextToRecordConverter.DEFAULT_SCHEMA;
                    } else if (Format.csv.equals(format)) {
                        this.schema = SourceConfig.convertAvroSchema(inputSchema);
                    } else {
                        this.schema = SourceConfig.convertAvroSchema(inputSchema);
                    }

                    TextIO.Read read = TextIO.read().from(parameters.getInput());
                    if (parameters.getCompression() != null) {
                        read = read.withCompression(Compression
                                .valueOf(parameters.getCompression().trim().toUpperCase()));
                    }

                    final PCollection<String> lines;
                    if (parameters.getFilterPrefix() != null) {
                        final String filterPrefix = parameters.getFilterPrefix();
                        lines = begin
                                .apply("ReadLine", read)
                                .apply("FilterPrefix", Filter.by(s -> !s.startsWith(filterPrefix)));
                    } else {
                        lines = begin.apply("ReadLine", read);
                    }

                    records = lines
                            .apply("ConvertToRecord",ParDo.of(new ToRecordDoFn(format, this.schema.toString())))
                            .setCoder(AvroCoder.of(schema));
                    break;
                }
                default:
                    throw new IllegalArgumentException("StorageSource only support format avro, csv, json. but: " + format);
            }

            if(timestampAttribute == null) {
                return records;
            } else {
                return records.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.AVRO, timestampAttribute, timestampDefault));
            }

        }
    }

    public static class StorageTextRowBatchSource
            extends PTransform<PBegin, PCollection<Row>> {

        private Schema schema;

        private final StorageSourceParameters parameters;
        private final SourceConfig.InputSchema inputSchema;
        private final String timestampAttribute;
        private final String timestampDefault;

        public StorageSourceParameters getParameters() {
            return parameters;
        }

        private StorageTextRowBatchSource(final SourceConfig config) {
            this.inputSchema = config.getSchema();
            this.parameters = new Gson().fromJson(config.getParameters(), StorageSourceParameters.class);
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
        }

        public PCollection<Row> expand(final PBegin begin) {

            final Format format = parameters.getFormat();

            switch (format) {
                case csv:
                case json: {
                    final SerializableFunction<String, Row> func;
                    if (this.inputSchema == null || (inputSchema.getAvroSchema() == null && inputSchema.getFields() == null)) {
                        this.schema = TextToRowConverter.DEFAULT_SCHEMA;
                        func = TextToRowConverter::convert;
                    } else if (Format.csv.equals(format)) {
                        this.schema = SourceConfig.convertSchema(inputSchema);
                        func = r -> CsvToRowConverter.convert(schema, r);
                    } else {
                        this.schema = SourceConfig.convertSchema(inputSchema);
                        func = r -> JsonToRowConverter.convert(schema, r);
                    }

                    TextIO.Read read = TextIO.read().from(parameters.getInput());
                    if (parameters.getCompression() != null) {
                        read = read.withCompression(Compression
                                .valueOf(parameters.getCompression().trim().toUpperCase()));
                    }

                    final PCollection<String> lines;
                    if (parameters.getFilterPrefix() != null) {
                        final String filterPrefix = parameters.getFilterPrefix();
                        lines = begin
                                .apply("ReadLine", read)
                                .apply("FilterPrefix", Filter.by(s -> !s.startsWith(filterPrefix)));
                    } else {
                        lines = begin.apply("ReadLine", read);
                    }

                    PCollection<Row> rows = lines
                            .apply("ConvertToRow", MapElements.into(TypeDescriptor.of(Row.class)).via(func))
                            .setRowSchema(schema);

                    if(timestampAttribute == null) {
                        return rows;
                    } else {
                        return rows.apply("WithTimestamp", DataTypeTransform
                                .withTimestamp(DataType.ROW, timestampAttribute, timestampDefault));
                    }
                }
                default:
                    throw new IllegalArgumentException("StorageSource only support format avro, csv, json. but: " + format);
            }
        }
    }

    private static class StorageMicrobatchRead extends PTransform<PCollection<Long>, PCollection<Row>> {

        private final SourceConfig config;

        private StorageMicrobatchRead(final SourceConfig config) {
            this.config = config;
        }

        public PCollection<Row> expand(final PCollection<Long> begin) {
            return null;
        }

    }

    private static class ToRecordDoFn extends DoFn<String, GenericRecord> {

        private final Format format;
        private final String schemaString;

        private transient org.apache.avro.Schema schema;

        ToRecordDoFn(final Format format, final String schemaString) {
            if(!Format.csv.equals(format) && !Format.json.equals(format)) {
                throw new IllegalArgumentException("Storage module not support text format: " + format);
            }
            this.format = format;
            this.schemaString = schemaString;
        }

        @Setup
        public void setup() {
            this.schema = new org.apache.avro.Schema.Parser().parse(this.schemaString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if(Format.csv.equals(format)) {
                c.output(CsvToRecordConverter.convert(schema, c.element()));
            } else {
                c.output(JsonToRecordConverter.convert(schema, c.element()));
            }
        }

    }

    private static org.apache.avro.Schema getAvroSchema(
            final SourceConfig.InputSchema inputSchema,
            final String accessKey, final String secretKey, final String region) {

        if(inputSchema != null) {
            if(inputSchema.getAvroSchema() != null) {
                if(inputSchema.getAvroSchema().startsWith("gs://")) {
                    final String schemaString = StorageUtil.readString(inputSchema.getAvroSchema());
                    return AvroSchemaUtil.convertSchema(schemaString);
                } else if(inputSchema.getAvroSchema().startsWith("s3://")) {
                    final String schemaString = S3Util.readString(inputSchema.getAvroSchema(), accessKey, secretKey, region);
                    return AvroSchemaUtil.convertSchema(schemaString);
                }
            } else if(inputSchema.getFields() != null && inputSchema.getFields().size() > 0) {
                return SourceConfig.convertAvroSchema(inputSchema.getFields());
            }
        }
        return null;
    }

    private static org.apache.avro.Schema getAvroSchema(
            final String input,
            final SourceConfig.InputSchema inputSchema,
            final String accessKey, final String secretKey, final String region) {

        org.apache.avro.Schema avroSchema = getAvroSchema(inputSchema, accessKey, secretKey, region);
        if(avroSchema != null) {
            return avroSchema;
        }

        if(input.startsWith("gs://")) {
            avroSchema = StorageUtil.getAvroSchema(input);
            if(avroSchema != null) {
                return avroSchema;
            }
            return StorageUtil.listFiles(input)
                    .stream()
                    .map(StorageUtil::getAvroSchema)
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else if(input.startsWith("s3://")) {
            final S3Client client = S3Util.storage(accessKey, secretKey, region);
            avroSchema = S3Util.getAvroSchema(client, input);
            if(avroSchema != null) {
                return avroSchema;
            }
            final String bucket = S3Util.getBucketName(input);
            return S3Util.listFiles(client, input)
                    .stream()
                    .map(object -> S3Util.getAvroSchema(client, bucket, object))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else {
            throw new IllegalArgumentException("Avro schema not found for input: " + input);
        }

    }

    private static org.apache.avro.Schema getParquetSchema(
            final String input,
            final SourceConfig.InputSchema inputSchema,
            final String accessKey, final String secretKey, final String region) {

        org.apache.avro.Schema avroSchema = getAvroSchema(inputSchema, accessKey, secretKey, region);
        if(avroSchema != null) {
            return avroSchema;
        }

        if(input.startsWith("gs://")) {
            avroSchema = StorageUtil.getParquetSchema(input);
            if(avroSchema != null) {
                return avroSchema;
            }
            return StorageUtil.listFiles(input)
                    .stream()
                    .map(StorageUtil::getParquetSchema)
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else if(input.startsWith("s3://")) {
            final S3Client client = S3Util.storage(accessKey, secretKey, region);
            avroSchema = S3Util.getParquetSchema(client, input);
            if(avroSchema != null) {
                return avroSchema;
            }
            final String bucket = S3Util.getBucketName(input);
            return S3Util.listFiles(client, input)
                    .stream()
                    .map(path -> S3Util.getParquetSchema(client, bucket, path))
                    .filter(Objects::nonNull)
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Avro schema not found!"));
        } else {
            throw new IllegalArgumentException("Avro schema not found for input: " + input);
        }

    }

}
