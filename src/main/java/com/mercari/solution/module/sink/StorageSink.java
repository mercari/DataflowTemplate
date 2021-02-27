package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.TextFileSink;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.DatastoreUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StorageSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSink.class);

    private class StorageSinkParameters implements Serializable {

        private String output;
        private String format;
        private String suffix;
        private String dynamicSplitField;
        private Boolean withoutSharding;
        private String tempDirectory;
        private Integer numShards;
        private String prefix;
        private String compression;

        private String datetimeFormat;
        private String datetimeFormatZone;
        private Boolean useOnlyEndDatetime;

        // csv
        private Boolean header;
        private Boolean bom;

        public String getOutput() {
            return output;
        }

        public void setOutput(String output) {
            this.output = output;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        public String getSuffix() {
            return suffix;
        }

        public void setSuffix(String suffix) {
            this.suffix = suffix;
        }

        public String getDynamicSplitField() {
            return dynamicSplitField;
        }

        public void setDynamicSplitField(String dynamicSplitField) {
            this.dynamicSplitField = dynamicSplitField;
        }

        public Boolean getWithoutSharding() {
            return withoutSharding;
        }

        public void setWithoutSharding(Boolean withoutSharding) {
            this.withoutSharding = withoutSharding;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public void setTempDirectory(String tempDirectory) {
            this.tempDirectory = tempDirectory;
        }

        public Integer getNumShards() {
            return numShards;
        }

        public void setNumShards(Integer numShards) {
            this.numShards = numShards;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public String getCompression() {
            return compression;
        }

        public void setCompression(String compression) {
            this.compression = compression;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        public void setDatetimeFormat(String datetimeFormat) {
            this.datetimeFormat = datetimeFormat;
        }

        public String getDatetimeFormatZone() {
            return datetimeFormatZone;
        }

        public void setDatetimeFormatZone(String datetimeFormatZone) {
            this.datetimeFormatZone = datetimeFormatZone;
        }

        public Boolean getUseOnlyEndDatetime() {
            return useOnlyEndDatetime;
        }

        public void setUseOnlyEndDatetime(Boolean useOnlyEndDatetime) {
            this.useOnlyEndDatetime = useOnlyEndDatetime;
        }

        public Boolean getHeader() {
            return header;
        }

        public void setHeader(Boolean header) {
            this.header = header;
        }

        public Boolean getBom() {
            return bom;
        }

        public void setBom(Boolean bom) {
            this.bom = bom;
        }

    }

    public String getName() { return "storage"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), StorageSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final StorageSinkParameters parameters = new Gson().fromJson(config.getParameters(), StorageSinkParameters.class);
        final StorageWrite write = new StorageWrite(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        final FCollection<?> fcollection = FCollection.update(collection, output);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return fcollection;
    }

    public static class StorageWrite extends PTransform<PCollection<?>, PCollection<KV>> {

        private FCollection<?> collection;
        private final StorageSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private StorageWrite(final FCollection<?> collection,
                             final StorageSinkParameters parameters,
                             final List<FCollection<?>> waits) {
            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<KV> expand(final PCollection<?> inputP) {

            validateParameters();
            setDefaultParameters();

            final PCollection<?> input;
            if(waits == null || waits.size() == 0) {
                input = inputP;
            } else {
                final List<PCollection<?>> wait = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = inputP
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder)inputP.getCoder());
            }

            final String format = this.parameters.getFormat();
            final String destinationField = this.parameters.getDynamicSplitField();
            WriteFilesResult writeResult;
            switch (format.trim().toLowerCase()) {
                case "csv": {
                    switch (collection.getDataType()) {
                        case AVRO: {
                            final FileIO.Write<String, GenericRecord> write = createWrite(
                                    parameters, e -> e.get(destinationField).toString());
                            writeResult = ((PCollection<GenericRecord>) input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getAvroSchema().getFields().stream()
                                            .map(org.apache.avro.Schema.Field::name)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RecordToCsvConverter::convert)));
                            break;
                        }
                        case ROW: {
                            final FileIO.Write<String, Row> write = createWrite(
                                    parameters, e -> e.getValue(destinationField).toString());
                            writeResult = ((PCollection<Row>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RowToCsvConverter::convert)));
                            break;
                        }
                        case STRUCT: {
                            final FileIO.Write<String, Struct> write = createWrite(
                                    parameters, e -> SpannerUtil.getAsString(e, destinationField));
                            writeResult = ((PCollection<Struct>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    StructToCsvConverter::convert)));
                            break;
                        }
                        case ENTITY: {
                            final FileIO.Write<String, Entity> write = createWrite(
                                    parameters, e -> DatastoreUtil.getFieldValueAsString(e, destinationField));
                            writeResult = ((PCollection<Entity>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    EntityToCsvConverter::convert)));
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Not supported csv input type: " + collection.getDataType());
                        }
                    }
                    break;
                }
                case "json": {
                    switch (collection.getDataType()) {
                        case AVRO: {
                            final FileIO.Write<String, GenericRecord> write = createWrite(
                                    parameters, e -> e.get(destinationField).toString());
                            writeResult = ((PCollection<GenericRecord>)input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RecordToJsonConverter::convert)));
                            break;
                        }
                        case ROW: {
                            final FileIO.Write<String, Row> write = createWrite(
                                    parameters, e -> e.getValue(destinationField).toString());
                            writeResult = ((PCollection<Row>)input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RowToJsonConverter::convert)));
                            break;
                        }
                        case STRUCT: {
                            final FileIO.Write<String, Struct> write = createWrite(
                                    parameters, e -> SpannerUtil.getAsString(e, destinationField));
                            writeResult = ((PCollection<Struct>)input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    StructToJsonConverter::convert)));
                            break;
                        }
                        case ENTITY: {
                            final FileIO.Write<String, Entity> write = createWrite(
                                    parameters, e -> DatastoreUtil.getFieldValueAsString(e, destinationField));
                            writeResult = ((PCollection<Entity>)input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    EntityToJsonConverter::convert)));
                            break;
                        }
                        default: {
                            throw new IllegalArgumentException("Not supported json input type: " + collection.getDataType());
                        }
                    }
                    break;
                }
                case "avro":
                case "parquet": {
                    final DataTypeTransform.TypeTransform<GenericRecord> transform = DataTypeTransform.transform(collection, DataType.AVRO);
                    final PCollection<GenericRecord> records = input.apply(transform);
                    final FileIO.Write<String, GenericRecord> write = createWrite(
                            parameters, e -> e.get(destinationField).toString());
                    final org.apache.avro.Schema avroSchema = transform.getOutputCollection().getAvroSchema();

                    if ("avro".equals(format.trim().toLowerCase())) {
                        writeResult = records.apply("WriteAvro", write.via(AvroIO.sink(avroSchema)));
                    } else if("parquet".equals(format.trim().toLowerCase())) {
                        writeResult = records.apply("WriteParquet", write.via(ParquetIO.sink(avroSchema)));
                    } else {
                        throw new IllegalArgumentException("Not supported format: " + format);
                    }
                    break;
                }
                /*
                case "tfrecord":
                    final TFRecordSink.RecordFormatter<Row> tfrecordFormatter = RowToTFRecordConverter::convert;
                    //write = write.via(TFRecordSink.of(schema, tfrecordFormatter));
                    break;

                */
                default:
                    throw new IllegalArgumentException("Not supported format: " + format);
            }

            return writeResult.getPerDestinationOutputFilenames();
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final StorageSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            final String suffix = parameters.getSuffix();
            if(parameters.getDynamicSplitField() != null) {
                write = FileIO.<String, InputT>writeDynamic()
                        .to(output)
                        .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                        .withDestinationCoder(StringUtf8Coder.of());
            } else {
                final String outdir = parameters.getWithoutSharding() ? StorageUtil.removeDirSuffix(output) : output;
                write = FileIO.<String, InputT>writeDynamic()
                        .to(outdir)
                        .by(d -> "")
                        .withDestinationCoder(StringUtf8Coder.of());
            }

            if(parameters.getWithoutSharding()) {
                write = write.withNumShards(1);
                final String datetimeFormat = parameters.getDatetimeFormat();
                final String datetimeFormatZone = parameters.getDatetimeFormatZone();
                final Boolean useOnlyEndDatetime = parameters.getUseOnlyEndDatetime();
                if(parameters.getDynamicSplitField() != null) {
                    write = write
                            .withNaming(key -> FixedFileNaming.of(
                                    StorageUtil.addFilePrefix(output, (String)key), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                } else {
                    write = write
                            .withNaming(key -> FixedFileNaming.of(
                                    StorageUtil.addFilePrefix(output, ""), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                }
            } else {
                write = write.withNaming(key -> FileIO.Write.defaultNaming(
                        StorageUtil.addFilePrefix(output, (String)key),
                        suffix));
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }
            if(parameters.getNumShards() != null) {
                write = write.withNumShards(parameters.getNumShards());
            }
            if(parameters.getPrefix() != null) {
                write = write.withPrefix(parameters.getPrefix());
            }
            if(parameters.getCompression() != null) {
                final Compression c;
                switch (parameters.getCompression().trim().toUpperCase()) {
                    case "ZIP":
                        c = Compression.ZIP;
                        break;
                    case "GZIP":
                        c = Compression.GZIP;
                        break;
                    case "BZIP2":
                        c = Compression.BZIP2;
                        break;
                    case "ZSTD":
                        c = Compression.ZSTD;
                        break;
                    case "LZO":
                        c = Compression.LZO;
                        break;
                    case "LZOP":
                        c = Compression.LZOP;
                        break;
                    case "AUTO":
                        c = Compression.AUTO;
                        break;
                    case "UNCOMPRESSED":
                        c = Compression.UNCOMPRESSED;
                        break;
                    default:
                        c = Compression.DEFLATE;
                        break;
                }
                write = write.withCompression(c);
            }
            return write;
        }

        private void validateParameters() {

        }

        private void setDefaultParameters() {
            if(this.parameters.getFormat() == null) {
                this.parameters.setFormat("avro");
            }
            if(this.parameters.getSuffix() == null) {
                //this.parameters.setSuffix("." + this.parameters.getFormat());
                this.parameters.setSuffix("");
            }
            if(this.parameters.getWithoutSharding() == null) {
                this.parameters.setWithoutSharding(false);
            }
            if(this.parameters.getDatetimeFormat() == null) {
                this.parameters.setDatetimeFormat("yyyyMMddHHmmss");
            }
            if(this.parameters.getDatetimeFormatZone() == null) {
                this.parameters.setDatetimeFormatZone("Etc/GMT");
            }
            if(this.parameters.getUseOnlyEndDatetime() == null) {
                this.parameters.setUseOnlyEndDatetime(false);
            }
            if(this.parameters.getHeader() == null) {
                this.parameters.setHeader(false);
            }
            if(this.parameters.getBom() == null) {
                this.parameters.setBom(false);
            }
        }

    }

}
