package com.mercari.solution.module.sink;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.module.sink.fileio.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import freemarker.template.Template;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;


public class StorageSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSink.class);

    private static class StorageSinkParameters implements Serializable {

        private String output;
        private WriteFormat format;
        private String prefix;
        private String suffix;
        private String tempDirectory;
        private Integer numShards;
        private Compression compression;
        private CodecName codec;
        private Boolean noSpilling;
        private String avroSchema;
        private List<String> outputTemplateArgs;
        private String outputNotify;
        private Boolean useLegacy;

        // csv
        private Boolean header;
        private Boolean bom;
        private Boolean outputEmpty;

        // Deprecated
        private String dynamicSplitField;
        private Boolean withoutSharding;
        private String datetimeFormat;
        private String datetimeFormatZone;
        private Boolean useOnlyEndDatetime;

        public String getOutput() {
            return output;
        }

        public WriteFormat getFormat() {
            return format;
        }

        public String getSuffix() {
            return suffix;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public Integer getNumShards() {
            return numShards;
        }

        public Compression getCompression() {
            return compression;
        }

        public CodecName getCodec() {
            return codec;
        }

        public Boolean getNoSpilling() {
            return noSpilling;
        }

        public String getAvroSchema() {
            return avroSchema;
        }

        public List<String> getOutputTemplateArgs() {
            return outputTemplateArgs;
        }

        public String getOutputNotify() {
            return outputNotify;
        }

        public Boolean getUseLegacy() {
            return useLegacy;
        }

        // for CSV format
        public Boolean getHeader() {
            return header;
        }

        public Boolean getBom() {
            return bom;
        }

        public Boolean getOutputEmpty() {
            return outputEmpty;
        }


        // Deprecated
        public String getDynamicSplitField() {
            return dynamicSplitField;
        }

        public Boolean getWithoutSharding() {
            return withoutSharding;
        }

        public String getDatetimeFormat() {
            return datetimeFormat;
        }

        public String getDatetimeFormatZone() {
            return datetimeFormatZone;
        }

        public Boolean getUseOnlyEndDatetime() {
            return useOnlyEndDatetime;
        }


        public static StorageSinkParameters of(final JsonElement jsonElement, final String name) {
            final StorageSinkParameters parameters = new Gson().fromJson(jsonElement, StorageSinkParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("storage sink[" + name + "].parameters must not be empty!");
            }
            parameters.validate(name);
            parameters.setDefaults();
            return parameters;
        }

        private void validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("storage sink[" + name + "].parameters.output must not be null");
            }
            if(this.format == null) {
                errorMessages.add("storage sink[" + name + "].parameters.format must not be null");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults() {
            if(this.suffix == null) {
                this.suffix = "";
            }
            if(this.codec == null) {
                this.codec = CodecName.SNAPPY;
            }
            if(this.noSpilling == null) {
                this.noSpilling = false;
            }
            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }
            if(useLegacy == null) {
                this.useLegacy = true;
            }

            // For CSV format
            if(this.header == null) {
                this.header = false;
            }
            if(this.bom == null) {
                this.bom = false;
            }

            // Deprecated
            if(this.withoutSharding == null) {
                this.withoutSharding = false;
            }
            if(this.datetimeFormat == null) {
                this.datetimeFormat = "yyyyMMddHHmmss";
            }
            if(this.datetimeFormatZone == null) {
                this.datetimeFormatZone = "Etc/GMT";
            }
            if(this.useOnlyEndDatetime == null) {
                this.useOnlyEndDatetime = false;
            }
        }
    }

    private enum WriteFormat {
        csv,
        json,
        avro,
        parquet
    }

    public enum CodecName {
        // common
        SNAPPY,
        UNCOMPRESSED,
        // avro only
        BZIP2,
        DEFLATE,
        XZ,
        // parquet only
        LZO,
        LZ4,
        LZ4_RAW,
        BROTLI,
        GZIP,
        ZSTD
    }

    public String getName() { return "storage"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("storage sink module[" + config.getName() + "] requires input or inputs parameter");
        }

        final StorageSinkParameters parameters = StorageSinkParameters.of(config.getParameters(), config.getName());

        final Schema outputAvroSchema;
        if(parameters.getAvroSchema() != null) {
            final String avroSchemaJson = StorageUtil.readString(parameters.getAvroSchema());
            outputAvroSchema = AvroSchemaUtil.convertSchema(avroSchemaJson);
            if(outputAvroSchema == null) {
                throw new IllegalArgumentException("Failed to get avroSchema: " + parameters.getAvroSchema());
            }
        } else if(DataType.UNIFIEDMUTATION.equals(inputs.get(0).getDataType())) {
            outputAvroSchema = UnifiedMutation.createAvroSchema();
        } else {
            outputAvroSchema = inputs.get(0).getAvroSchema();
        }

        try {
            config.outputAvroSchema(outputAvroSchema);
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for {} to path: {}", config.getName(), config.getOutputAvroSchema(), e);
        }

        final FCollection<?> output;
        if(inputs.size() > 1 || isTemplatePath(parameters.getOutput()) || !parameters.getUseLegacy()) {
            output = writeMulti(config.getName(), parameters, inputs, outputAvroSchema, waits);
        } else {
            final FCollection<?> input = inputs.get(0);
            output = write(config.getName(), parameters, input, waits);
        }
        return Collections.singletonMap(config.getName(), output);
    }

    public static FCollection<?> write(
            final String name,
            final StorageSinkParameters parameters,
            final FCollection<?> collection,
            final List<FCollection<?>> waits) {

        final StorageWrite write = new StorageWrite(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(name, write);
        final FCollection<?> fcollection = FCollection.update(collection, output);
        return fcollection;
    }

    public static FCollection<?> writeMulti(
            final String name,
            final StorageSinkParameters parameters,
            final List<FCollection<?>> inputs,
            final Schema outputAvroSchema,
            final List<FCollection<?>> waits) {

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final Write write = new Write(
                name,
                parameters,
                inputTags,
                inputNames,
                inputTypes,
                inputs.get(0).getAvroSchema(),
                outputAvroSchema,
                waits);

        final PCollection<Row> output = tuple.apply(name, write);
        return FCollection.of(name, output, DataType.ROW, OutputDoFn.schema);
    }

    public static class Write extends PTransform<PCollectionTuple, PCollection<Row>> {

        private final String name;
        private final StorageSinkParameters parameters;

        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final String inputAvroSchemaJson;
        private final String outputAvroSchemaJson;

        private final List<FCollection<?>> waitCollections;

        private Write(
                final String name,
                final StorageSinkParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final Schema inputAvroSchema,
                final Schema outputAvroSchema,
                final List<FCollection<?>> waits) {

            this.name = name;
            this.parameters = parameters;

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.inputAvroSchemaJson = inputAvroSchema.toString();
            this.outputAvroSchemaJson = outputAvroSchema.toString();
            this.waitCollections = waits;
        }

        public PCollection<Row> expand(final PCollectionTuple inputs) {

            final PCollection<UnionValue> unionValues = inputs
                    .apply("Union", Union.flatten(inputTags, inputTypes, inputNames));

            final PCollection<UnionValue> waited;
            if(waitCollections == null) {
                waited = unionValues;
            } else {
                final List<PCollection<?>> waits = waitCollections.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                waited = unionValues
                        .apply("Wait", Wait.on(waits))
                        .setCoder(unionValues.getCoder());
            }

            final String object = getObject(parameters.getOutput());
            final Schema inputAvroSchema = AvroSchemaUtil.convertSchema(inputAvroSchemaJson);

            final List<String> outputTemplateArgs = Optional
                    .ofNullable(parameters.getOutputTemplateArgs())
                    .orElseGet(() -> TemplateUtil.extractTemplateArgs(object, inputAvroSchema));

            final PCollection<KV<String, UnionValue>> unionValuesWithKey = waited
                    .apply("WithObjectName", ParDo.of(new ObjectNameDoFn(
                            this.name, object, outputTemplateArgs)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), waited.getCoder()));

            final FileIO.Sink<KV<String, UnionValue>> sink = switch (parameters.getFormat()) {
                case csv -> {
                    final List<String> fields = inputAvroSchema.getFields().stream()
                            .map(org.apache.avro.Schema.Field::name)
                            .toList();
                    yield UnionValueCsvSink.of(fields, parameters.getHeader(), null, parameters.getBom());
                }
                case json -> UnionValueJsonSink.of(null, false, parameters.getBom());
                case avro, parquet -> {
                    final boolean fitSchema = parameters.getAvroSchema() != null;
                    final Schema outputAvroSchema = AvroSchemaUtil.convertSchema(outputAvroSchemaJson);
                    yield switch (this.parameters.getFormat()) {
                        case avro -> UnionValueAvroSink.of(outputAvroSchema, parameters.getCodec(), fitSchema);
                        case parquet -> UnionValueParquetSink.of(outputAvroSchema, parameters.getCodec(), fitSchema);
                        default -> throw new IllegalArgumentException();
                    };
                }
            };

            final String label = switch (parameters.getFormat()) {
                case csv -> "WriteCSV";
                case json -> "WriteJSON";
                case avro -> "WriteAvro";
                case parquet -> "WriteParquet";
            };

            final WriteFilesResult writeFilesResult;
            if(isTemplatePath(object)) {
                final FileIO.Write<String, KV<String, UnionValue>> write = createDynamicWrite(parameters);
                writeFilesResult = unionValuesWithKey.apply(label + "Dynamic", write.via(sink));
            } else {
                final FileIO.Write<Void, KV<String, UnionValue>> write = createWrite(parameters);
                writeFilesResult = unionValuesWithKey.apply(label, write.via(sink));
            }

            final PCollection<KV> rows = writeFilesResult
                    .getPerDestinationOutputFilenames();
            return rows
                    .apply("Output", ParDo.of(new OutputDoFn(this.name)))
                    .setCoder(RowCoder.of(OutputDoFn.schema));
        }

        private static FileIO.Write<Void, KV<String, UnionValue>> createWrite(final StorageSinkParameters parameters) {
            final String bucket = getBucket(parameters.getOutput());
            final String object = getObject(parameters.getOutput());
            final String suffix = parameters.getSuffix();
            final Integer numShards = parameters.getNumShards();

            FileIO.Write<Void, KV<String, UnionValue>> write = FileIO
                    .<KV<String, UnionValue>>write()
                    .to(bucket)
                    .withNaming(getFileNaming(object, suffix, numShards));

            if(parameters.getNumShards() == null || parameters.getNumShards() < 1) {
                write = write.withAutoSharding();
            } else {
                write = write.withNumShards(parameters.getNumShards());
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }
            if(parameters.getCompression() != null) {
                write = write.withCompression(parameters.getCompression());
            }
            if(parameters.getNoSpilling()) {
                write = write.withNoSpilling();
            }
            return write;
        }

        private static FileIO.Write<String, KV<String, UnionValue>> createDynamicWrite(final StorageSinkParameters parameters) {
            final String bucket = getBucket(parameters.getOutput());
            final String suffix = parameters.getSuffix();
            final Integer numShards = parameters.getNumShards();

            FileIO.Write<String, KV<String, UnionValue>> write = FileIO
                    .<String, KV<String, UnionValue>>writeDynamic()
                    .to(bucket)
                    .by(KV::getKey)
                    .withDestinationCoder(StringUtf8Coder.of())
                    .withNaming(key -> getFileNaming(key, suffix, numShards));

            if(parameters.getNumShards() == null || parameters.getNumShards() < 1) {
                write = write.withAutoSharding();
            } else {
                write = write.withNumShards(parameters.getNumShards());
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }
            if(parameters.getCompression() != null) {
                write = write.withCompression(parameters.getCompression());
            }
            return write;
        }

        private static class ObjectNameDoFn extends DoFn<UnionValue, KV<String, UnionValue>> {

            private final String name;
            private final String path;
            private final List<String> templateArgs;
            private final boolean useTemplate;

            private transient Template pathTemplate;

            public ObjectNameDoFn(
                    final String name,
                    final String path,
                    final List<String> templateArgs) {

                this.name = name;
                this.path = path;
                this.templateArgs = templateArgs;
                this.useTemplate = isTemplatePath(this.path);
            }

            @Setup
            public void setup() {
                if(useTemplate) {
                    this.pathTemplate = TemplateUtil.createSafeTemplate("pathTemplate" + name, path);
                }
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final UnionValue unionValue = c.element();
                if(useTemplate) {
                    final Map<String, Object> values = UnionValue.getAsMap(unionValue, templateArgs);
                    TemplateUtil.setFunctions(values);
                    values.put("__timestamp", Instant.ofEpochMilli(c.timestamp().getMillis()));
                    final String key = TemplateUtil.executeStrictTemplate(pathTemplate, values);
                    c.output(KV.of(key, unionValue));
                } else {
                    c.output(KV.of(this.path, unionValue));
                }
            }

        }

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

            final PCollection<?> input;
            if(waits == null || waits.size() == 0) {
                input = inputP;
            } else {
                final List<PCollection<?>> wait = waits.stream()
                        .map(FCollection::getCollection)
                        .collect(Collectors.toList());
                input = inputP
                        .apply("Wait", Wait.on(wait))
                        .setCoder((Coder) inputP.getCoder());
            }

            final String destinationField = this.parameters.getDynamicSplitField();
            final WriteFilesResult writeResult = switch (this.parameters.getFormat()) {
                case csv ->
                    switch (collection.getDataType()) {
                        case AVRO -> {
                            final FileIO.Write<String, GenericRecord> write = createWrite(
                                    parameters, e -> e.get(destinationField).toString());
                            yield ((PCollection<GenericRecord>) input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getAvroSchema().getFields().stream()
                                            .map(org.apache.avro.Schema.Field::name)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RecordToCsvConverter::convert)));
                        }
                        case ROW -> {
                            final FileIO.Write<String, Row> write = createWrite(
                                    parameters, e -> e.getValue(destinationField).toString());
                            yield ((PCollection<Row>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RowToCsvConverter::convert)));
                        }
                        case STRUCT -> {
                            final FileIO.Write<String, Struct> write = createWrite(
                                    parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
                            yield ((PCollection<Struct>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    StructToCsvConverter::convert)));
                        }
                        case ENTITY -> {
                            final FileIO.Write<String, Entity> write = createWrite(
                                    parameters, e -> EntitySchemaUtil.getFieldValueAsString(e, destinationField));
                            yield ((PCollection<Entity>)input).apply("WriteCSV", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    EntityToCsvConverter::convert)));
                        }
                        default -> throw new IllegalArgumentException("Not supported csv input type: " + collection.getDataType());

                    };
                case json ->
                    switch (collection.getDataType()) {
                        case AVRO -> {
                            final FileIO.Write<String, GenericRecord> write = createWrite(
                                    parameters, e -> e.get(destinationField).toString());
                            yield ((PCollection<GenericRecord>) input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RecordToJsonConverter::convert)));
                        }
                        case ROW -> {
                            final FileIO.Write<String, Row> write = createWrite(
                                    parameters, e -> e.getValue(destinationField).toString());
                            yield ((PCollection<Row>) input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    RowToJsonConverter::convert)));
                        }
                        case STRUCT -> {
                            final FileIO.Write<String, Struct> write = createWrite(
                                    parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
                            yield ((PCollection<Struct>) input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSpannerType().getStructFields().stream()
                                            .map(Type.StructField::getName)
                                            .collect(Collectors.toList()),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    StructToJsonConverter::convert)));
                        }
                        case ENTITY -> {
                            final FileIO.Write<String, Entity> write = createWrite(
                                    parameters, e -> EntitySchemaUtil.getFieldValueAsString(e, destinationField));
                            yield ((PCollection<Entity>) input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    EntityToJsonConverter::convert)));
                        }
                        case UNIFIEDMUTATION -> {
                            final FileIO.Write<String, UnifiedMutation> write = createWrite(
                                    parameters, e -> UnifiedMutation.getAsString(e, destinationField));
                            yield ((PCollection<UnifiedMutation>) input).apply("WriteJson", write.via(TextFileSink.of(
                                    collection.getSchema().getFieldNames(),
                                    this.parameters.getHeader(),
                                    this.parameters.getBom(),
                                    UnifiedMutation::toJson)));
                        }
                        default -> throw new IllegalArgumentException("Not supported json input type: " + collection.getDataType());
                    };
                case avro, parquet -> {
                    final DataTypeTransform.TypeTransform<GenericRecord> transform = DataTypeTransform.transform(collection, DataType.AVRO);
                    PCollection<GenericRecord> records = input.apply(transform);
                    final FileIO.Write<String, GenericRecord> write = createWrite(
                            parameters, e -> e.get(destinationField).toString());
                    final org.apache.avro.Schema avroSchema;
                    if(parameters.getAvroSchema() == null) {
                        avroSchema = transform.getOutputCollection().getAvroSchema();
                    } else {
                        final String avroSchemaJson = StorageUtil.readString(parameters.getAvroSchema());
                        avroSchema = AvroSchemaUtil.convertSchema(avroSchemaJson);
                        records = records
                                .apply("FitSchema", ParDo.of(new FitSchemaDoFn(avroSchemaJson)))
                                .setCoder(AvroCoder.of(avroSchema));
                    }

                    yield switch (this.parameters.getFormat()) {
                        case avro -> {
                            AvroIO.Sink<GenericRecord> sink = AvroIO.sink(avroSchema);
                            if(parameters.getCodec() != null) {
                                sink = switch (parameters.getCodec()) {
                                    case BZIP2 -> sink.withCodec(CodecFactory.bzip2Codec());
                                    case SNAPPY -> sink.withCodec(CodecFactory.snappyCodec());
                                    case DEFLATE -> sink.withCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
                                    case XZ -> sink.withCodec(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL));
                                    case UNCOMPRESSED -> sink;
                                    default -> throw new IllegalArgumentException("Not supported avro compression: " + parameters.getCompression());
                                };
                            }
                            yield records.apply("WriteAvro", write.via(sink));
                        }
                        case parquet -> {
                            ParquetIO.Sink sink = ParquetIO.sink(avroSchema);
                            if(parameters.getCodec() != null) {
                                sink = switch (parameters.getCodec()) {
                                    case LZO -> sink.withCompressionCodec(CompressionCodecName.LZO);
                                    case LZ4 -> sink.withCompressionCodec(CompressionCodecName.LZ4);
                                    case LZ4_RAW -> sink.withCompressionCodec(CompressionCodecName.LZ4_RAW);
                                    case ZSTD -> sink.withCompressionCodec(CompressionCodecName.ZSTD);
                                    case SNAPPY -> sink.withCompressionCodec(CompressionCodecName.SNAPPY);
                                    case GZIP -> sink.withCompressionCodec(CompressionCodecName.GZIP);
                                    case BROTLI -> sink.withCompressionCodec(CompressionCodecName.BROTLI);
                                    case UNCOMPRESSED -> sink.withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
                                    default -> throw new IllegalArgumentException("Not supported parquet compression: " + parameters.getCompression());
                                };
                            }
                            yield records.apply("WriteParquet", write.via(sink));
                        }
                        default -> throw new IllegalArgumentException();
                    };
                }
                /*
                case "tfrecord":
                    final TFRecordSink.RecordFormatter<Row> tfrecordFormatter = RowToTFRecordConverter::convert;
                    //write = write.via(TFRecordSink.of(schema, tfrecordFormatter));
                    break;

                */
                default -> throw new IllegalArgumentException("Not supported format: " + this.parameters.getFormat());
            };

            if(parameters.getOutputEmpty() || parameters.getOutputNotify() != null) {
                final String emptyContent;
                if(WriteFormat.csv.equals(parameters.getFormat())) {
                    emptyContent = String.join(",", getFieldNames(collection));
                } else {
                    emptyContent = "";
                }
                ((PCollection<KV<String, String>>)writeResult.getPerDestinationOutputFilenames())
                        .apply("ExtractOutputPath", MapElements.into(TypeDescriptors.strings()).via(s -> s.getValue()))
                        .setCoder(NullableCoder.of(StringUtf8Coder.of()))
                        .apply("GloballyAggregate", Combine.globally((a, b) -> a + "\n" + b))
                        .apply("Postprocess", ParDo.of(new PostprocessDoFn(
                                parameters.getOutputNotify(),
                                parameters.getOutputEmpty(),
                                parameters.getOutput(),
                                emptyContent)));
            }

            return writeResult.getPerDestinationOutputFilenames();
        }

        private static List<String> getFieldNames(final FCollection<?> collection) {
            return switch (collection.getDataType()) {
                case ROW -> collection.getSchema().getFieldNames();
                case AVRO -> collection.getAvroSchema().getFields().stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList());
                case STRUCT -> collection.getSpannerType().getStructFields().stream()
                        .map(Type.StructField::getName)
                        .collect(Collectors.toList());
                case ENTITY -> collection.getSchema().getFieldNames();
                default -> throw new IllegalArgumentException("Not supported data type: " + collection.getDataType().name());
            };
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final StorageSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            final String suffix = parameters.getSuffix();
            if(parameters.getDynamicSplitField() != null) {
                write = FileIO.<String, InputT>writeDynamic()
                        .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                        .withDestinationCoder(StringUtf8Coder.of());
            } else {
                final String prefix = parameters.getPrefix() == null ? "" : parameters.getPrefix();
                final String outdir = parameters.getWithoutSharding() ? StorageUtil.removeDirSuffix(output) : output;
                write = FileIO.<String, InputT>writeDynamic()
                        .to(outdir)
                        .by(d -> prefix)
                        .withDestinationCoder(StringUtf8Coder.of());
            }

            if(parameters.getWithoutSharding()) {
                final String datetimeFormat = parameters.getDatetimeFormat();
                final String datetimeFormatZone = parameters.getDatetimeFormatZone();
                final Boolean useOnlyEndDatetime = parameters.getUseOnlyEndDatetime();
                if(parameters.getDynamicSplitField() != null) {
                    write = write
                            .withNaming(key -> FixedFileNaming.of(
                                    StorageUtil.addFilePrefix(output, key), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                } else {
                    write = write
                            .withNaming(key -> FixedFileNaming.of(
                                    StorageUtil.addFilePrefix(output, ""), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                }
                write = write.withNumShards(1);
            } else {
                write = write.withNaming(key -> FileIO.Write.defaultNaming(
                        StorageUtil.addFilePrefix(output, key),
                        suffix));
                if(parameters.getNumShards() != null) {
                    write = write.withNumShards(parameters.getNumShards());
                }
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }
            if(parameters.getCompression() != null) {
                write = write.withCompression(parameters.getCompression());
            }
            return write;
        }

        private static class FitSchemaDoFn extends DoFn<GenericRecord, GenericRecord> {

            private final String outputSchemaJson;

            private transient Schema outputSchema;

            FitSchemaDoFn(final String outputSchemaJson) {
                this.outputSchemaJson = outputSchemaJson;
            }

            @Setup
            public void setup() {
                this.outputSchema = AvroSchemaUtil.convertSchema(this.outputSchemaJson);
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final GenericRecord input = c.element();
                final GenericRecord output = AvroSchemaUtil.toBuilder(outputSchema, input).build();
                c.output(output);
            }

        }

        private static class PostprocessDoFn extends DoFn<String, Void> {

            private final String outputNotify;
            private final Boolean outputEmpty;
            private final String output;
            private final String emptyText;

            PostprocessDoFn(final String outputNotify, final Boolean outputEmpty, final String output, final String emptyText) {
                this.outputNotify = outputNotify;
                this.outputEmpty = outputEmpty;
                this.output = output;
                this.emptyText = emptyText;
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                final String filePaths = c.element();
                if(outputEmpty && filePaths == null) {
                    LOG.info("OutputEmptyFile: " + output);
                    StorageUtil.writeString(output, emptyText == null ? "" : emptyText);
                }

                if(outputNotify != null && outputNotify.startsWith("gs://")) {
                    LOG.info("OutputNotifyFile: " + outputNotify);
                    StorageUtil.writeString(outputNotify, filePaths == null ? "" : filePaths);
                }
            }

        }

    }

    public static class JsonSchemaDetector extends PTransform<PCollection<String>, PCollection<String>> {

        private final Double mapRate;

        JsonSchemaDetector(final Double mapRate) {
            this.mapRate = mapRate;
        }

        public PCollection<String> expand(final PCollection<String> input) {
            final PCollectionView<Long> counts = input
                    .apply("Count", Count.globally())
                    .apply("AsSingletonView", View.asSingleton());
            return input
                    .apply("JsonToColumns", ParDo.of(new ColumnDoFn()))
                    .apply("ColumnsToColumnSchema", Combine.perKey(new JsonAvroSchemaDetectorPerKeyFn()))
                    .apply("GlobalKey", WithKeys.of(""))
                    .apply("GroupByKey", GroupByKey.create())
                    .apply("ConvertToSchema", ParDo
                            .of(new SchemaDoFn(mapRate, counts))
                            .withSideInputs(counts));
        }

        private static class ColumnDoFn extends DoFn<String, KV<String, Column>> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String line = c.element();
                final List<Column> columns = new ArrayList<>();
                final JsonObject jsonObject = new Gson().fromJson(line, JsonObject.class);
                setJsonObject(columns, "", "", jsonObject);
                for(final Column column : columns) {
                    c.output(KV.of(column.path, column));
                }
            }

            private void setJsonObject(final List<Column> columns, final String id, final String path, final JsonObject jsonObject) {
                for(final Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                    setJsonElement(columns, id, path + "/" + entry.getKey(), entry.getValue());
                }
            }

            private void setJsonElement(final List<Column> columns, final String id, final String path, final JsonElement jsonElement) {
                setJsonElement(columns, id, path, jsonElement, false);
            }

            private void setJsonElement(final List<Column> columns, final String id, final String path, final JsonElement jsonElement, boolean inArray) {
                if(jsonElement.isJsonNull()) {
                    if(!inArray) {
                        columns.add(Column.of(id, path, "null", null));
                    }
                } else if(jsonElement.isJsonPrimitive()) {
                    if(!inArray) {
                        columns.add(Column.of(id, path, "primitive", jsonElement.getAsString()));
                    }
                } else if(jsonElement.isJsonObject()) {
                    if(!inArray) {
                        columns.add(Column.of(id, path, "object", null));
                    }
                    setJsonObject(columns, id, path, jsonElement.getAsJsonObject());
                } else if(jsonElement.isJsonArray()) {
                    final HashSet<String> elementTypes = new HashSet<>();
                    final List<Column> childColumns = new ArrayList<>();
                    for(final JsonElement jsonElementChild : jsonElement.getAsJsonArray()) {
                        final String elementType = getElementType(jsonElementChild);
                        if("array".equals(elementType)) {
                            elementTypes.add("object");
                            for(final JsonElement jsonElementGrandChild : jsonElementChild.getAsJsonArray()) {
                                final JsonObject jsonObject = new JsonObject();
                                jsonObject.add("value", jsonElementGrandChild);
                                setJsonElement(childColumns, id, path, jsonObject, true);
                            }
                        } else {
                            elementTypes.add(elementType);
                            setJsonElement(childColumns, id, path, jsonElementChild, true);
                        }
                    }
                    columns.addAll(childColumns);
                    columns.add(Column.of(id, path, "array", null, elementTypes));
                }
            }

            private String getElementType(final JsonElement element) {
                if(element.isJsonNull()) {
                    return "null";
                } else if(element.isJsonPrimitive()) {
                    return "primitive";
                } else if(element.isJsonObject()) {
                    return "object";
                } else if(element.isJsonArray()) {
                    return "array";
                } else {
                    throw new IllegalArgumentException("Illegal " + element.toString());
                }
            }
        }

        private static class JsonAvroSchemaDetectorPerKeyFn extends Combine.CombineFn<Column, ColumnSchema, ColumnSchema> {

            @Override
            public ColumnSchema createAccumulator() { return new ColumnSchema(); }

            @Override
            public ColumnSchema addInput(final ColumnSchema accum, final Column input) {
                if(accum.getPath() == null) {
                    accum.setPath(input.getPath());
                }
                accum.getTypes().add(input.getType());
                accum.getDataTypes().add(input.getDataType());
                if(input.getElementTypes() != null) {
                    accum.getElementTypes().addAll(input.getElementTypes());
                }
                accum.setCount(accum.getCount() + 1);
                return accum;
            }

            @Override
            public ColumnSchema mergeAccumulators(final Iterable<ColumnSchema> accums) {
                final ColumnSchema merged = createAccumulator();
                for(final ColumnSchema columnSchema : accums) {
                    if(merged.getPath() == null) {
                        merged.setPath(columnSchema.getPath());
                    }
                    merged.getTypes().addAll(columnSchema.getTypes());
                    merged.getDataTypes().addAll(columnSchema.getDataTypes());
                    merged.getElementTypes().addAll(columnSchema.getElementTypes());
                    merged.setCount(merged.getCount() + columnSchema.getCount());
                }
                return merged;
            }

            @Override
            public ColumnSchema extractOutput(final ColumnSchema accum) {
                return accum;
            }

        }

        private static class SchemaDoFn extends DoFn<KV<String, Iterable<KV<String, ColumnSchema>>>, String> {

            private static final Logger LOG = LoggerFactory.getLogger(SchemaDoFn.class);

            private static final org.apache.avro.Schema SCHEMA_NULL = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);


            private final Double mapRate;
            private final PCollectionView<Long> countsView;

            SchemaDoFn(final Double mapRate, final PCollectionView<Long> countsView) {
                this.mapRate = mapRate;
                this.countsView = countsView;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {

                final Long count = c.sideInput(countsView);

                final List<ColumnSchema> columnSchemas = new ArrayList<>();
                for(final KV<String, ColumnSchema> kv : c.element().getValue()) {
                    columnSchemas.add(kv.getValue());
                }

                final ColumnSchema root = new ColumnSchema();
                root.setPath("");
                root.setTypes(new HashSet<>());
                root.getTypes().add("object");
                root.setDataTypes(new HashSet<>());
                root.setElementTypes(new HashSet<>());

                LOG.info("ColumnSize: " + columnSchemas.size());
                for(ColumnSchema cs : columnSchemas) {
                    LOG.info(cs.path + " " + cs.types + " " + cs.dataTypes + " " + cs.elementTypes + " " + cs.count);
                }

                final org.apache.avro.Schema schema = convertSchema(root, columnSchemas, count);
                final String schemaString = schema.toString();
                LOG.info(schemaString);
                c.output(schemaString);
            }

            private org.apache.avro.Schema convertSchema(
                    final ColumnSchema parentColumn,
                    final List<ColumnSchema> descendantColumns,
                    final Long count) {

                final String type = mergeTypes(parentColumn.getTypes());
                switch (type) {
                    case "null": {
                        return AvroSchemaUtil.NULLABLE_STRING;
                    }
                    case "primitive": {
                        final String dataType = mergeDataTypes(parentColumn.getDataTypes());
                        switch (dataType) {
                            case "STRING":
                                return AvroSchemaUtil.NULLABLE_STRING;
                            case "BOOLEAN":
                                return AvroSchemaUtil.NULLABLE_BOOLEAN;
                            case "INT64":
                                return AvroSchemaUtil.NULLABLE_LONG;
                            case "FLOAT64":
                                return AvroSchemaUtil.NULLABLE_DOUBLE;
                            case "DATE":
                                return AvroSchemaUtil.NULLABLE_LOGICAL_DATE_TYPE;
                            case "TIME":
                                return AvroSchemaUtil.NULLABLE_LOGICAL_TIME_MILLI_TYPE;
                            case "TIMESTAMP":
                                return AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE;
                            default:
                                return AvroSchemaUtil.NULLABLE_STRING;
                        }
                    }
                    case "object": {
                        final List<ColumnSchema> childrenColumns = descendantColumns.stream()
                                .filter(cs -> cs.getPath().startsWith(parentColumn.getPath()))
                                .filter(cs -> !cs.getPath().replaceFirst(parentColumn.getPath() + "/", "").contains("/"))
                                .collect(Collectors.toList());

                        final double countMean = StatUtils.mean(childrenColumns.stream().mapToDouble(ColumnSchema::getCount).toArray());
                        final boolean containsInvalidName = childrenColumns.stream()
                                .map(ColumnSchema::getPath)
                                .anyMatch(c -> !AvroSchemaUtil.isValidFieldName(c));

                        final boolean isMap = containsInvalidName || (countMean / count) < mapRate;
                        if(isMap) {
                            return AvroSchemaUtil.NULLABLE_MAP_STRING;
                        } else {
                            final List<org.apache.avro.Schema.Field> fields = childrenColumns.stream()
                                    .map(cs -> new org.apache.avro.Schema.Field(
                                            getName(cs.getPath()),
                                            convertSchema(cs, descendantColumns.stream()
                                                    .filter(ccs -> ccs.getPath().startsWith(cs.getPath() + "/"))
                                                    .collect(Collectors.toList()), count),
                                            null,
                                            (Object)null,
                                            org.apache.avro.Schema.Field.Order.IGNORE))
                                    .collect(Collectors.toList());
                            if(fields.size() == 0) {
                                return SCHEMA_NULL;
                            }
                            final String name = getName(parentColumn.getPath());
                            final String namespace = getNamespace(parentColumn.getPath());
                            return org.apache.avro.Schema.createRecord(name, "", namespace, false, fields);
                        }
                    }
                    case "array": {
                        final List<org.apache.avro.Schema> elementSchemas = new ArrayList<>();
                        for(final String elementType : parentColumn.getElementTypes()) {
                            final ColumnSchema elementColumnSchema = new ColumnSchema();
                            elementColumnSchema.setPath(parentColumn.getPath());
                            elementColumnSchema.setTypes(new HashSet<>());
                            elementColumnSchema.getTypes().add(elementType);
                            final org.apache.avro.Schema elementSchema = convertSchema(elementColumnSchema, descendantColumns.stream()
                                    .filter(cs -> cs.getPath().startsWith(elementColumnSchema.getPath() + "/"))
                                    .collect(Collectors.toList()), count);
                            elementSchemas.add(elementSchema);
                        }
                        return org.apache.avro.Schema.createArray(elementSchemas.get(0));
                    }
                    default: {
                        return AvroSchemaUtil.NULLABLE_STRING;
                    }
                }
            }

            private static String getName(final String path) {
                if(path.length() == 0) {
                    return "root";
                }
                final String[] paths = path.replaceFirst("/", "").split("/");
                return paths[paths.length - 1];
            }

            private static String getNamespace(final String path) {
                if(path.length() == 0) {
                    return "";
                }
                final String namespace = path.replaceAll("/", ".");
                if(namespace.startsWith(".")) {
                    return namespace.substring(1);
                }
                return namespace;
            }

            private static String mergeTypes(final Set<String> types) {
                if(types.contains("primitive")) {
                    return "primitive";
                } else if(types.containsAll(Set.of("object", "null")) && types.size() == 2) {
                    return "object";
                } else if(types.containsAll(Set.of("array", "null")) && types.size() == 2) {
                    return "array";
                } else if(types.contains("object")  && types.size() == 1) {
                    return "object";
                } else if(types.contains("array")  && types.size() == 1) {
                    return "array";
                } else {
                    return "primitive";
                }
            }

            private static String mergeDataTypes(final Set<String> dataTypes) {
                if(dataTypes.contains("STRING")) {
                    return "STRING";
                } else if(dataTypes.containsAll(Set.of("FLOAT64", "INT64")) && dataTypes.size() == 2) {
                    return "FLOAT64";
                } else if(dataTypes.containsAll(Set.of("TIMESTAMP", "DATE")) && dataTypes.size() == 2) {
                    return "TIMESTAMP";
                } else if(dataTypes.contains("INT64")  && dataTypes.size() == 1) {
                    return "INT64";
                } else if(dataTypes.contains("FLOAT64")  && dataTypes.size() == 1) {
                    return "FLOAT64";
                } else if(dataTypes.contains("BOOLEAN")  && dataTypes.size() == 1) {
                    return "BOOLEAN";
                } else if(dataTypes.contains("DATE")  && dataTypes.size() == 1) {
                    return "DATE";
                } else if(dataTypes.contains("TIME")  && dataTypes.size() == 1) {
                    return "TIME";
                } else {
                    return "STRING";
                }
            }

        }

        @DefaultCoder(AvroCoder.class)
        private static class Column {

            private String id;
            private String path;
            private String type;
            @Nullable
            private String dataType;
            @Nullable
            private HashSet<String> elementTypes;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            public String getPath() {
                return path;
            }

            public void setPath(String path) {
                this.path = path;
            }

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }

            public String getDataType() {
                return dataType;
            }

            public void setDataType(String dataType) {
                this.dataType = dataType;
            }

            public Set<String> getElementTypes() {
                return elementTypes;
            }

            public void setElementTypes(HashSet<String> elementTypes) {
                this.elementTypes = elementTypes;
            }

            public static Column of(final String id, final String path, final String type, final String value) {
                return of(id, path, type, value, null);
            }

            public static Column of(final String id, final String path, final String type, final String value, final HashSet<String> elementTypes) {
                final Column column = new Column();
                column.setId(id);
                column.setPath(path);
                column.setType(type);
                column.setElementTypes(elementTypes);
                column.setDataType(getDataType(value));
                return column;
            }

            private static String getDataType(final String value) {
                if(value == null) {
                    return "NULL";
                }
                final String v = value.trim().toLowerCase();
                if(NumberUtils.isParsable(v)) {
                    if(NumberUtils.isDigits(v)) {
                        return "INT64";
                    } else {
                        return "FLOAT64";
                    }
                } else if("true".equals(v) || "false".equals(v)) {
                    return "BOOLEAN";
                } else if(DateTimeUtil.isTimestamp(v)) {
                    return "TIMESTAMP";
                } else if(DateTimeUtil.isDate(v)) {
                    return "DATE";
                } else if(DateTimeUtil.isTime(v)) {
                    return "TIME";
                } else if("null".equals(v)) {
                    return "NULL";
                } else {
                    return "STRING";
                }
            }

        }

        @DefaultCoder(AvroCoder.class)
        private static class ColumnSchema {

            @Nullable
            private String path;
            private HashSet<String> types;
            @Nullable
            private HashSet<String> elementTypes;
            @Nullable
            private HashSet<String> dataTypes;

            @Nullable
            private Long count;

            public String getPath() {
                return path;
            }

            public void setPath(String path) {
                this.path = path;
            }

            public Set<String> getTypes() {
                return types;
            }

            public void setTypes(HashSet<String> types) {
                this.types = types;
            }

            public HashSet<String> getElementTypes() {
                return elementTypes;
            }

            public void setElementTypes(HashSet<String> elementTypes) {
                this.elementTypes = elementTypes;
            }

            public Set<String> getDataTypes() {
                return dataTypes;
            }

            public void setDataTypes(HashSet<String> dataTypes) {
                this.dataTypes = dataTypes;
            }

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public ColumnSchema() {
                this.types = new HashSet<>();
                this.elementTypes = new HashSet<>();
                this.dataTypes = new HashSet<>();
                this.count = 0L;
            }

        }

    }

    private static class OutputDoFn extends DoFn<KV, Row> {

        private static final org.apache.beam.sdk.schemas.Schema schema = org.apache.beam.sdk.schemas.Schema.builder()
                .addField(org.apache.beam.sdk.schemas.Schema.Field.of("sink", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(false)))
                .addField(org.apache.beam.sdk.schemas.Schema.Field.of("path", org.apache.beam.sdk.schemas.Schema.FieldType.STRING.withNullable(false)))
                .addField(org.apache.beam.sdk.schemas.Schema.Field.of("timestamp", org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME.withNullable(false)))
                .build();

        private final String name;

        OutputDoFn(String name) {
            this.name = name;
        }


        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Row row = Row.withSchema(schema)
                    .withFieldValue("sink", this.name)
                    .withFieldValue("path", c.element().getValue().toString())
                    .withFieldValue("timestamp", c.timestamp())
                    .build();
            c.output(row);
        }

    }

    public static String getBucket(String output) {
        final String[] paths = output.replaceFirst("gs://", "").split("/", -1);
        return "gs://" + paths[0] + "/";
    }

    public static String getObject(String output) {
        final String[] paths = output.replaceFirst("gs://", "").split("/", 2);
        return paths[1];
    }

    private static boolean isTemplatePath(final String path) {
        if(path == null) {
            return false;
        }
        return path.contains("${");
    }

    private static FileIO.Write.FileNaming getFileNaming(
            final String key,
            final String suffix,
            final Integer numShards) {

        if(isTemplatePath(suffix)) {
            return new TemplateFileNaming(key, suffix);
        } else if(numShards != null && numShards == 1) {
            return new SingleFileNaming(key, suffix);
        }

        return FileIO.Write.defaultNaming(key, suffix);
    }


    private static class TemplateFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;
        private transient Template suffixTemplate;

        private TemplateFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            if(suffixTemplate == null) {
                this.suffixTemplate = TemplateUtil.createStrictTemplate("TemplateFileNaming", suffix);
            }

            final Map<String,Object> values = new HashMap<>();
            if (window != GlobalWindow.INSTANCE) {
                final IntervalWindow iw = (IntervalWindow)window;
                final Instant start = Instant.ofEpochMilli(iw.start().getMillis());
                final Instant end   = Instant.ofEpochMilli(iw.end().getMillis());
                values.put("windowStart", start);
                values.put("windowEnd",   end);
            }

            values.put("paneIndex", pane.getIndex());
            values.put("paneIsFirst", pane.isFirst());
            values.put("paneIsLast", pane.isLast());
            values.put("paneTiming", pane.getTiming().name());
            values.put("paneIsOnlyFiring", pane.isFirst() && pane.isLast());
            values.put("numShards", numShards);
            values.put("shardIndex", shardIndex);
            values.put("suggestedSuffix", compression.getSuggestedSuffix());

            TemplateUtil.setFunctions(values);

            final String filename = TemplateUtil.executeStrictTemplate(suffixTemplate, values);
            final String fullPath = this.path + filename;
            LOG.info("templateFilename: " + fullPath);
            return fullPath;
        }

    }

    private static class SingleFileNaming implements FileIO.Write.FileNaming {

        private final String path;
        private final String suffix;

        private SingleFileNaming(final String path, final String suffix) {
            this.path = path;
            this.suffix = suffix;
        }

        public String getFilename(final BoundedWindow window,
                                  final PaneInfo pane,
                                  final int numShards,
                                  final int shardIndex,
                                  final Compression compression) {

            final String fullPath = this.path + this.suffix;
            return fullPath;
        }

    }


}
