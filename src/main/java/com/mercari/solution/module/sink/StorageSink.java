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
import com.mercari.solution.module.sink.fileio.TextFileSink;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class StorageSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSink.class);

    private static class StorageSinkParameters implements Serializable {

        private String output;
        private String format;
        private String suffix;
        private String dynamicSplitField;
        private Boolean withoutSharding;
        private String tempDirectory;
        private Integer numShards;
        private String prefix;
        private String compression;
        private String outputNotify;

        private String datetimeFormat;
        private String datetimeFormatZone;
        private Boolean useOnlyEndDatetime;

        // csv
        private Boolean header;
        private Boolean bom;
        private Boolean outputEmpty;

        public String getOutput() {
            return output;
        }

        public String getFormat() {
            return format;
        }

        public String getSuffix() {
            return suffix;
        }

        public String getDynamicSplitField() {
            return dynamicSplitField;
        }

        public Boolean getWithoutSharding() {
            return withoutSharding;
        }

        public String getTempDirectory() {
            return tempDirectory;
        }

        public Integer getNumShards() {
            return numShards;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getCompression() {
            return compression;
        }

        public String getOutputNotify() {
            return outputNotify;
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

        public Boolean getHeader() {
            return header;
        }

        public Boolean getBom() {
            return bom;
        }

        public Boolean getOutputEmpty() {
            return outputEmpty;
        }

        private void validate() {

        }

        private void setDefaults() {
            if(this.format == null) {
                this.format = "avro";
            }
            if(this.suffix == null) {
                //this.parameters.setSuffix("." + this.parameters.getFormat());
                this.suffix = "";
            }
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
            if(this.header == null) {
                this.header = false;
            }
            if(this.bom == null) {
                this.bom = false;
            }
            if(this.outputEmpty == null) {
                this.outputEmpty = false;
            }
        }
    }

    public String getName() { return "storage"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("storage sink module requires input parameter");
        }
        final FCollection<?> input = inputs.get(0);
        return Collections.singletonMap(config.getName(), StorageSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final StorageSinkParameters parameters = new Gson().fromJson(config.getParameters(), StorageSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("storage sink module parameters must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

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
                                    parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
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
                                    parameters, e -> EntitySchemaUtil.getFieldValueAsString(e, destinationField));
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
                                    parameters, e -> StructSchemaUtil.getAsString(e, destinationField));
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
                                    parameters, e -> EntitySchemaUtil.getFieldValueAsString(e, destinationField));
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

                    if ("avro".equalsIgnoreCase(format.trim())) {
                        writeResult = records.apply("WriteAvro", write.via(AvroIO.sink(avroSchema)));
                    } else if("parquet".equalsIgnoreCase(format.trim())) {
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

            if(parameters.getOutputEmpty() || parameters.getOutputNotify() != null) {
                final String emptyContent;
                if("csv".equals(parameters.getFormat().trim().toLowerCase())) {
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
            switch (collection.getDataType()) {
                case ROW:
                    return collection.getSchema().getFieldNames();
                case AVRO:
                    return collection.getAvroSchema().getFields().stream()
                            .map(org.apache.avro.Schema.Field::name)
                            .collect(Collectors.toList());
                case STRUCT:
                    return collection.getSpannerType().getStructFields().stream()
                            .map(Type.StructField::getName)
                            .collect(Collectors.toList());
                case ENTITY:
                    return collection.getSchema().getFieldNames();
                default:
                    throw new IllegalArgumentException("Not supported data type: " + collection.getDataType().name());
            }
        }

        private <InputT> FileIO.Write<String, InputT> createWrite(
                final StorageSinkParameters parameters,
                final SerializableFunction<InputT, String> destinationFunction) {

            final String output = parameters.getOutput();

            FileIO.Write<String, InputT> write;
            final String suffix = parameters.getSuffix();
            final String prefix = parameters.getPrefix() == null ? "" : parameters.getPrefix();
            if(parameters.getDynamicSplitField() != null) {
                write = FileIO.<String, InputT>writeDynamic()
                        .to(output)
                        .by(d -> Optional.ofNullable(destinationFunction.apply(d)).orElse(""))
                        .withDestinationCoder(StringUtf8Coder.of());
            } else {
                final String outdir = parameters.getWithoutSharding() ? StorageUtil.removeDirSuffix(output) : output;
                write = FileIO.<String, InputT>writeDynamic()
                        .to(outdir)
                        .by(d -> prefix)
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
                                    StorageUtil.addFilePrefix(output, key), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                } else {
                    write = write
                            .withNaming(key -> FixedFileNaming.of(
                                    StorageUtil.addFilePrefix(output, ""), suffix,
                                    datetimeFormat, datetimeFormatZone, useOnlyEndDatetime));
                }
            } else {
                write = write.withNaming(key -> FileIO.Write.defaultNaming(
                        StorageUtil.addFilePrefix(output, key),
                        suffix));
            }

            if(parameters.getTempDirectory() != null) {
                write = write.withTempDirectory(parameters.getTempDirectory());
            }
            if(parameters.getNumShards() != null) {
                write = write.withNumShards(parameters.getNumShards());
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

}
