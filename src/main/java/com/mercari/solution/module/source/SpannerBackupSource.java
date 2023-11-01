package com.mercari.solution.module.source;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.RecordToMutationConverter;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class SpannerBackupSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerBackupSource.class);

    private static class SpannerBackupSourceParameters implements Serializable {

        // common
        private BackupType backupType;
        private OutputType outputType;

        // for snapshot backup
        private String snapshotBackupInput;

        // for changestream backup
        private String changeStreamBackupInput;
        private String changeStreamBackupStartAt;
        private BackupFormat changeStreamBackupFormat;


        private List<String> tables;
        //
        private String projectId;
        private String instanceId;
        private String databaseId;

        public BackupType getBackupType() {
            return backupType;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public String getSnapshotBackupInput() {
            return snapshotBackupInput;
        }

        public String getChangeStreamBackupInput() {
            return changeStreamBackupInput;
        }

        public String getChangeStreamBackupStartAt() {
            return changeStreamBackupStartAt;
        }

        public BackupFormat getChangeStreamBackupFormat() {
            return changeStreamBackupFormat;
        }


        public List<String> getTables() {
            return tables;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public void validate() {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.backupType == null) {
                errorMessages.add("Parameter must contain backupType");
            }
            if(BackupType.both.equals(this.backupType) && (this.snapshotBackupInput == null || this.changeStreamBackupInput == null)) {
                errorMessages.add("Parameter must contain both inputSnapshotBackup and inputChangeStreamBackup for backupType both");
            }
            if(BackupType.snapshot.equals(this.backupType) && this.snapshotBackupInput == null) {
                errorMessages.add("Parameter must contain inputSnapshotBackup for backupType snapshot");
            }
            if(BackupType.changestream.equals(this.backupType) && this.changeStreamBackupInput == null) {
                errorMessages.add("Parameter must contain inputChangeStreamBackup for backupType changestream");
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }
        public void setDefaults() {
            if(this.backupType == null) {
                this.backupType = BackupType.snapshot;
            }
            if(this.outputType == null) {
                this.outputType = OutputType.mutation;
            }
            if(this.changeStreamBackupFormat == null) {
                if(this.changeStreamBackupInput != null) {
                    if(this.changeStreamBackupInput.startsWith("gs://")) {
                        this.changeStreamBackupFormat = BackupFormat.avro;
                    } else {
                        this.changeStreamBackupFormat = BackupFormat.bigquery;
                    }
                } else {
                    this.changeStreamBackupFormat = BackupFormat.avro;
                }
            }
            if(this.tables == null) {
                this.tables = new ArrayList<>();
            }
            if(this.changeStreamBackupStartAt == null) {
                this.changeStreamBackupStartAt = "1970-01-01T00:00:00.000Z";
            }
        }
    }

    private enum BackupType implements Serializable {
        snapshot,
        changestream,
        both
    }

    private enum OutputType implements Serializable {
        record,
        mutation
    }

    private enum BackupFormat implements Serializable {
        avro,
        bigquery
    }

    public String getName() { return "spannerBackup"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if(OptionUtil.isStreaming(begin)) {
            throw new IllegalArgumentException("SpannerBackupSource does not support streaming mode.");
        }
        if(config.getMicrobatch() != null && config.getMicrobatch()) {
            throw new IllegalArgumentException("SpannerBackupSource does not support microbatch mode.");
        }

        final SpannerBackupSourceParameters parameters = new Gson().fromJson(config.getParameters(), SpannerBackupSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("SpannerBackupSource config must not be empty!");
        }
        parameters.validate();
        parameters.setDefaults();

        final Map<String, FCollection<?>> output = new HashMap<>();
        if(OutputType.mutation.equals(parameters.getOutputType())) {
            output.putAll(restore(begin, config, parameters));
        } else if(OutputType.record.equals(parameters.getOutputType())) {
            output.putAll(restore(begin, config, parameters));
        } else {
            throw new IllegalArgumentException("Not supported outputType: " + parameters.getOutputType());
        }
        return output;
    }

    public static Map<String, FCollection<?>> restore(
            final PBegin begin, final SourceConfig config, final SpannerBackupSourceParameters parameters) {

        final SpannerBackupBatchRestore source = new SpannerBackupBatchRestore(config, parameters);
        final PCollectionTuple mutationsTuple = begin.apply(config.getName(), source);

        final Map<TupleTag<?>, Schema> avroSchemas = source.avroSchemaStrings
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> AvroSchemaUtil.convertSchema(e.getValue())));

        if(OutputType.mutation.equals(parameters.getOutputType())) {
            final Map<TupleTag<?>, DataType> dataTypes = source.avroSchemaStrings
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> DataType.MUTATION));
            final FCollection collection = FCollection
                    .of(config.getName(), new HashMap<>(), mutationsTuple, dataTypes, avroSchemas);
            return Map.of(config.getName(), collection);
        } else if(OutputType.record.equals(parameters.getOutputType())) {
            final Map<String, FCollection<?>> output = new HashMap<>();
            for(final Map.Entry<TupleTag<?>, String> entry : source.tagNames.entrySet()) {
                final PCollection<GenericRecord> record = (PCollection<GenericRecord>) mutationsTuple.get(entry.getKey());
                final FCollection collection = FCollection
                        .of(config.getName(), record, DataType.AVRO, avroSchemas.get(entry.getKey()));
                final String name = String.format("%s.%s", config.getName(), entry.getValue());
                output.put(name, collection);
            }
            return output;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static class SpannerBackupBatchRestore extends PTransform<PBegin, PCollectionTuple> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerBackupBatchRestore.class);

        private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("`[a-zA-Z|_][0-9|a-zA-Z|_]*`");

        private final SpannerBackupSourceParameters parameters;

        // inner use
        private final Map<TupleTag<?>, String> tagNames;
        private final Map<TupleTag<?>, String> avroSchemaStrings;

        private SpannerBackupBatchRestore(final SourceConfig config,
                                          final SpannerBackupSourceParameters parameters) {

            this.parameters = parameters;
            this.tagNames = new HashMap<>();
            this.avroSchemaStrings = new HashMap<>();
        }

        @Override
        public PCollectionTuple expand(final PBegin begin) {
            // Read spanner snapshot backup files
            final Map<String, Schema> tableSchemas = new HashMap<>();
            final Map<String, TupleTag<KV<KV<String, String>, GenericRecord>>> tupleTags = new HashMap<>();
            PCollectionTuple snapshotWithKeysTuple = PCollectionTuple.empty(begin.getPipeline());
            if(BackupType.changestream.equals(parameters.getBackupType())) {
                final Map<String, Schema> types = SpannerUtil.getAvroSchemasFromDatabase(
                        parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), false);
                final Schema dummySchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
                for(final Map.Entry<String, Schema> entry : types.entrySet()) {
                    final String table = entry.getKey();
                    final Schema tableSchema = entry.getValue();
                    tableSchemas.put(table, tableSchema);

                    final PCollection<KV<KV<String, String>, GenericRecord>> snapshotWithKeys = begin
                            .apply("EmptySnapshot." + table, Create
                                    .empty(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), AvroCoder.of(dummySchema))));
                    final TupleTag<KV<KV<String, String>, GenericRecord>> tag = new TupleTag<>(){};
                    snapshotWithKeysTuple = snapshotWithKeysTuple.and(tag, snapshotWithKeys);
                    tupleTags.put(table, tag);
                }
            } else {
                final Map<String, List<String>> exportTables = readExportFiles(parameters.getSnapshotBackupInput());
                for(final Map.Entry<String, List<String>> entry : exportTables.entrySet()) {
                    if(entry.getValue().size() == 0) {
                        continue;
                    }
                    final String table = entry.getKey();
                    if(parameters.getTables().size() > 0 && !parameters.getTables().contains(table)) {
                        continue;
                    }
                    final KV<Schema, PCollection<GenericRecord>> schemaAndRecords = readRecords(begin, entry.getKey(), entry.getValue());
                    final Schema tableSchema = schemaAndRecords.getKey();
                    tableSchemas.put(table, tableSchema);

                    final String primaryKey = tableSchema.getProp("spannerPrimaryKey");
                    final List<String> primaryKeys = extractPrimaryKeys(primaryKey);
                    final PCollection<KV<KV<String, String>, GenericRecord>> snapshotWithKeys = schemaAndRecords.getValue()
                            .apply("SnapshotWithKeys." + table, ParDo
                                    .of(new SnapshotWithKeyDoFn(table, primaryKeys)))
                            .setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), AvroCoder.of(tableSchema)));
                    final TupleTag<KV<KV<String, String>, GenericRecord>> tag = new TupleTag<>(){};
                    snapshotWithKeysTuple = snapshotWithKeysTuple.and(tag, snapshotWithKeys);
                    tupleTags.put(table, tag);
                }
            }

            // Read spanner change stream records
            final Schema changeRecordAvroSchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
            final PCollection<GenericRecord> changeRecords;
            if(BackupType.snapshot.equals(parameters.getBackupType())) {
                changeRecords = begin
                        .apply("EmptyChangeRecord", Create.empty(AvroCoder.of(changeRecordAvroSchema)));
            } else {
                switch (parameters.getChangeStreamBackupFormat()) {
                    case avro: {
                        final PCollection<FileIO.ReadableFile> files = begin
                                .apply("SupplyChangeRecordPath", Create
                                        .of(parameters.getChangeStreamBackupInput()))
                                .apply("MatchAllChangeRecordPath", FileIO
                                        .matchAll()
                                        .withConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.ALLOW)))
                                .apply("ReadChangeRecordFiles", FileIO
                                        .readMatches());

                        if(BackupFormat.avro.equals(parameters.getChangeStreamBackupFormat())) {
                            changeRecords = files
                                    .apply("ReadGenericRecords", AvroIO
                                            .readFilesGenericRecords(changeRecordAvroSchema)
                                            .withUsesReshuffle(true))
                                    .setCoder(AvroCoder.of(changeRecordAvroSchema));
                        } else {
                            changeRecords = files
                                    .apply("ReadGenericRecords", ParquetIO
                                            .readFiles(changeRecordAvroSchema))
                                    .setCoder(AvroCoder.of(changeRecordAvroSchema));
                        }
                        break;
                    }
                    case bigquery: {
                        changeRecords = begin
                                .apply("ReadBigQuery", BigQueryIO
                                        .read(SchemaAndRecord::getRecord)
                                        .from(parameters.getChangeStreamBackupInput())
                                        .useAvroLogicalTypes()
                                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                                        .withCoder(AvroCoder.of(changeRecordAvroSchema)));
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Not supported formatChangeStreamBackup: " + parameters.getChangeStreamBackupFormat());
                }
            }

            final Map<String, String> tableSchemaStrings = tableSchemas
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().toString()));
            final TupleTag<KV<KV<String, String>, GenericRecord>> dummyTag = new TupleTag<>(){};

            final Long startCommitTimestampEpochMicros = DateTimeUtil.toEpochMicroSecond(parameters.getChangeStreamBackupStartAt());

            final PCollectionTuple changeRecordWithKeysTuple = changeRecords
                    .apply("ChangeRecordWithKey", ParDo
                            .of(new ChangeRecordWithKeyDoFn(tableSchemaStrings, tupleTags, startCommitTimestampEpochMicros))
                            .withOutputTags(dummyTag, TupleTagList.of(new ArrayList<>(tupleTags.values()))));

            final boolean skipDeleteMutation = !BackupType.changestream.equals(parameters.getBackupType());
            final TupleTag<GenericRecord> snapshotTagTag = new TupleTag<>();
            final TupleTag<GenericRecord> changeRecordsTag = new TupleTag<>();

            PCollectionTuple outputTuple = PCollectionTuple.empty(begin.getPipeline());
            for(final Map.Entry<String, TupleTag<KV<KV<String, String>, GenericRecord>>> entry : tupleTags.entrySet()) {
                final String table = entry.getKey();
                final Schema tableSchema = tableSchemas.get(table);
                final TupleTag<KV<KV<String, String>, GenericRecord>> tag = entry.getValue();
                final KeyedPCollectionTuple<KV<String, String>> keyedTuple = KeyedPCollectionTuple
                        .of(snapshotTagTag, snapshotWithKeysTuple.get(tag))
                        .and(changeRecordsTag, changeRecordWithKeysTuple.get(tag));

                final TupleTag outputTag = new TupleTag<>(){};
                final PCollection<KV<KV<String, String>, CoGbkResult>> coGroupResult = keyedTuple
                        .apply("Merge." + table, CoGroupByKey.create());
                if(OutputType.mutation.equals(parameters.getOutputType())) {
                    final PCollection<Mutation> mutations = coGroupResult
                            .apply("ToMutation." + table, ParDo
                                    .of(new MergeMutationDoFn(skipDeleteMutation, tableSchema.toString(), snapshotTagTag, changeRecordsTag)));

                    outputTuple = outputTuple.and(outputTag, mutations);
                } else if(OutputType.record.equals(parameters.getOutputType())) {
                    final PCollection<GenericRecord> records = coGroupResult
                            .apply("ToRecord." + table, ParDo
                                    .of(new MergeRecordDoFn(tableSchema.toString(), snapshotTagTag, changeRecordsTag)))
                            .setCoder(AvroCoder.of(tableSchema));

                    outputTuple = outputTuple.and(outputTag, records);
                } else {
                    throw new IllegalArgumentException();
                }

                this.tagNames.put(outputTag, table);
                this.avroSchemaStrings.put(outputTag, tableSchema.toString());
            }

            return outputTuple;
        }

        private static class ChangeRecordWithKeyDoFn extends DoFn<GenericRecord, KV<KV<String, String>, GenericRecord>> {

            private final Map<String, String> tableSchemasStrings;
            private final Map<String, TupleTag<KV<KV<String, String>, GenericRecord>>> tags;
            private final Long startCommitTimestamp;

            private transient Map<String, Schema> tableSchemas;

            ChangeRecordWithKeyDoFn(final Map<String, String> tableSchemaStrings,
                                    final Map<String, TupleTag<KV<KV<String, String>, GenericRecord>>> tags,
                                    final Long startCommitTimestamp) {
                this.tableSchemasStrings = tableSchemaStrings;
                this.tags = tags;
                this.startCommitTimestamp = startCommitTimestamp;
            }

            @Setup
            public void setup() {
                this.tableSchemas = tableSchemasStrings
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> AvroSchemaUtil.convertSchema(e.getValue())));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final GenericRecord changeRecord = c.element();
                final String tableName = StructSchemaUtil.getChangeRecordTableName(changeRecord);
                if(!tags.containsKey(tableName)) {
                    return;
                }
                final Long commitTimestampMicros = (Long)changeRecord.get("commitTimestamp");
                if(commitTimestampMicros == null) {
                    throw new IllegalStateException();
                }
                if(commitTimestampMicros <= startCommitTimestamp) {
                    return;
                }
                final TupleTag<KV<KV<String, String>, GenericRecord>> tag = tags.get(tableName);
                final Schema tableSchema = tableSchemas.get(tableName);
                final List<KV<com.google.cloud.spanner.Key, GenericRecord>> keyAndChangeRecords = StructSchemaUtil
                        .createChangeRecordKey(tableSchema, changeRecord);

                for(final KV<com.google.cloud.spanner.Key, GenericRecord> keyAndChangeRecord : keyAndChangeRecords) {
                    c.output(tag, KV.of(KV.of(tableName, keyAndChangeRecord.getKey().toString()), keyAndChangeRecord.getValue()));
                }
            }
        }

        private static class SnapshotWithKeyDoFn extends DoFn<GenericRecord, KV<KV<String ,String>, GenericRecord>> {

            private final String tableName;
            private final List<String> primaryKeyFields;

            SnapshotWithKeyDoFn(
                    final String tableName,
                    final List<String> primaryKeyFields) {

                this.tableName = tableName;
                this.primaryKeyFields = primaryKeyFields;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final GenericRecord record = c.element();
                final com.google.cloud.spanner.Key key = RecordToMutationConverter.createKey(record, primaryKeyFields);
                c.output(KV.of(KV.of(tableName, key.toString()), record));
            }

        }

        private static class MergeMutationDoFn extends DoFn<KV<KV<String, String>, CoGbkResult>, Mutation> {

            private final TupleTag<GenericRecord> snapshotTag;
            private final TupleTag<GenericRecord> changeRecordsTag;
            private final boolean skipDeleteMutation;
            private final String schemaString;

            private transient Schema tableSchema;

            MergeMutationDoFn(final boolean skipDeleteMutation,
                              final String schemaString,
                              final TupleTag<GenericRecord> snapshotTag,
                              final TupleTag<GenericRecord> changeRecordsTag) {

                this.skipDeleteMutation = skipDeleteMutation;
                this.schemaString = schemaString;
                this.changeRecordsTag = changeRecordsTag;
                this.snapshotTag = snapshotTag;
            }

            @Setup
            public void setup() {
                this.tableSchema = AvroSchemaUtil.convertSchema(schemaString);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String table = c.element().getKey().getKey();
                final CoGbkResult result = c.element().getValue();
                final List<GenericRecord> snapshots = Lists.newArrayList(result.getAll(snapshotTag));
                if(snapshots.size() > 1) {
                    final String hashKey = c.element().getKey().getValue();
                    throw new IllegalArgumentException("snapshots size must be one but got " + snapshots.size() + " for table: " + table + ", key: " + hashKey);
                }
                final GenericRecord snapshot = snapshots.size() > 0 ? snapshots.get(0) : null;
                final List<GenericRecord> changeRecords  = Lists
                        .newArrayList(result.getAll(changeRecordsTag))
                        .stream()
                        .sorted(Comparator.comparing((GenericRecord r) -> (Long)(r.get("commitTimestamp"))))
                        .collect(Collectors.toList());
                try {
                    final Mutation mutation = StructSchemaUtil.accumulateChangeRecords(table, tableSchema, snapshot, changeRecords);
                    if(skipDeleteMutation && Mutation.Op.DELETE.equals(mutation.getOperation())) {
                        return;
                    }
                    c.output(mutation);
                } catch (Throwable e) {
                    final String message = "Failed to accum table: " + table + " schema: " + tableSchema + " snapshot: " + snapshot + " changeRecords: " + changeRecords;
                    LOG.error(message);
                    throw new IllegalStateException(message, e);
                }
            }

        }

        private static class MergeRecordDoFn extends DoFn<KV<KV<String, String>, CoGbkResult>, GenericRecord> {

            private final TupleTag<GenericRecord> snapshotTag;
            private final TupleTag<GenericRecord> changeRecordsTag;
            private final String schemaString;

            private transient Schema tableSchema;

            MergeRecordDoFn(final String schemaString,
                            final TupleTag<GenericRecord> snapshotTag,
                            final TupleTag<GenericRecord> changeRecordsTag) {

                this.schemaString = schemaString;
                this.changeRecordsTag = changeRecordsTag;
                this.snapshotTag = snapshotTag;
            }

            @Setup
            public void setup() {
                this.tableSchema = AvroSchemaUtil.convertSchema(schemaString);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String table = c.element().getKey().getKey();
                final CoGbkResult result = c.element().getValue();
                final List<GenericRecord> snapshots = Lists.newArrayList(result.getAll(snapshotTag));
                if(snapshots.size() > 1) {
                    final String hashKey = c.element().getKey().getValue();
                    throw new IllegalArgumentException("snapshots size must be one but got " + snapshots.size() + " for table: " + table + ", key: " + hashKey);
                }
                final GenericRecord snapshot = snapshots.size() > 0 ? snapshots.get(0) : null;
                final List<GenericRecord> changeRecords  = Lists
                        .newArrayList(result.getAll(changeRecordsTag))
                        .stream()
                        .sorted(Comparator.comparing((GenericRecord r) -> (Long)(r.get("commitTimestamp"))))
                        .collect(Collectors.toList());
                try {
                    final GenericRecord record = StructSchemaUtil.accumulateChangeRecords(tableSchema, snapshot, changeRecords);
                    if(record == null) {
                        return;
                    }
                    c.output(record);
                } catch (Throwable e) {
                    final String message = "Failed to accum table: " + table + " schema: " + tableSchema + " snapshot: " + snapshot + " changeRecords: " + changeRecords;
                    LOG.error(message);
                    throw new IllegalStateException(message, e);
                }

            }

        }

        private List<String> extractPrimaryKeys(final String str) {
            final List<String> primaryKeys = new ArrayList<>();
            final String[] keys = str.split(",");
            for(String key : keys) {
                final Matcher matcher = COLUMN_NAME_PATTERN.matcher(key);
                if(matcher.find()) {
                    final String primaryKey = matcher.group().replaceAll("`", "");
                    primaryKeys.add(primaryKey);
                }
            }
            return primaryKeys;
        }

    }

    private static KV<Schema, PCollection<GenericRecord>> readRecords(
            final PBegin begin, final String tableName, final List<String> files) {

        final Schema avroSchema = StorageUtil.getAvroSchema(files.get(0));
        final PCollection<GenericRecord> records = begin
                .apply("Files." + tableName, Create.of(files))
                .apply("Match." + tableName, FileIO
                        .matchAll()
                        .withConfiguration(FileIO.MatchConfiguration.create(EmptyMatchTreatment.ALLOW)))
                .apply("Reads." + tableName, FileIO.readMatches())
                .apply("Parse." + tableName, AvroIO.readFilesGenericRecords(avroSchema))
                .setCoder(AvroCoder.of(avroSchema));

        return KV.of(avroSchema, records);
    }

    private static Map<String, List<String>> readExportFiles(final String input) {
        final String exportFilePath;
        final String exportDirPath;
        if(input.endsWith("spanner-export.json")) {
            exportFilePath = input;
        } else if(input.endsWith("/")) {
            exportFilePath = input + "spanner-export.json";
        } else {
            exportFilePath = input + "/spanner-export.json";
        }
        exportDirPath = exportFilePath.replace("spanner-export.json", "");
        final Map<String, List<String>> paths = new HashMap<>();
        LOG.info(exportFilePath);
        final String exportContent = StorageUtil.readString(exportFilePath);
        final JsonObject exportFile = new Gson().fromJson(exportContent, JsonObject.class);
        for(final JsonElement table : exportFile.getAsJsonArray("tables")) {
            final String tableName = table.getAsJsonObject().get("name").getAsString();
            final String manifestFilePath = table.getAsJsonObject().get("manifestFile").getAsString();

            final String manifestContent = StorageUtil.readString(exportDirPath + manifestFilePath);
            final JsonObject manifestFile = new Gson().fromJson(manifestContent, JsonObject.class);
            final List<String> files = new ArrayList<>();
            for(final JsonElement avroFile : manifestFile.getAsJsonArray("files")) {
                final String avroFilePath = avroFile.getAsJsonObject().get("name").getAsString();
                files.add(exportDirPath + avroFilePath);
            }

            paths.put(tableName, files);
        }
        return paths;
    }
}