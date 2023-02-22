package com.mercari.solution.module.source;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.StructToRecordConverter;
import com.mercari.solution.util.converter.StructToRowConverter;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class SpannerSource implements SourceModule {

    private static final String SQL_SPLITTER = "--SPLITTER--";
    private static final Logger LOG = LoggerFactory.getLogger(SpannerSource.class);

    private class SpannerSourceParameters implements Serializable {

        // common
        private Mode mode;
        private String projectId;
        private String instanceId;
        private String databaseId;
        private String query;
        private Options.RpcPriority priority;
        private Boolean emulator;
        private OutputType outputType;

        // for batch
        private String table;
        private List<String> fields;
        private List<KeyRangeParameter> keyRange;
        private String timestampBound;

        // for changestream
        private ChangeStreamMode changeStreamMode;
        private String changeStreamName;
        private String metadataInstance;
        private String metadataDatabase;
        private String metadataTable;
        private String inclusiveStartAt;
        private String inclusiveEndAt;
        private Boolean outputChangeRecord;
        private List<String> tables;

        // for microbatch
        private Integer intervalSecond;
        private Integer gapSecond;
        private Integer maxDurationMinute;
        private Integer catchupIntervalSecond;

        private String startDatetime;
        private String outputCheckpoint;
        private Boolean useCheckpointAsStartDatetime;


        public Mode getMode() {
            return mode;
        }

        public void setMode(Mode mode) {
            this.mode = mode;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public void setDatabaseId(String databaseId) {
            this.databaseId = databaseId;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public List<KeyRangeParameter> getKeyRange() {
            return keyRange;
        }

        public void setKeyRange(List<KeyRangeParameter> keyRange) {
            this.keyRange = keyRange;
        }

        public String getTimestampBound() {
            return timestampBound;
        }

        public void setTimestampBound(String timestampBound) {
            this.timestampBound = timestampBound;
        }

        public Options.RpcPriority getPriority() {
            return priority;
        }

        public void setPriority(Options.RpcPriority priority) {
            this.priority = priority;
        }

        public Boolean getEmulator() {
            return emulator;
        }

        public void setEmulator(Boolean emulator) {
            this.emulator = emulator;
        }

        public ChangeStreamMode getChangeStreamMode() {
            return changeStreamMode;
        }

        public OutputType getOutputType() {
            return outputType;
        }

        public void setOutputType(OutputType outputType) {
            this.outputType = outputType;
        }

        public void setChangeStreamMode(ChangeStreamMode changeStreamMode) {
            this.changeStreamMode = changeStreamMode;
        }

        public String getChangeStreamName() {
            return changeStreamName;
        }

        public void setChangeStreamName(String changeStreamName) {
            this.changeStreamName = changeStreamName;
        }

        public String getMetadataInstance() {
            return metadataInstance;
        }

        public void setMetadataInstance(String metadataInstance) {
            this.metadataInstance = metadataInstance;
        }

        public String getMetadataDatabase() {
            return metadataDatabase;
        }

        public void setMetadataDatabase(String metadataDatabase) {
            this.metadataDatabase = metadataDatabase;
        }

        public String getMetadataTable() {
            return metadataTable;
        }

        public void setMetadataTable(String metadataTable) {
            this.metadataTable = metadataTable;
        }

        public String getInclusiveStartAt() {
            return inclusiveStartAt;
        }

        public void setInclusiveStartAt(String inclusiveStartAt) {
            this.inclusiveStartAt = inclusiveStartAt;
        }

        public String getInclusiveEndAt() {
            return inclusiveEndAt;
        }

        public void setInclusiveEndAt(String inclusiveEndAt) {
            this.inclusiveEndAt = inclusiveEndAt;
        }

        public Boolean getOutputChangeRecord() {
            return outputChangeRecord;
        }

        public void setOutputChangeRecord(Boolean outputChangeRecord) {
            this.outputChangeRecord = outputChangeRecord;
        }


        public List<String> getTables() {
            return tables;
        }

        public void setTables(List<String> tables) {
            this.tables = tables;
        }

        public Integer getIntervalSecond() {
            return intervalSecond;
        }

        public void setIntervalSecond(Integer intervalSecond) {
            this.intervalSecond = intervalSecond;
        }

        public Integer getGapSecond() {
            return gapSecond;
        }

        public void setGapSecond(Integer gapSecond) {
            this.gapSecond = gapSecond;
        }

        public Integer getMaxDurationMinute() {
            return maxDurationMinute;
        }

        public void setMaxDurationMinute(Integer maxDurationMinute) {
            this.maxDurationMinute = maxDurationMinute;
        }

        public Integer getCatchupIntervalSecond() {
            return catchupIntervalSecond;
        }

        public void setCatchupIntervalSecond(Integer catchupIntervalSecond) {
            this.catchupIntervalSecond = catchupIntervalSecond;
        }

        public String getStartDatetime() {
            return startDatetime;
        }

        public void setStartDatetime(String startDatetime) {
            this.startDatetime = startDatetime;
        }

        public String getOutputCheckpoint() {
            return outputCheckpoint;
        }

        public void setOutputCheckpoint(String outputCheckpoint) {
            this.outputCheckpoint = outputCheckpoint;
        }

        public Boolean getUseCheckpointAsStartDatetime() {
            return useCheckpointAsStartDatetime;
        }

        public void setUseCheckpointAsStartDatetime(Boolean useCheckpointAsStartDatetime) {
            this.useCheckpointAsStartDatetime = useCheckpointAsStartDatetime;
        }

        private class KeyRangeParameter {

            private String startType;
            private String endType;
            private JsonElement startKeys;
            private JsonElement endKeys;

            public String getStartType() {
                return startType;
            }

            public void setStartType(String startType) {
                this.startType = startType;
            }

            public String getEndType() {
                return endType;
            }

            public void setEndType(String endType) {
                this.endType = endType;
            }

            public JsonElement getStartKeys() {
                return startKeys;
            }

            public void setStartKeyValues(JsonElement startKeys) {
                this.startKeys = startKeys;
            }

            public JsonElement getEndKeys() {
                return endKeys;
            }

            public void setEndKeyValues(JsonElement endKeys) {
                this.endKeys = endKeys;
            }
        }

        private class ChangeStreamFilterParameter {

            private String startType;
            private String endType;
            private JsonElement startKeys;
            private JsonElement endKeys;

            public String getStartType() {
                return startType;
            }

            public void setStartType(String startType) {
                this.startType = startType;
            }

            public String getEndType() {
                return endType;
            }

            public void setEndType(String endType) {
                this.endType = endType;
            }

            public JsonElement getStartKeys() {
                return startKeys;
            }

            public void setStartKeyValues(JsonElement startKeys) {
                this.startKeys = startKeys;
            }

            public JsonElement getEndKeys() {
                return endKeys;
            }

            public void setEndKeyValues(JsonElement endKeys) {
                this.endKeys = endKeys;
            }
        }

        public void validateBatchParameters() {
            final List<String> errorMessages = validateCommonParameters();
            if(query == null && table == null) {
                errorMessages.add("spanner source module config requires query or table parameter in batch mode");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        public void validateChangeStreamParameters() {
            final List<String> errorMessages = validateCommonParameters();
            if(changeStreamName == null) {
                errorMessages.add("spanner source module config requires changeStreamName parameter in changestream mode");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        public void validateMicroBatchParameters() {
            final List<String> errorMessages = validateCommonParameters();
            if(query == null) {
                errorMessages.add("spanner source module config requires query parameter in microbatch mode");
            }
            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setBatchDefaultParameters() {
            this.setCommonDefaultParameters();
            if(this.outputType == null) {
                this.outputType = OutputType.avro;
            }
        }

        private void setChangeStreamDefaultParameters() {
            this.setCommonDefaultParameters();
            if(this.changeStreamMode == null) {
                this.changeStreamMode = ChangeStreamMode.struct;
            }
            if(this.metadataInstance == null) {
                this.metadataInstance = instanceId;
            }
            if(this.metadataDatabase == null) {
                this.metadataDatabase = databaseId;
            }
            if(this.inclusiveStartAt == null) {
                this.inclusiveStartAt = Timestamp.now().toString();
            }
            if(this.inclusiveEndAt == null) {
                this.inclusiveEndAt = Timestamp.MAX_VALUE.toString();
            }

            if(this.getTables() == null) {
                this.setTables(new ArrayList<>());
            }

            if(this.outputChangeRecord == null) {
                this.outputChangeRecord = true;
            }

            if(this.outputType == null) {
                this.outputType = OutputType.row;
            }

        }

        private void setMicroBatchDefaultParameters() {
            this.setCommonDefaultParameters();
            if(intervalSecond == null) {
                this.setIntervalSecond(60);
            }
            if(gapSecond == null) {
                this.setGapSecond(30);
            }
            if(maxDurationMinute == null) {
                this.setMaxDurationMinute(60);
            }
            if(catchupIntervalSecond == null) {
                this.setCatchupIntervalSecond(this.getIntervalSecond());
            }
            if(useCheckpointAsStartDatetime == null) {
                this.setUseCheckpointAsStartDatetime(false);
            }

            if(this.outputType == null) {
                this.outputType = OutputType.avro;
            }
        }

        private List<String> validateCommonParameters() {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(projectId == null) {
                errorMessages.add("spanner source module config requires projectId");
            }
            if(instanceId == null) {
                errorMessages.add("spanner source module config requires instanceId");
            }
            if(databaseId == null) {
                errorMessages.add("spanner source module config requires databaseId");
            }

            return errorMessages;
        }

        private void setCommonDefaultParameters() {
            if (priority == null) {
                this.setPriority(Options.RpcPriority.MEDIUM);
            }
            if (emulator == null) {
                this.setEmulator(false);
            }
        }
    }

    private enum OutputType implements Serializable {
        row,
        avro
    }

    private enum Mode implements Serializable {
        batch,
        microbatch,
        changestream
    }

    private enum ChangeStreamMode implements Serializable {
        struct,
        mutation,
        mutationGroup
    }

    public String getName() { return "spanner"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        final SpannerSourceParameters parameters = new Gson().fromJson(config.getParameters(), SpannerSourceParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
        }
        final boolean isStreaming = OptionUtil.isStreaming(begin.getPipeline().getOptions());
        final Mode mode = Optional
                .ofNullable(parameters.getMode())
                .orElse(isStreaming ? (config.getMicrobatch() != null && config.getMicrobatch() ? Mode.microbatch : Mode.changestream) : Mode.batch);

        switch (mode) {
            case batch:
                return Collections.singletonMap(config.getName(), batch(begin, config, parameters));
            case microbatch:
                return Collections.singletonMap(config.getName(), microbatch(beats, config, parameters));
            case changestream:
                return changestream(begin, config, parameters);
            default:
                throw new IllegalStateException("spanner source module does not support mode: " + mode);
        }
    }

    public static FCollection<Struct> batch(final PBegin begin, final SourceConfig config, final SpannerSourceParameters parameters) {
        parameters.validateBatchParameters();
        parameters.setBatchDefaultParameters();
        final SpannerBatchSource source = new SpannerBatchSource(config, parameters);
        final PCollection<Struct> output = begin.apply(config.getName(), source);
        return FCollection.of(config.getName(), output, DataType.STRUCT, source.type);
    }

    public static Map<String, FCollection<?>> changestream(
            final PBegin begin,
            final SourceConfig config,
            final SpannerSourceParameters parameters) {

        parameters.validateChangeStreamParameters();
        parameters.setChangeStreamDefaultParameters();

        final Map<String, Type> tableTypes = SpannerUtil
                .getTypesFromDatabase(parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getEmulator());
        switch (parameters.getChangeStreamMode()) {
            case struct: {
                if(parameters.getTables().size() == 0) {
                    parameters.setTables(new ArrayList<>(tableTypes.keySet()));
                }
                final Map<String, Type> filteredTableTypes = tableTypes
                        .entrySet()
                        .stream()
                        .filter(e -> parameters.getTables().contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                switch (parameters.getOutputType()) {
                    case row: {
                        return createRowStream(begin, config.getName(), parameters, filteredTableTypes);
                    }
                    case avro: {
                        return createAvroStream(begin, config.getName(), parameters, filteredTableTypes);
                    }
                    default: {
                        throw new IllegalStateException("spanner source module does not support outputType: " + parameters.getOutputType());
                    }
                }
            }
            /*
            case mutation: {
                final Schema dummySchema = StructSchemaUtil.createDataChangeRecordRowSchema();
                final PCollection<Mutation> output = begin
                        .apply(new SpannerChangeStreamMutationRead(parameters, tableTypes))
                        .setCoder(SerializableCoder.of(Mutation.class));
                return Collections.singletonMap(config.getName(), FCollection.of(config.getName(), output, DataType.MUTATION, dummySchema));
            }
             */
            case mutation: {
                final Schema dummySchema = StructSchemaUtil.createDataChangeRecordRowSchema();
                final PCollection<MutationGroup> output = begin
                        .apply(new SpannerChangeStreamMutationGroupRead(parameters, tableTypes))
                        .setCoder(SerializableCoder.of(MutationGroup.class));
                return Collections.singletonMap(config.getName(), FCollection.of(config.getName(), output, DataType.MUTATIONGROUP, dummySchema));
            }
            default: {
                throw new IllegalStateException("Spanner source module does not support changeStreamMode: " + parameters.getChangeStreamMode());
            }
        }
    }

    public static FCollection<Struct> microbatch(
            final PCollection<Long> beats,
            final SourceConfig config,
            final SpannerSourceParameters parameters) {

        parameters.validateMicroBatchParameters();
        parameters.setMicroBatchDefaultParameters();

        final SpannerMicrobatchRead source = new SpannerMicrobatchRead(config, parameters);
        final PCollection<Struct> output = beats.apply(config.getName(), source);
        return FCollection.of(config.getName(), output, DataType.STRUCT, source.type);
    }

    public static class SpannerBatchSource extends PTransform<PBegin, PCollection<Struct>> {

        private static final TupleTag<KV<String, KV<BatchTransactionId, Partition>>> tagOutputPartition = new TupleTag<>(){ private static final long serialVersionUID = 1L; };
        private static final TupleTag<Struct> tagOutputStruct = new TupleTag<>(){ private static final long serialVersionUID = 1L; };

        private Type type;

        private final String timestampAttribute;
        private final String timestampDefault;
        private final SpannerSourceParameters parameters;

        private final Map<String, Object> templateArgs;


        private SpannerBatchSource(final SourceConfig config, final SpannerSourceParameters parameters) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.templateArgs = config.getArgs();
            this.parameters = parameters;
        }

        public PCollection<Struct> expand(final PBegin begin) {

            final String projectId = parameters.getProjectId();
            final String instanceId = parameters.getInstanceId();
            final String databaseId = parameters.getDatabaseId();
            final String timestampBound = parameters.getTimestampBound();

            final PCollection<Struct> structs;
            if(parameters.getQuery() != null) {

                final PCollectionView<Transaction> transactionView = begin
                        .apply(Create.of(1L))
                        .apply("CreateTransaction", ParDo.of(new CreateTransactionFn(projectId, instanceId, databaseId, timestampBound, parameters.getEmulator())))
                        .apply("AsView", View.asSingleton());

                final String query;
                if(parameters.getQuery().startsWith("gs://")) {
                    final String rawQuery = StorageUtil.readString(parameters.getQuery());
                    query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                } else {
                    query = parameters.getQuery();
                }

                this.type = SpannerUtil.getTypeFromQuery(projectId, instanceId, databaseId, query, parameters.getEmulator());
                final PCollectionTuple results = begin
                        .apply("SupplyQuery", Create.of(query))
                        .apply("SplitQuery", FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))))
                        .apply("ExecuteQuery", ParDo.of(new QueryPartitionSpannerDoFn(
                                    projectId, instanceId, databaseId, timestampBound,
                                    parameters.getPriority(), parameters.getEmulator(), transactionView))
                                .withSideInput("transactionView", transactionView)
                                .withOutputTags(tagOutputPartition, TupleTagList.of(tagOutputStruct)));

                final PCollection<Struct> struct1 = results.get(tagOutputPartition)
                        .apply("GroupByPartition", GroupByKey.create())
                        .apply("ReadStruct", ParDo.of(new ReadStructSpannerDoFn(projectId, instanceId, databaseId, parameters.getEmulator(), transactionView))
                                .withSideInput("transactionView", transactionView))
                        .setCoder(SerializableCoder.of(Struct.class));
                final PCollection<Struct> struct2 = results.get(tagOutputStruct);
                structs = PCollectionList.of(struct1).and(struct2)
                        .apply(Flatten.pCollections());

            } else if(parameters.getTable() != null) {
                final String table = parameters.getTable();
                if(parameters.getFields() != null) {
                    final List<String> columns = parameters.getFields().stream()
                            .map(String::trim)
                            .collect(Collectors.toList());
                    this.type = SpannerUtil.getTypeFromTable(projectId, instanceId, databaseId, table, columns, parameters.getEmulator());
                } else {
                    this.type = SpannerUtil.getTypeFromTable(projectId, instanceId, databaseId, table, null, parameters.getEmulator());
                }

                // TODO check columns exists in table
                final List<String> columns = type.getStructFields().stream()
                        .map(Type.StructField::getName)
                        .collect(Collectors.toList());
                final List<SpannerSourceParameters.KeyRangeParameter> keyRanges = parameters.getKeyRange();
                final KeySet keySet;
                if(keyRanges == null) {
                    keySet = KeySet.all();
                } else {
                    final List<String> keyFieldNames = SpannerUtil.getPrimaryKeyFieldNames(projectId, instanceId, databaseId, table, parameters.getEmulator());
                    final List<Type.StructField> keyFields = keyFieldNames.stream()
                            .map(f -> type.getStructFields().stream()
                                    .filter(s -> s.getName().equals(f))
                                    .findAny()
                                    .orElseThrow(() -> new IllegalArgumentException("PrimaryKey: " + f + " not found!")))
                            .collect(Collectors.toList());

                    final KeySet.Builder builder = KeySet.newBuilder();
                    for(final SpannerSourceParameters.KeyRangeParameter keyRangeParameter : keyRanges) {
                        final KeyRange.Endpoint startType;
                        if(keyRangeParameter.getStartType() == null) {
                            startType = KeyRange.Endpoint.CLOSED;
                        } else {
                            startType = "open".equalsIgnoreCase(keyRangeParameter.getStartType()) ?
                                    KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                        }

                        final KeyRange.Endpoint endType;
                        if(keyRangeParameter.getEndType() == null) {
                            endType = KeyRange.Endpoint.CLOSED;
                        } else {
                            endType = "open".equalsIgnoreCase(keyRangeParameter.getEndType()) ?
                                    KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                        }
                        final Key start = createRangeKey(keyFields, keyRangeParameter.getStartKeys());
                        final Key end   = createRangeKey(keyFields, keyRangeParameter.getEndKeys());

                        builder.addRange(KeyRange.newBuilder()
                                .setStartType(startType)
                                .setEndType(endType)
                                .setStart(start)
                                .setEnd(end)
                                .build());
                    }
                    keySet = builder.build();
                }

                final SpannerIO.Read read = SpannerIO.read()
                        .withProjectId(projectId)
                        .withInstanceId(instanceId)
                        .withDatabaseId(databaseId)
                        .withTable(table)
                        .withKeySet(keySet)
                        .withColumns(columns)
                        .withBatching(true)
                        .withTimestampBound(toTimestampBound(timestampBound));

                if(parameters.getEmulator()) {
                    structs = begin.apply("ReadSpannerTable", read
                            .withEmulatorHost(SpannerUtil.SPANNER_HOST_EMULATOR));
                } else {
                    structs = begin.apply("ReadSpannerTable", read);
                }
            } else {
                throw new IllegalArgumentException("spanner module support only query or table");
            }

            if(timestampAttribute == null) {
                return structs;
            } else {
                return structs.apply("WithTimestamp", DataTypeTransform
                        .withTimestamp(DataType.STRUCT, timestampAttribute, timestampDefault));
            }

        }

        private static Key createRangeKey(final List<Type.StructField> keyFields, final JsonElement keyValues) {
            final Key.Builder key = Key.newBuilder();
            if(keyValues == null) {
                return key.build();
            }
            if(keyValues.isJsonPrimitive()) {
                final Type.StructField field = keyFields.get(0);
                setRangeKey(key, field, keyValues);
            } else {
                for(int i=0; i< keyValues.getAsJsonArray().size(); i++) {
                    final Type.StructField field = keyFields.get(i);
                    setRangeKey(key, field, keyValues.getAsJsonArray().get(i));
                }
            }
            return key.build();
        }

        private static void setRangeKey(final Key.Builder key, final Type.StructField field, final JsonElement element) {
            switch (field.getType().getCode()) {
                case STRING:
                    key.append(element.getAsString());
                    break;
                case INT64:
                    key.append(element.getAsLong());
                    break;
                case FLOAT64:
                    key.append(element.getAsDouble());
                    break;
                case BOOL:
                    key.append(element.getAsBoolean());
                    break;
                case DATE:
                    key.append(Date.parseDate(element.getAsString()));
                    break;
                case TIMESTAMP:
                    key.append(Timestamp.parseTimestamp(element.getAsString()));
                    break;
                case STRUCT:
                case ARRAY:
                case BYTES:
                default:
                    break;
            }
        }

        public static class QueryPartitionSpannerDoFn extends DoFn<String, KV<String, KV<BatchTransactionId, Partition>>> {

            private static final Logger LOG = LoggerFactory.getLogger(QueryPartitionSpannerDoFn.class);

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final String timestampBound;
            private final Options.RpcPriority priority;
            private final Boolean emulator;
            private final PCollectionView<Transaction> transactionView;

            private QueryPartitionSpannerDoFn(
                    final String projectId,
                    final String instanceId,
                    final String databaseId,
                    final String timestampBound,
                    final Options.RpcPriority priority,
                    final Boolean emulator,
                    final PCollectionView<Transaction> transactionView) {

                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.timestampBound = timestampBound;
                this.priority = priority;
                this.emulator = emulator;
                this.transactionView = transactionView;
            }

            @Setup
            public void setup() {
                LOG.info("QueryPartitionSpannerDoFn.setup");
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String query = c.element();
                LOG.info(String.format("Received query [%s], timestamp bound [%s]", query, this.timestampBound));
                final Statement statement = Statement.of(query);
                final Transaction tx = c.sideInput(transactionView);

                try(final Spanner spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, true, this.emulator)) {
                    final BatchReadOnlyTransaction transaction = spanner
                            .getBatchClient(DatabaseId.of(projectId, instanceId, databaseId))
                            .batchReadOnlyTransaction(tx.transactionId());

                    final PartitionOptions options = PartitionOptions.newBuilder()
                            //.setMaxPartitions(10000) // Note: this hint is currently ignored in v1.
                            //.setPartitionSizeBytes(100000000) // Note: this hint is currently ignored in v1.
                            .build();
                    try {
                        final List<Partition> partitions = transaction
                                .partitionQuery(options, statement, Options.priority(priority));
                        LOG.info(String.format("Query [%s] divided to [%d] partitions.", query, partitions.size()));
                        for (int i = 0; i < partitions.size(); ++i) {
                            final KV<BatchTransactionId, Partition> value = KV.of(transaction.getBatchTransactionId(), partitions.get(i));
                            final String key = String.format("%d-%s", i, query);
                            final KV<String, KV<BatchTransactionId, Partition>> kv = KV.of(key, value);
                            c.output(kv);
                        }
                    } catch (SpannerException e) {
                        if (!e.getErrorCode().equals(ErrorCode.INVALID_ARGUMENT)) {
                            throw e;
                        }
                        LOG.warn(String.format("Query [%s] could not be executed. Retrying as single query.", query));
                        try (final ResultSet resultSet = transaction.executeQuery(statement, Options.priority(priority))) {
                            int count = 0;
                            while (resultSet.next()) {
                                c.output(tagOutputStruct, resultSet.getCurrentRowAsStruct());
                                count++;
                            }
                            LOG.info(String.format("Query read record num [%d]", count));
                        }
                    }
                }

            }

            @Teardown
            public void teardown() {
                LOG.info("QueryPartitionSpannerDoFn.teardown");
            }

        }

        public static class ReadStructSpannerDoFn extends DoFn<KV<String, Iterable<KV<BatchTransactionId, Partition>>>, Struct> {

            private static final Logger LOG = LoggerFactory.getLogger(ReadStructSpannerDoFn.class);

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final Boolean emulator;
            private final PCollectionView<Transaction> transactionView;
            private transient Spanner spanner;
            private transient BatchClient batchClient;

            private ReadStructSpannerDoFn(final String projectId,
                                          final String instanceId,
                                          final String databaseId,
                                          final Boolean emulator,
                                          final PCollectionView<Transaction> transactionView) {

                this.projectId = projectId;
                this.instanceId= instanceId;
                this.databaseId = databaseId;
                this.emulator = emulator;
                this.transactionView = transactionView;
            }

            @Setup
            public void setup() {
                LOG.info("ReadStructSpannerDoFn.setup");
                this.spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, true, this.emulator);
                this.batchClient = spanner.getBatchClient(DatabaseId.of(projectId, instanceId, databaseId));
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final KV<String, Iterable<KV<BatchTransactionId, Partition>>> kv = c.element();
                final String partitionNumberQuery = kv.getKey();
                final KV<BatchTransactionId, Partition> value = kv.getValue().iterator().next();

                final Transaction tx = c.sideInput(transactionView);
                final BatchReadOnlyTransaction transaction = this.batchClient.batchReadOnlyTransaction(tx.transactionId()); // DO NOT CLOSE!!!
                final Partition partition = value.getValue();

                try(final ResultSet resultSet = transaction.execute(partition)) {
                    LOG.info(String.format("Started %s th partition[%s] query.", partitionNumberQuery.split("-")[0], partition));
                    int count = 0;
                    while (resultSet.next()) {
                        c.output(resultSet.getCurrentRowAsStruct());
                        count++;
                    }
                    LOG.info(String.format("%s th partition completed to read record: [%d]", partitionNumberQuery.split("-")[0], count));
                }
            }

            @Teardown
            public void teardown() {
                this.spanner.close();
                LOG.info("ReadStructSpannerDoFn.teardown");
            }

        }

    }

    private static SpannerIO.ReadChangeStream createDataChangeRecordSource(final SpannerSourceParameters parameters) {
        final SpannerConfig spannerConfig = SpannerConfig.create()
                .withHost(ValueProvider.StaticValueProvider.of(SpannerUtil.SPANNER_HOST_BATCH))
                .withProjectId(parameters.getProjectId())
                .withInstanceId(parameters.getInstanceId())
                .withDatabaseId(parameters.getDatabaseId());

        SpannerIO.ReadChangeStream readChangeStream = SpannerIO.readChangeStream()
                .withSpannerConfig(spannerConfig)
                .withChangeStreamName(parameters.getChangeStreamName())
                .withMetadataInstance(parameters.getMetadataInstance())
                .withMetadataDatabase(parameters.getMetadataDatabase())
                .withRpcPriority(parameters.getPriority());

        if(parameters.getInclusiveStartAt() != null) {
            final Timestamp inclusiveStartAt = Timestamp.parseTimestamp(parameters.getInclusiveStartAt());
            readChangeStream = readChangeStream.withInclusiveStartAt(inclusiveStartAt);
        }
        if(parameters.getInclusiveEndAt() != null) {
            final Timestamp inclusiveEndAt = Timestamp.parseTimestamp(parameters.getInclusiveEndAt());
            readChangeStream = readChangeStream.withInclusiveEndAt(inclusiveEndAt);
        }
        if(parameters.getMetadataTable() != null) {
            readChangeStream = readChangeStream.withMetadataTable(parameters.getMetadataTable());
        }

        return readChangeStream;
    }

    private static class SpannerChangeStreamStructRead<SchemaInputT, SchemaRuntimeT, T> extends PTransform<PBegin, PCollectionTuple> {

        private final SpannerSourceParameters parameters;

        private final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter;
        private final DataChangeRecordConverter<SchemaRuntimeT, T> changeRecordConverter;
        private final DataChangeRecordConverter<SchemaRuntimeT, T> tableRecordConverter;
        private final SchemaInputT changeRecordSchema;
        private final Map<String, SchemaInputT> tableRecordSchemas;
        private final TupleTag<T> changeRecordTag;
        private final Map<String, TupleTag<T>> tableTags;

        private SpannerChangeStreamStructRead(final SpannerSourceParameters parameters,
                                        final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter,
                                        final DataChangeRecordConverter<SchemaRuntimeT, T> changeRecordConverter,
                                        final DataChangeRecordConverter<SchemaRuntimeT, T> tableRecordConverter,
                                        final SchemaInputT changeRecordSchema,
                                        final Map<String, SchemaInputT> tableRecordSchemas,
                                        final TupleTag<T> changeRecordTag,
                                        final Map<String, TupleTag<T>> tableTags) {

            this.parameters = parameters;
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            this.schemaConverter = schemaConverter;
            this.changeRecordConverter = changeRecordConverter;
            this.tableRecordConverter = tableRecordConverter;
            this.changeRecordSchema = changeRecordSchema;
            this.tableRecordSchemas = tableRecordSchemas;
            this.changeRecordTag = changeRecordTag;
            this.tableTags = tableTags;
        }

        public PCollectionTuple expand(final PBegin begin) {
            final SpannerIO.ReadChangeStream readChangeStream = createDataChangeRecordSource(parameters);
            final PCollection<DataChangeRecord> dataChangeRecords = begin
                    .apply("ReadChangeStream", readChangeStream);

            return dataChangeRecords
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("Convert", ParDo.of(new ConvertDoFn<>(
                                    parameters,
                                    schemaConverter, changeRecordConverter, tableRecordConverter,
                                    changeRecordSchema, tableRecordSchemas, tableTags))
                            .withOutputTags(changeRecordTag, TupleTagList.of(new ArrayList<>(tableTags.values()))));
        }

        private static class ConvertDoFn<SchemaInputT,SchemaRuntimeT,T> extends DoFn<DataChangeRecord, T> {

            private static final Logger LOG = LoggerFactory.getLogger(ConvertDoFn.class);

            private final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter;
            private final DataChangeRecordConverter<SchemaRuntimeT, T> changeRecordConverter;
            private final DataChangeRecordConverter<SchemaRuntimeT, T> tableRecordConverter;
            private final SchemaInputT changeRecordSchema;
            private final Map<String, SchemaInputT> tableSchemas;
            private final Map<String, TupleTag<T>> tableTags;

            private final boolean outputChangeRecord;
            private final boolean outputTableRecord;

            private transient SchemaRuntimeT runtimeChangeRecordSchema;
            private transient Map<String, SchemaRuntimeT> runtimeTableSchemas;

            private ConvertDoFn(final SpannerSourceParameters parameters,
                                final SchemaUtil.SchemaConverter<SchemaInputT, SchemaRuntimeT> schemaConverter,
                                final DataChangeRecordConverter<SchemaRuntimeT, T> changeRecordConverter,
                                final DataChangeRecordConverter<SchemaRuntimeT, T> tableRecordConverter,
                                final SchemaInputT changeRecordSchema,
                                final Map<String, SchemaInputT> tableSchemas,
                                final Map<String, TupleTag<T>> outputTableTags) {

                this.schemaConverter = schemaConverter;
                this.changeRecordConverter = changeRecordConverter;
                this.tableRecordConverter = tableRecordConverter;
                this.changeRecordSchema = changeRecordSchema;
                this.tableSchemas = tableSchemas;
                this.tableTags = outputTableTags;

                this.outputChangeRecord = parameters.getOutputChangeRecord();
                this.outputTableRecord = parameters.getTables().size() > 0;
            }

            @Setup
            public void setup() {
                LOG.info("ConvertDoFn.setup");
                this.runtimeChangeRecordSchema = schemaConverter.convert(changeRecordSchema);
                this.runtimeTableSchemas = this.tableSchemas
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> schemaConverter.convert(e.getValue())));
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final DataChangeRecord record = c.element();
                if(this.outputChangeRecord) {
                    final T output = this.changeRecordConverter.convert(this.runtimeChangeRecordSchema, record);
                    c.output(output);
                }
                switch (record.getModType()) {
                    case UNKNOWN:
                        //case UPDATE:
                        //case INSERT:
                    case DELETE: {
                        return;
                    }
                }
                if(this.outputTableRecord) {
                    final String table = record.getTableName();
                    final SchemaRuntimeT runtimeSchema = this.runtimeTableSchemas.get(table);
                    if(runtimeSchema == null) {
                        LOG.warn("table: " + table + " schema is null");
                        return;
                    }
                    final T output = this.tableRecordConverter.convert(runtimeSchema, record);
                    if(output == null) {
                        LOG.warn("table: " + table + " output is null");
                        return;
                    }
                    if(tableTags.get(table) == null) {
                        LOG.warn("table: " + table + " tag is null");
                        return;
                    }
                    c.output(tableTags.get(table), output);
                }
            }

        }

    }

    private static class SpannerChangeStreamMutationRead extends PTransform<PBegin, PCollection<Mutation>> {

        private final SpannerSourceParameters parameters;
        private final Map<String, Type> tableTypes;

        private SpannerChangeStreamMutationRead(final SpannerSourceParameters parameters, final Map<String, Type> tableTypes) {
            this.parameters = parameters;
            this.tableTypes = tableTypes;
        }

        public PCollection<Mutation> expand(final PBegin begin) {

            final SpannerIO.ReadChangeStream readChangeStream = createDataChangeRecordSource(parameters);
            return begin
                    .apply("ReadChangeStream", readChangeStream)
                    .apply("Reshuffle", Reshuffle.viaRandomKey())
                    .apply("ConvertToMutation",ParDo.of(new ConvertMutationDoFn(tableTypes)));
        }

        private static class ConvertMutationDoFn extends DoFn<DataChangeRecord, Mutation> {
            private static final Logger LOG = LoggerFactory.getLogger(ConvertMutationDoFn.class);

            private final Map<String, Type> tableTypes;

            private ConvertMutationDoFn(final Map<String, Type> tableTypes) {
                this.tableTypes = tableTypes;
            }

            @Setup
            public void setup() {
                LOG.info("ConvertMutationDoFn.setup");
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final DataChangeRecord record = c.element();
                final Type type = tableTypes.get(record.getTableName());
                if(type == null) {
                    return;
                }
                final List<Mutation> mutations = StructSchemaUtil.convertToMutation(type, record);
                for(final Mutation mutation : mutations) {
                    c.output(mutation);
                }
            }

        }

    }

    private static class SpannerChangeStreamMutationGroupRead extends PTransform<PBegin, PCollection<MutationGroup>> {

        private final SpannerSourceParameters parameters;
        private final Map<String, Type> tableTypes;

        private SpannerChangeStreamMutationGroupRead(
                final SpannerSourceParameters parameters,
                final Map<String, Type> tableTypes) {

            this.parameters = parameters;
            this.tableTypes = tableTypes;
        }

        public PCollection<MutationGroup> expand(final PBegin begin) {

            final SpannerIO.ReadChangeStream readChangeStream = createDataChangeRecordSource(parameters);
            return begin
                    .apply("ReadChangeStream", readChangeStream)
                    .apply("WithFixedWindow", Window
                            .<DataChangeRecord>into(FixedWindows.of(Duration.standardSeconds(1L)))
                            .triggering(AfterWatermark.pastEndOfWindow())
                            .accumulatingFiredPanes()
                            .withAllowedLateness(Duration.ZERO))
                    .apply("WithKeyTransaction", WithKeys
                            .of(DataChangeRecord::getServerTransactionId)
                            .withKeyType(TypeDescriptors.strings()))
                    .apply("GroupByTransaction", GroupByKey.create())
                    .apply("ConvertToMutationGroup", ParDo
                            .of(new ConvertMutationGroupDoFn(tableTypes)))
                    .setCoder(SerializableCoder.of(MutationGroup.class));
        }

        private static class ConvertMutationGroupDoFn extends DoFn<KV<String, Iterable<DataChangeRecord>>, MutationGroup> {
            private static final Logger LOG = LoggerFactory.getLogger(ConvertMutationGroupDoFn.class);

            private final Map<String, Type> tableTypes;

            private ConvertMutationGroupDoFn(final Map<String, Type> tableTypes) {
                this.tableTypes = tableTypes;
            }

            @Setup
            public void setup() {
                LOG.info("setup");
            }

            @ProcessElement
            public void processElement(final ProcessContext c) {
                final List<Mutation> mutations = StreamSupport
                        .stream(c.element().getValue().spliterator(), false)
                        .sorted(Comparator.comparing(DataChangeRecord::getRecordSequence))
                        .flatMap(r -> StructSchemaUtil.convertToMutation(tableTypes.get(r.getTableName()), r).stream())
                        .collect(Collectors.toList());

                final MutationGroup mutationGroup;
                if(mutations.size() > 1) {
                    mutationGroup = MutationGroup.create(mutations.get(0), mutations.subList(1, mutations.size()));
                } else {
                    mutationGroup = MutationGroup.create(mutations.get(0));
                }

                c.output(mutationGroup);
            }

        }

    }

    private static class SpannerMicrobatchRead extends PTransform<PCollection<Long>, PCollection<Struct>> {

        private Type type;

        private final String timestampAttribute;
        private final SpannerSourceParameters parameters;

        public SpannerMicrobatchRead(final SourceConfig config, final SpannerSourceParameters parameters) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.parameters = parameters;
        }

        public PCollection<Struct> expand(final PCollection<Long> beat) {

            final String sampleQuery = MicrobatchQuery.createQuery(MicrobatchQuery.createTemplate(
                    parameters.getQuery()), Instant.now(), Instant.now().plus(1));
            this.type = SpannerUtil.getTypeFromQuery(
                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), sampleQuery, false);

            return beat.apply("MicrobatchQuery", MicrobatchQuery.of(
                    parameters.getQuery(),
                    parameters.getStartDatetime(),
                    parameters.getIntervalSecond(),
                    parameters.getGapSecond(),
                    parameters.getMaxDurationMinute(),
                    parameters.getOutputCheckpoint(),
                    parameters.getCatchupIntervalSecond(),
                    parameters.getUseCheckpointAsStartDatetime(),
                    new MicrobatchQueryDoFn(parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), timestampAttribute)
            ));
        }

        private static class MicrobatchQueryDoFn extends DoFn<KV<KV<Integer, KV<Long, Instant>>, String>, Struct> {

            private static final Logger LOG = LoggerFactory.getLogger(MicrobatchQueryDoFn.class);

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final String timestampAttribute;

            private transient Spanner spanner;
            private transient BatchClient client;

            private MicrobatchQueryDoFn(final String projectId, final String instanceId, final String databaseId, final String timestampAttribute) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.timestampAttribute = timestampAttribute;
            }

            @Setup
            public void setup() {
                this.spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, true, false);
                this.client = spanner.getBatchClient(DatabaseId.of(projectId, instanceId, databaseId));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {

                final String query = c.element().getValue();
                final Statement statement = Statement.of(query);
                try(final BatchReadOnlyTransaction transaction = this.client
                        .batchReadOnlyTransaction(TimestampBound.strong())) {

                    final Instant eventTimestamp = c.timestamp();
                    try {
                        final List<Partition> partitions = transaction
                                .partitionQuery(PartitionOptions.newBuilder().build(), statement);

                        int count = 0;
                        final Instant start = Instant.now();
                        for (final Partition partition : partitions) {
                            try (final ResultSet resultSet = transaction.execute(partition)) {
                                while (resultSet.next()) {
                                    final Struct struct = resultSet.getCurrentRowAsStruct();
                                    if(timestampAttribute == null) {
                                        c.output(struct);
                                    } else {
                                        c.outputWithTimestamp(struct, StructSchemaUtil.getTimestamp(struct, timestampAttribute, eventTimestamp));
                                    }
                                    count += 1;
                                }
                            }
                        }
                        final long time = Instant.now().getMillis() - start.getMillis();
                        LOG.info(String.format("Partition Query [%s] divided to [%d] partitions and result num [%d], took [%d] millisec to execute the query.",
                                statement.getSql(), partitions.size(), count, time));
                        c.output(new TupleTag<>("checkpoint"), c.element().getKey());
                    } catch (SpannerException e) {
                        if (!e.getErrorCode().equals(ErrorCode.INVALID_ARGUMENT)) {
                            throw e;
                        }
                        final Instant start = Instant.now();
                        try (final ResultSet resultSet = transaction.executeQuery(statement)) {
                            int count = 0;
                            while (resultSet.next()) {
                                final Struct struct = resultSet.getCurrentRowAsStruct();
                                if(timestampAttribute == null) {
                                    c.output(struct);
                                } else {
                                    c.outputWithTimestamp(struct, StructSchemaUtil.getTimestamp(struct, timestampAttribute, eventTimestamp));
                                }
                                count += 1;
                            }
                            final long time = Instant.now().getMillis() - start.getMillis();
                            LOG.info(String.format("Single query [%s] result num [%d], took [%d] millisec to execute the query.",
                                    statement.getSql(), count, time));
                            c.output(new TupleTag<>("checkpoint"), c.element().getKey());
                        }
                    }
                }
            }

            @Override
            public org.joda.time.Duration getAllowedTimestampSkew() {
                if(timestampAttribute != null) {
                    return org.joda.time.Duration.standardDays(365);
                } else {
                    return super.getAllowedTimestampSkew();
                }
            }

        }

    }

    public static class CreateTransactionFn extends DoFn<Object, Transaction> {

        private static final Logger LOG = LoggerFactory.getLogger(CreateTransactionFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Boolean emulator;
        private final TimestampBound timestampBound;

        public CreateTransactionFn(final String projectId, final String instanceId, final String databaseId, final String timestampBound, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.emulator = emulator;
            this.timestampBound = toTimestampBound(timestampBound);
            LOG.info(String.format("TimestampBound: %s", this.timestampBound.toString()));
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try(final Spanner spanner = SpannerUtil
                    .connectSpanner(projectId, 1, 1, 1, true, this.emulator)) {

                final BatchReadOnlyTransaction tx = spanner
                        .getBatchClient(DatabaseId.of(projectId, instanceId, databaseId))
                        .batchReadOnlyTransaction(timestampBound);
                c.output(Transaction.create(tx.getBatchTransactionId()));
            }
        }
    }

    private static Map<String, FCollection<?>> createRowStream(
            final PBegin begin,
            final String moduleName,
            final SpannerSourceParameters parameters,
            final Map<String, Type> tableTypes) {

        final TupleTag<Row> changeRecordTag = new TupleTag<>();
        final Map<String, TupleTag<Row>> tableTags = tableTypes
                .keySet()
                .stream()
                .collect(Collectors.toMap(e -> e, e -> new TupleTag<>()));
        final Map<String, Schema> outputTableRecordSchemas = tableTypes
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StructToRowConverter.convertSchema(e.getValue())));
        final Map<String, Coder<Row>> coders = outputTableRecordSchemas
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> RowCoder.of(e.getValue())));

        final Schema outputChangeRecordSchema = StructSchemaUtil.createDataChangeRecordRowSchema();
        final SpannerChangeStreamStructRead<Schema, Schema, Row> source = new SpannerChangeStreamStructRead<>(
                parameters, s -> s,
                StructToRowConverter::convertToDataChangeRow, StructToRowConverter::convert,
                outputChangeRecordSchema, outputTableRecordSchemas,
                changeRecordTag, tableTags);
        final PCollectionTuple outputs = begin.apply(moduleName, source);

        final Map<String, FCollection<?>> results = new HashMap<>();

        final PCollection<Row> changeRecord = outputs.get(changeRecordTag);
        final Coder<Row> changeRecordCoder = RowCoder.of(outputChangeRecordSchema);
        results.put(moduleName, FCollection.of(moduleName, changeRecord.setCoder(changeRecordCoder), DataType.ROW, outputChangeRecordSchema));
        for(final Map.Entry<String, TupleTag<Row>> entry : tableTags.entrySet()) {
            final String table = entry.getKey();
            final String name = moduleName + "." + table;
            final TupleTag<Row> tag = tableTags.get(table);
            final PCollection<Row> output = outputs.get(tag);
            final Coder<Row> coder = coders.get(table);
            final Schema outputSchema = outputTableRecordSchemas.get(table);
            if(coder == null || outputSchema == null) {
                LOG.warn("Table not found: [" + table + "]");
                continue;
            }
            results.put(name, FCollection.of(name, output.setCoder(coder), DataType.ROW, outputSchema));
        }

        return results;
    }

    private static Map<String, FCollection<?>> createAvroStream(
            final PBegin begin,
            final String moduleName,
            final SpannerSourceParameters parameters,
            final Map<String, Type> tableTypes) {

        final TupleTag<GenericRecord> changeRecordTag = new TupleTag<>();
        final Map<String, TupleTag<GenericRecord>> tableTags = tableTypes
                .keySet()
                .stream()
                .collect(Collectors.toMap(e -> e, e -> new TupleTag<>()));
        final Map<String, org.apache.avro.Schema> outputTableRecordSchemas = tableTypes
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> StructToRecordConverter.convertSchema(e.getValue())));
        final Map<String, Coder<GenericRecord>> coders = outputTableRecordSchemas
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> AvroCoder.of(e.getValue())));

        final org.apache.avro.Schema outputChangeRecordSchema = StructSchemaUtil.createDataChangeRecordAvroSchema();
        final SpannerChangeStreamStructRead<String, org.apache.avro.Schema, GenericRecord> source = new SpannerChangeStreamStructRead<>(
                parameters, AvroSchemaUtil::convertSchema,
                StructToRecordConverter::convertToDataChangeRecord, StructToRecordConverter::convert,
                outputChangeRecordSchema.toString(), outputTableRecordSchemas.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e->e.getValue().toString())),
                changeRecordTag, tableTags);
        final PCollectionTuple outputs = begin.apply(moduleName, source);

        final Map<String, FCollection<?>> results = new HashMap<>();

        final PCollection<GenericRecord> changeRecord = outputs.get(changeRecordTag);
        final Coder<GenericRecord> changeRecordCoder = AvroCoder.of(outputChangeRecordSchema);
        results.put(moduleName, FCollection.of(moduleName, changeRecord.setCoder(changeRecordCoder), DataType.AVRO, outputChangeRecordSchema));
        for(final Map.Entry<String, TupleTag<GenericRecord>> entry : tableTags.entrySet()) {
            final String table = entry.getKey();
            final String name = moduleName + "." + table;
            final TupleTag<GenericRecord> tag = tableTags.get(table);
            final PCollection<GenericRecord> output = outputs.get(tag);
            final Coder<GenericRecord> coder = coders.get(table);
            final org.apache.avro.Schema outputSchema = outputTableRecordSchemas.get(table);
            if(coder == null || outputSchema == null) {
                LOG.warn("Table not found: [" + table + "]");
                continue;
            }
            results.put(name, FCollection.of(name, output.setCoder(coder), DataType.AVRO, outputSchema));
        }

        return results;
    }

    private static TimestampBound toTimestampBound(final String timestampBoundString) {
        if(timestampBoundString == null) {
            return TimestampBound.strong();
        } else {
            try {
                final Instant instant = Instant.parse(timestampBoundString);
                final com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.ofTimeMicroseconds(instant.getMillis() * 1000);
                return TimestampBound.ofReadTimestamp(timestamp);
            } catch (Exception e) {
                return TimestampBound.strong();
            }
        }
    }

    private interface DataChangeRecordConverter<SchemaT, T> extends Serializable {
        T convert(final SchemaT schema, final DataChangeRecord record);
    }


}
