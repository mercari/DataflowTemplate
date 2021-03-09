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
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SpannerSource implements SourceModule {

    private static final String SQL_SPLITTER = "--SPLITTER--";

    private class SpannerSourceParameters implements Serializable {

        // common
        private String projectId;
        private String instanceId;
        private String databaseId;
        private String query;
        private Boolean emulator;

        // for batch
        private String table;
        private List<String> fields;
        private List<KeyRangeParameter> keyRange;
        private String timestampBound;

        // for microbatch
        private Integer intervalSecond;
        private Integer gapSecond;
        private Integer maxDurationMinute;
        private Integer catchupIntervalSecond;

        private String startDatetime;
        private String outputCheckpoint;
        private Boolean useCheckpointAsStartDatetime;


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

        public Boolean getEmulator() {
            return emulator;
        }

        public void setEmulator(Boolean emulator) {
            this.emulator = emulator;
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
    }

    public String getName() { return "spanner"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {
        if (config.getMicrobatch() != null && config.getMicrobatch()) {
            return Collections.singletonMap(config.getName(), SpannerSource.microbatch(beats, config));
        } else {
            return Collections.singletonMap(config.getName(), SpannerSource.batch(begin, config));
        }
    }

    public static FCollection<Struct> batch(final PBegin begin, final SourceConfig config) {
        final SpannerBatchSource source = new SpannerBatchSource(config);
        final PCollection<Struct> output = begin.apply(config.getName(), source);
        return FCollection.of(config.getName(), output, DataType.STRUCT, source.type);
    }

    public static FCollection<Struct> microbatch(final PCollection<Long> beats, final SourceConfig config) {
        final SpannerMicrobatchRead source = new SpannerMicrobatchRead(config);
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


        private SpannerBatchSource(final SourceConfig config) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
            this.parameters = new Gson().fromJson(config.getParameters(), SpannerSourceParameters.class);
            this.templateArgs = config.getArgs();
            validateParameters();
            setDefaultParameters();
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

                this.type =  SpannerUtil.getTypeFromQuery(projectId, instanceId, databaseId, query, parameters.getEmulator());
                final PCollectionTuple results = begin
                        .apply("SupplyQuery", Create.of(query))
                        .apply("SplitQuery", FlatMapElements.into(TypeDescriptors.strings()).via(s -> Arrays.asList(s.split(SQL_SPLITTER))))
                        .apply("ExecuteQuery", ParDo.of(new QueryPartitionSpannerDoFn(projectId, instanceId, databaseId, timestampBound, parameters.getEmulator(), transactionView))
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
                            startType = "open".equals(keyRangeParameter.getStartType().toLowerCase()) ?
                                    KeyRange.Endpoint.OPEN : KeyRange.Endpoint.CLOSED;
                        }

                        final KeyRange.Endpoint endType;
                        if(keyRangeParameter.getEndType() == null) {
                            endType = KeyRange.Endpoint.CLOSED;
                        } else {
                            endType = "open".equals(keyRangeParameter.getEndType().toLowerCase()) ?
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

                structs = begin
                        .apply("ReadSpannerTable", SpannerIO.read()
                                .withProjectId(projectId)
                                .withInstanceId(instanceId)
                                .withDatabaseId(databaseId)
                                .withTable(table)
                                .withKeySet(keySet)
                                .withColumns(columns)
                                .withBatching(true)
                                .withTimestampBound(toTimestampBound(timestampBound)));
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

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getProjectId() == null) {
                errorMessages.add("Parameter must contain projectId");
            }
            if(parameters.getInstanceId() == null) {
                errorMessages.add("Parameter must contain instanceId");
            }
            if(parameters.getDatabaseId() == null) {
                errorMessages.add("Parameter must contain databaseId");
            }
            if(parameters.getQuery() == null && parameters.getTable() == null) {
                errorMessages.add("Parameter must contain query or table");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(parameters.getEmulator() == null) {
                parameters.setEmulator(false);
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
            private final Boolean emulator;
            private final PCollectionView<Transaction> transactionView;

            private QueryPartitionSpannerDoFn(
                    final String projectId,
                    final String instanceId,
                    final String databaseId,
                    final String timestampBound,
                    final Boolean emulator,
                    final PCollectionView<Transaction> transactionView) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.timestampBound = timestampBound;
                this.emulator = emulator;
                this.transactionView = transactionView;
            }

            @Setup
            public void setup() {
                LOG.info("InitQueryPartitionSpannerDoFn");
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
                        final List<Partition> partitions = transaction.partitionQuery(options, statement);
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
                        try (final ResultSet resultSet = transaction.executeQuery(statement)) {
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
                LOG.info("TeardownQueryPartitionSpannerDoFn");
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
                LOG.info("setupReadStructSpannerDoFn");
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
                LOG.info("TeardownReadStructSpannerDoFn");
            }

        }

    }

    private static class SpannerMicrobatchRead extends PTransform<PCollection<Long>, PCollection<Struct>> {

        private Type type;

        private final String timestampAttribute;
        private final SpannerSourceParameters parameters;

        public SpannerMicrobatchRead(final SourceConfig config) {
            this.timestampAttribute = config.getTimestampAttribute();
            this.parameters = new Gson().fromJson(config.getParameters(), SpannerSourceParameters.class);
            validateParameters();
            setDefaultParameters();
        }

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getProjectId() == null) {
                errorMessages.add("Parameter must contain projectId");
            }
            if(parameters.getInstanceId() == null) {
                errorMessages.add("Parameter must contain instanceId");
            }
            if(parameters.getDatabaseId() == null) {
                errorMessages.add("Parameter must contain databaseId");
            }
            if(parameters.getQuery() == null && parameters.getTable() == null) {
                errorMessages.add("Parameter must contain query or table");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(parameters.getEmulator() == null) {
                parameters.setEmulator(false);
            }
            if(parameters.getIntervalSecond() == null) {
                parameters.setIntervalSecond(60);
            }
            if(parameters.getGapSecond() == null) {
                parameters.setGapSecond(30);
            }
            if(parameters.getMaxDurationMinute() == null) {
                parameters.setMaxDurationMinute(60);
            }
            if(parameters.getCatchupIntervalSecond() == null) {
                parameters.setCatchupIntervalSecond(parameters.getIntervalSecond());
            }
            if(parameters.getUseCheckpointAsStartDatetime() == null) {
                parameters.setUseCheckpointAsStartDatetime(false);
            }
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

}
