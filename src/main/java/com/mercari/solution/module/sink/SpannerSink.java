package com.mercari.solution.module.sink;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.gson.Gson;
import com.google.spanner.admin.instance.v1.UpdateInstanceMetadata;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.RowSchemaUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.gcp.SpannerUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SpannerSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSink.class);

    private class SpannerSinkParameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String databaseId;
        private String table;
        private String mutationOp;
        private List<String> keyFields;

        private Boolean emulator;

        private Boolean createTable;
        private Boolean emptyTable;
        private String interleavedIn;
        private Boolean cascade;
        private Boolean recreate;

        private List<String> fields;
        private List<String> maskFields;
        private Boolean exclude;

        private Long maxNumRows;
        private Long maxNumMutations;
        private Long batchSizeBytes;
        private Integer groupingFactor;

        private Integer nodeCount;
        private Integer originalNodeCount;

        private Boolean isDirectRunner;

        @Deprecated
        private String primaryKeyFields;

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

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getMutationOp() {
            return mutationOp;
        }

        public void setMutationOp(String mutationOp) {
            this.mutationOp = mutationOp;
        }

        public Boolean getEmulator() {
            return emulator;
        }

        public void setEmulator(Boolean emulator) {
            this.emulator = emulator;
        }

        public Boolean getCreateTable() {
            return createTable;
        }

        public void setCreateTable(Boolean createTable) {
            this.createTable = createTable;
        }

        public Boolean getEmptyTable() {
            return emptyTable;
        }

        public void setEmptyTable(Boolean emptyTable) {
            this.emptyTable = emptyTable;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields;
        }

        public String getInterleavedIn() {
            return interleavedIn;
        }

        public void setInterleavedIn(String interleavedIn) {
            this.interleavedIn = interleavedIn;
        }

        public Boolean getCascade() {
            return cascade;
        }

        public void setCascade(Boolean cascade) {
            this.cascade = cascade;
        }

        public Boolean getRecreate() {
            return recreate;
        }

        public void setRecreate(Boolean recreate) {
            this.recreate = recreate;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public List<String> getMaskFields() {
            return maskFields;
        }

        public void setMaskFields(List<String> maskFields) {
            this.maskFields = maskFields;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public void setExclude(Boolean exclude) {
            this.exclude = exclude;
        }

        public Long getMaxNumRows() {
            return maxNumRows;
        }

        public void setMaxNumRows(Long maxNumRows) {
            this.maxNumRows = maxNumRows;
        }

        public Long getMaxNumMutations() {
            return maxNumMutations;
        }

        public void setMaxNumMutations(Long maxNumMutations) {
            this.maxNumMutations = maxNumMutations;
        }

        public Long getBatchSizeBytes() {
            return batchSizeBytes;
        }

        public void setBatchSizeBytes(Long batchSizeBytes) {
            this.batchSizeBytes = batchSizeBytes;
        }

        public Integer getGroupingFactor() {
            return groupingFactor;
        }

        public void setGroupingFactor(Integer groupingFactor) {
            this.groupingFactor = groupingFactor;
        }

        public Integer getNodeCount() {
            return nodeCount;
        }

        public void setNodeCount(Integer nodeCount) {
            this.nodeCount = nodeCount;
        }

        public Integer getOriginalNodeCount() {
            return originalNodeCount;
        }

        public void setOriginalNodeCount(Integer originalNodeCount) {
            this.originalNodeCount = originalNodeCount;
        }

        public Boolean getDirectRunner() {
            return isDirectRunner;
        }

        public void setDirectRunner(Boolean directRunner) {
            isDirectRunner = directRunner;
        }

        public String getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public void setPrimaryKeyFields(String primaryKeyFields) {
            this.primaryKeyFields = primaryKeyFields;
        }
    }

    public String getName() { return "spanner"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits) {
        return Collections.singletonMap(config.getName(), SpannerSink.write(input, config, waits));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final SpannerSinkParameters parameters = new Gson().fromJson(config.getParameters(), SpannerSinkParameters.class);
        final SpannerWrite write = new SpannerWrite(collection, parameters, waits);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return FCollection.update(write.collection, output);
    }

    public static class SpannerWrite extends PTransform<PCollection<?>, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWrite.class);

        private FCollection<?> collection;
        private final SpannerSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private SpannerWrite(final FCollection<?> collection,
                             final SpannerSinkParameters parameters,
                             final List<FCollection<?>> waits) {

            this.collection = collection;
            this.parameters = parameters;
            this.waits = waits;
        }

        public PCollection<Void> expand(final PCollection<?> input) {

            setDefaultParameters();
            validateParameters(input.getPipeline().getOptions());

            final String projectId = this.parameters.getProjectId();
            final String instanceId = this.parameters.getInstanceId();
            final String databaseId = this.parameters.getDatabaseId();
            final String mutationOp = this.parameters.getMutationOp();
            final Set<String> fields = OptionUtil.toSet(this.parameters.getFields());
            final Set<String> maskFields = OptionUtil.toSet(this.parameters.getMaskFields());

            final Schema schema = collection.getSchema();

            // create excludeFields
            final Set<String> excludeFields;
            if(fields.size() > 0) {
                if(parameters.getExclude()) {
                    excludeFields = fields;
                } else {
                    excludeFields = schema.getFields().stream()
                            .map(Schema.Field::getName)
                            .collect(Collectors.toSet());
                    excludeFields.removeAll(fields);
                }
            } else {
                excludeFields = new HashSet<>();
            }

            // CreateTable DDLs
            final List<List<String>> ddls;
            if (this.parameters.getCreateTable()) {
                final List<String> ddl = buildDdls(projectId, instanceId, databaseId, parameters.getTable(), schema, parameters.getKeyFields(), excludeFields);
                if(ddl == null || ddl.size() == 0) {
                    ddls = new ArrayList<>();
                } else {
                    ddls = Arrays.asList(ddl);
                }
            } else {
                ddls = new ArrayList<>();
            }


            // SpannerWrite
            final SpannerIO.Write write = SpannerIO.write()
                    //.withHost(SpannerUtil.SPANNER_HOST_BATCH)
                    .withProjectId(projectId)
                    .withInstanceId(instanceId)
                    .withDatabaseId(databaseId)
                    .withMaxNumRows(parameters.getMaxNumRows())
                    .withMaxNumMutations(parameters.getMaxNumMutations())
                    .withBatchSizeBytes(parameters.getBatchSizeBytes())
                    .withGroupingFactor(parameters.getGroupingFactor())
                    .withFailureMode(SpannerIO.FailureMode.FAIL_FAST);

            // For Mutation
            final PCollection<Mutation> mutations = input
                        .apply("ToMutation", DataTypeTransform
                                .spannerMutation(collection, parameters.getTable(), mutationOp, parameters.getKeyFields(), excludeFields, maskFields));

            final PCollection<Mutation> mutationTableReady;
            if(ddls.size() == 0 && parameters.getEmptyTable()) {
                final PCollection<String> wait = mutations
                        .apply("Wait", Sample.any(1))
                        .apply("EmptyTable", ParDo.of(new TableEmptyDoFn<>(projectId, instanceId, databaseId, parameters.getTable(), parameters.getEmulator())));
                mutationTableReady = mutations
                        .apply("WaitToEmptyTable", Wait.on(wait))
                        .setCoder(SerializableCoder.of(Mutation.class));
            } else if(ddls.size() == 0) {
                mutationTableReady = mutations;
            } else {
                final PCollection<String> wait = input.getPipeline()
                        .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                        .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(projectId, instanceId, databaseId, parameters.getEmulator())));
                mutationTableReady = mutations
                        .apply("WaitToTableCreation", Wait.on(wait))
                        .setCoder(SerializableCoder.of(Mutation.class));
            }

            // Custom SpannerWrite for DirectRunner
            if(isDirectRunner(input.getPipeline().getOptions())) {
                if(waits == null) {
                    return mutationTableReady.apply("WriteSpanner", ParDo
                            .of(new SpannerWriteDoFn(projectId, instanceId, databaseId, 500, parameters.getEmulator())));
                } else {
                    final List<PCollection<?>> wait = waits.stream().map(FCollection::getCollection).collect(Collectors.toList());
                    return mutationTableReady
                            .apply("Wait", Wait.on(wait))
                            .setCoder(mutationTableReady.getCoder())
                            .apply("WriteSpanner", ParDo
                                    .of(new SpannerWriteDoFn(projectId, instanceId, databaseId, 500, parameters.getEmulator())));
                }
            }

            final SpannerWriteResult writeResult;
            if((waits != null && waits.size() > 0) || parameters.getNodeCount() > 0) {

                final List<PCollection<?>> wait = new ArrayList<>();
                if(waits != null && waits.size() > 0) {
                    wait.addAll(waits.stream().map(FCollection::getCollection).collect(Collectors.toList()));
                }

                final PCollection<Integer> nodeCount;
                if(parameters.getNodeCount() > 0) {
                    nodeCount = input.getPipeline()
                            .apply("SupplyNodeCount", Create.of(parameters.getNodeCount()))
                            .apply("ScaleUpSpanner", ParDo.of(new SpannerScaleDoFn(
                                    parameters.getProjectId(), parameters.getInstanceId())));
                    wait.add(nodeCount);
                } else {
                    // Never required this value.
                    nodeCount = input.getPipeline().apply("Dummy", Create.of(-1));
                }

                writeResult = mutationTableReady
                        .apply("WaitForSpannerSetup", Wait.on(wait))
                        .setCoder(mutationTableReady.getCoder())
                        .apply("WriteSpanner", write);

                if(parameters.getNodeCount() > 0) {
                    final PCollection<Integer> targetNodeCount;
                    if(parameters.getOriginalNodeCount() != null) {
                        targetNodeCount = input.getPipeline()
                                .apply("SupplyOriginalNodeCount", Create.of(parameters.getOriginalNodeCount()));
                    } else {
                        targetNodeCount = nodeCount;
                    }
                    targetNodeCount
                            .apply("WaitForWriteSpanner", Wait.on(writeResult.getOutput()))
                            .apply("ScaleDownSpanner", ParDo.of(new SpannerScaleDoFn(
                                    parameters.getProjectId(), parameters.getInstanceId())));
                }
            } else {
                writeResult = mutationTableReady.apply("WriteSpanner", write);
            }

            return writeResult.getOutput();
        }

        private void validateParameters(PipelineOptions options) {
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
            if(parameters.getTable() == null) {
                errorMessages.add("Parameter must contain table");
            }
            if(parameters.getCreateTable() && parameters.getKeyFields() == null) {
                errorMessages.add("Parameter must contain primaryKeyFields if createTable");
            }

            if(parameters.getEmulator()) {
                if(!isDirectRunner(options)) {
                    errorMessages.add("If use spanner emulator, Use DirectRunner");
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            //
            if(parameters.getKeyFields() == null && parameters.getPrimaryKeyFields() != null) {
                parameters.setKeyFields(Arrays
                        .stream(parameters.getPrimaryKeyFields().split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()));
            }

            //
            if(parameters.getMutationOp() == null) {
                parameters.setMutationOp(Mutation.Op.INSERT_OR_UPDATE.name());
            }
            if(parameters.getEmulator() == null) {
                parameters.setEmulator(false);
            }
            if(parameters.getCreateTable() == null) {
                parameters.setCreateTable(false);
            }
            if(parameters.getEmptyTable() == null) {
                parameters.setEmptyTable(false);
            }
            if(parameters.getCascade() == null) {
                parameters.setCascade(true);
            }
            if(parameters.getRecreate() == null) {
                parameters.setRecreate(false);
            }
            if(parameters.getExclude() == null) {
                parameters.setExclude(false);
            }

            if(parameters.getMaxNumRows() == null) {
                // https://github.com/apache/beam/blob/v2.23.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L328
                parameters.setMaxNumRows(500L);
            }
            if(parameters.getMaxNumMutations() == null) {
                // https://github.com/apache/beam/blob/v2.23.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L326
                parameters.setMaxNumMutations(5000L);
            }
            if(parameters.getBatchSizeBytes() == null) {
                // https://github.com/apache/beam/blob/v2.23.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L324
                parameters.setBatchSizeBytes(1024L * 1024L);
            }
            if(parameters.getGroupingFactor() == null) {
                // https://github.com/apache/beam/blob/v2.23.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L330
                parameters.setGroupingFactor(1000);
            }
            if(parameters.getNodeCount() == null) {
                parameters.setNodeCount(-1);
            }
        }

        private List<String> buildDdls(final String projectId, final String instanceId, final String databaseId, final String table,
                                       final Schema schema, final List<String> keyFields,
                                       final Set<String> excludeFields) {

            final List<String> ddl = new ArrayList<>();
            final Schema tableSchema = RowSchemaUtil.removeFields(schema, excludeFields);
            final List<String> createTableDdl = buildCreateTableDdl(tableSchema, table, keyFields);
            try(Spanner spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, false, parameters.getEmulator())) {
                if(!SpannerUtil.existsTable(spanner, DatabaseId.of(projectId, instanceId, databaseId), table)) {
                    LOG.info("Set create operation for spanner table: " + table);
                    if(keyFields == null) {
                        throw new IllegalArgumentException("Missing PrimaryKeyFields for creation table: " + table);
                    }
                    final List<String> primaryKeyFields = keyFields;//Arrays.asList(keyFields.split(","));
                    if(primaryKeyFields.stream().anyMatch(f -> !tableSchema.getFieldNames().contains(f))) {
                        throw new IllegalArgumentException(
                                "Missing primaryKeyFields " + keyFields
                                        + " in table: " + table + ", schema: " + tableSchema.toString());
                    }
                    ddl.addAll(createTableDdl);
                } else if(this.parameters.getRecreate()) {
                    LOG.info("Set recreate operation for spanner table: " + table);
                    ddl.add(SpannerUtil.buildDropTableSQL(table));
                    ddl.addAll(createTableDdl);
                } else {
                    LOG.info("Find spanner table: " + table);
                }
            } catch (Exception e) {
                throw e;
            }
            if(ddl.size() == 0) {
                return null;
            }
            return ddl;
        }

        private List<String> buildCreateTableDdl(final Schema schema, final String table,
                                                 final List<String> primaryKeyFields) {

            final List<String> createTableDdl = new ArrayList<>();
            createTableDdl.add(SpannerUtil.buildCreateTableSQL(schema, table,
                    primaryKeyFields, parameters.getInterleavedIn(), parameters.getCascade()));
            final Schema.Options options = schema.getOptions();
            if(options.hasOption("googleStorage")) {
                long size = options.getOptionNames().stream().filter(s -> s.startsWith("spannerIndex_")).count();
                for(long i=0; i<size; i++) {
                    createTableDdl.add(options.getValue(String.format("spannerIndex_%d", i)));
                }
            }
            return createTableDdl;
        }

        private PCollection<String> createWait(final Pipeline pipeline,
                                               final Iterable<List<String>> ddls,
                                               final String projectId,
                                               final String instanceId,
                                               final String databaseId,
                                               final boolean emulator) {
            return pipeline
                    .apply("SupplyDDL", Create.of(ddls))
                    .setCoder(ListCoder.of(StringUtf8Coder.of()))
                    .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(projectId, instanceId, databaseId, emulator)))
                    .setCoder(StringUtf8Coder.of());
        }

        private Boolean isDirectRunner(final PipelineOptions options) {
            return PipelineOptions.DirectRunner.class.getSimpleName().equals(options.getRunner().getSimpleName());
        }

    }

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TablePrepareDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final boolean emulator;

        TablePrepareDoFn(final String projectId, final String instanceId, final String databaseId, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.emulator = emulator;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final List<String> ddl = c.element();
            if(ddl.size() == 0) {
                c.output("ok");
                return;
            }
            try(final Spanner spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, false, emulator)) {
                for(String sql : ddl) {
                    LOG.info("Execute DDL: " + sql);
                    SpannerUtil.executeDdl(spanner, instanceId, databaseId, sql);
                }
                c.output("ok");
            }
        }
    }

    private static class TableEmptyDoFn<InputT> extends DoFn<InputT, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TableEmptyDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final String table;
        private final boolean emulator;

        TableEmptyDoFn(final String projectId, final String instanceId, final String databaseId, final String table, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.table = table;
            this.emulator = emulator;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try(final Spanner spanner = SpannerUtil
                    .connectSpanner(projectId, 1, 1, 1, false, emulator)) {

                final long result = SpannerUtil.emptyTable(spanner, projectId, instanceId, databaseId, table);
                c.output("ok");
            }
        }
    }

    private static class SpannerScaleDoFn extends DoFn<Integer, Integer> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerScaleDoFn.class);

        private final String projectId;
        private final String instanceId;

        public SpannerScaleDoFn(
                final String projectId,
                final String instanceId) {
            this.projectId = projectId;
            this.instanceId = instanceId;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            final Integer nodeCount = c.element();
            if(nodeCount == null || nodeCount <= 0) {
                LOG.info("Not scale spanner instance");
                c.output(-1);
                return;
            }
            final SpannerOptions options = SpannerOptions.newBuilder()
                    .setProjectId(this.projectId)
                    .build();

            final InstanceId instanceId = InstanceId.of(this.projectId, this.instanceId);
            try(final Spanner spanner = options.getService()) {
                final Instance instance = spanner
                        .getInstanceAdminClient().getInstance(instanceId.getInstance());
                final int currentNodeCount = instance.getNodeCount();
                LOG.info("Current spanner instance: " + instanceId.getInstance() + " nodeCount: " + currentNodeCount);
                if(currentNodeCount == nodeCount) {
                    LOG.info("Same spanner instance current and required node count.");
                    c.output(-1);
                    return;
                }

                final OperationFuture<Instance, UpdateInstanceMetadata> meta = spanner
                        .getInstanceAdminClient()
                        .updateInstance(InstanceInfo
                                .newBuilder(instanceId)
                                .setDisplayName(instance.getDisplayName())
                                .setNodeCount(nodeCount)
                                .build());

                meta.get();
                int waitingSeconds = 0;
                while(!meta.isDone()) {
                    Thread.sleep(5 * 1000L);
                    LOG.info("waiting scaling...");
                    waitingSeconds += 5;
                    if(waitingSeconds > 3600) {
                        throw new IllegalArgumentException("DEADLINE EXCEEDED for scale spanner instance!");
                    }
                }
                LOG.info("Scale spanner instance: " + instanceId.getInstance() + " nodeCount: " + nodeCount);

                c.output(currentNodeCount);
            }
        }
    }

    private static class SpannerWriteDoFn extends DoFn<Mutation, Void> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Integer bufferSize;
        private final Boolean emulator;

        private transient Spanner spanner;
        private transient DatabaseClient client;
        private transient List<Mutation> buffer;
        private transient Integer count;

        public SpannerWriteDoFn(final String projectId, final String instanceId, final String databaseId,
                                final Integer bufferSize, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.bufferSize = bufferSize;
            this.emulator = emulator;
        }

        @Setup
        public void setup() {
            this.spanner = SpannerUtil.connectSpanner(projectId, 1, 4, 8, !emulator, emulator);
            this.client = this.spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        }

        @StartBundle
        public void startBundle(StartBundleContext c) {
            this.count = 0;
            this.buffer = new ArrayList<>();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            this.buffer.add(c.element());
            if(this.buffer.size() >= bufferSize) {
                this.client.write(this.buffer);
                this.count += this.buffer.size();
                this.buffer.clear();
                LOG.info("write: " + count);
            }
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext c) {
            if(this.buffer.size() > 0) {
                this.client.write(this.buffer);
                this.count += this.buffer.size();
                LOG.info("write: " + count);
            }
            this.buffer.clear();
        }

        @Teardown
        public void teardown() {
            this.spanner.close();
        }
    }

}
