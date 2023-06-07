package com.mercari.solution.module.sink;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.spanner.admin.instance.v1.UpdateInstanceMetadata;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.gcp.SpannerUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
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
        private List<String> commitTimestampFields;
        private Boolean failFast;
        private WriteMode writeMode;

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
        private Options.RpcPriority priority;

        private Integer nodeCount;
        private Integer revertNodeCount;
        private Integer rebalancingMinite;

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

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

        public WriteMode getWriteMode() {
            return writeMode;
        }

        public void setWriteMode(WriteMode writeMode) {
            this.writeMode = writeMode;
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

        public List<String> getCommitTimestampFields() {
            return commitTimestampFields;
        }

        public void setCommitTimestampFields(List<String> commitTimestampFields) {
            this.commitTimestampFields = commitTimestampFields;
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

        public Options.RpcPriority getPriority() {
            return priority;
        }

        public void setPriority(Options.RpcPriority priority) {
            this.priority = priority;
        }

        public Integer getNodeCount() {
            return nodeCount;
        }

        public void setNodeCount(Integer nodeCount) {
            this.nodeCount = nodeCount;
        }

        public Integer getRevertNodeCount() {
            return revertNodeCount;
        }

        public void setRevertNodeCount(Integer revertNodeCount) {
            this.revertNodeCount = revertNodeCount;
        }

        public Integer getRebalancingMinite() {
            return rebalancingMinite;
        }

        public void setRebalancingMinite(Integer rebalancingMinite) {
            this.rebalancingMinite = rebalancingMinite;
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

        public void validate(final PInput input, final DataType dataType, final Boolean isTuple) {
            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(this.getProjectId() == null) {
                errorMessages.add("Parameter must contain projectId");
            }
            if(this.getInstanceId() == null) {
                errorMessages.add("Parameter must contain instanceId");
            }
            if(this.getDatabaseId() == null) {
                errorMessages.add("Parameter must contain databaseId");
            }
            if(this.getTable() == null) {
                switch (dataType) {
                    case MUTATION:
                    case MUTATIONGROUP:
                        break;
                    default:
                        errorMessages.add("Parameter must contain table");
                        break;
                }
            }
            if(this.getCreateTable() && this.getKeyFields() == null && !isTuple) {
                errorMessages.add("Parameter must contain primaryKeyFields if createTable is true");
            }

            if(this.getEmulator()) {
                if(!OptionUtil.isDirectRunner(input)) {
                    errorMessages.add("If use spanner emulator, Use DirectRunner");
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }
        public void setDefaults(final PInput input) {
            //
            if(this.keyFields == null && this.primaryKeyFields != null) {
                this.setKeyFields(Arrays
                        .stream(this.getPrimaryKeyFields().split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()));
            }

            //
            if(this.getMutationOp() == null) {
                this.setMutationOp(Mutation.Op.INSERT_OR_UPDATE.name());
            }
            if(this.getFailFast() == null) {
                this.setFailFast(true);
            }
            if(this.getRebalancingMinite() == null) {
                this.setRebalancingMinite(0);
            }
            if(this.getEmulator() == null) {
                this.setEmulator(false);
            }
            if(this.getCreateTable() == null) {
                this.setCreateTable(false);
            }
            if(this.getEmptyTable() == null) {
                this.setEmptyTable(false);
            }
            if(this.getCascade() == null) {
                this.setCascade(true);
            }
            if(this.getRecreate() == null) {
                this.setRecreate(false);
            }
            if(this.getExclude() == null) {
                this.setExclude(false);
            }

            if(this.getMaxNumRows() == null) {
                // https://github.com/apache/beam/blob/v2.42.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L381
                this.setMaxNumRows(500L);
            }
            if(this.getMaxNumMutations() == null) {
                // https://github.com/apache/beam/blob/v2.42.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L379
                this.setMaxNumMutations(5000L);
            }
            if(this.getBatchSizeBytes() == null) {
                // https://github.com/apache/beam/blob/v2.42.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L377
                this.setBatchSizeBytes(1024L * 1024L);
            }
            if(this.getGroupingFactor() == null) {
                // https://github.com/apache/beam/blob/v2.42.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L383
                this.setGroupingFactor(1000);
            }
            if(this.getNodeCount() == null) {
                this.setNodeCount(-1);
            }
            if(this.getPriority() == null) {
                this.setPriority(Options.RpcPriority.MEDIUM);
            }
            if(this.writeMode == null) {
                this.writeMode = WriteMode.normal;
            }
        }
    }

    private enum WriteMode {
        normal,
        simple
    }

    public String getName() { return "spanner"; }

    public Map<String, FCollection<?>> expand(FCollection<?> input, SinkConfig config, List<FCollection<?>> waits, List<FCollection<?>> sideInputs) {
        return Collections.singletonMap(config.getName(), write(input, config, waits, sideInputs));
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits, final List<FCollection<?>> sideInputs) {
        final SpannerSinkParameters parameters = new Gson().fromJson(config.getParameters(), SpannerSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
        }
        parameters.setDefaults(collection.getCollection());
        parameters.validate(collection.getCollection(), collection.getDataType(), collection.getIsTuple());

        if(DataType.MUTATIONGROUP.equals(collection.getDataType())) {
            return writeMutationGroup((FCollection<MutationGroup>) collection, config, parameters);
        } else if(collection.getIsTuple()) {
            return writeMulti(collection, config, parameters, waits, sideInputs);
        } else {
            return writeSingle(collection, config, parameters, waits, sideInputs);
        }
    }

    public static FCollection<?> writeSingle(final FCollection<?> collection,
                                             final SinkConfig config,
                                             final SpannerSinkParameters parameters,
                                             final List<FCollection<?>> waits,
                                             final List<FCollection<?>> sideInputs) {

        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final PCollection output;
        switch (collection.getDataType()) {
            case AVRO: {
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final SpannerWriteSingle<String, org.apache.avro.Schema, GenericRecord> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getAvroSchema().toString(),
                        AvroSchemaUtil::convertSchema,
                        RecordToMutationConverter::convert,
                        waits);
                output = input.apply(config.getName(), write);
                break;
            }
            case ROW: {
                final PCollection<Row> input = (PCollection<Row>) collection.getCollection();
                final SpannerWriteSingle<Schema, Schema, Row> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSchema(),
                        s -> s,
                        RowToMutationConverter::convert,
                        waits);
                output = input.apply(config.getName(), write);
                break;
            }
            case STRUCT: {
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final SpannerWriteSingle<Schema, Schema, Struct> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSchema(),
                        s -> s,
                        StructToMutationConverter::convert,
                        waits);
                output = input.apply(config.getName(), write);
                break;
            }
            case ENTITY: {
                final PCollection<Entity> input = (PCollection<Entity>) collection.getCollection();
                final SpannerWriteSingle<Type, Type, Entity> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSpannerType(),
                        s -> s,
                        EntityToMutationConverter::convert,
                        waits);
                output = input.apply(config.getName(), write);
                break;
            }
            case MUTATION: {
                final PCollection<Mutation> input = (PCollection<Mutation>) collection.getCollection();
                final SpannerWriteSingle<Type, Type, Mutation> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSpannerType(),
                        s -> s,
                        StructSchemaUtil::convert,
                        waits);
                output = input.apply(config.getName(), write);
                break;
            }
            default: {
                throw new IllegalArgumentException();
            }
        }

        return FCollection.update(collection, output);
    }

    public static FCollection<?> writeMulti(final FCollection<?> collection,
                                            final SinkConfig config,
                                            final SpannerSinkParameters parameters,
                                            final List<FCollection<?>> waits,
                                            final List<FCollection<?>> sideInputs) {

        final SpannerWriteMulti write = new SpannerWriteMulti(collection, parameters, waits);
        final PCollection<?> output = collection.getTuple().apply(config.getName(), write);
        final org.apache.avro.Schema schema = null;
        return FCollection.of(config.getName(), output, DataType.MUTATION, schema);
    }

    public static FCollection<?> writeMutationGroup(final FCollection<MutationGroup> collection,
                                                    final SinkConfig config,
                                                    final SpannerSinkParameters parameters) {

        final SpannerWriteMutationGroup write = new SpannerWriteMutationGroup(config.getName(), parameters);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return FCollection.update(collection, output);
    }

    public static class SpannerWriteSingle<InputSchemaT,RuntimeSchemaT,T> extends PTransform<PCollection<T>, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteSingle.class);

        private final SpannerSinkParameters parameters;
        private final List<FCollection<?>> waits;

        private final Schema schema;

        private final InputSchemaT inputSchema;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SpannerMutationConverter<RuntimeSchemaT, T> converter;

        private SpannerWriteSingle(final Schema schema,
                                   final SpannerSinkParameters parameters,
                                   final InputSchemaT inputSchema,
                                   final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                   final SpannerMutationConverter<RuntimeSchemaT, T> converter,
                                   final List<FCollection<?>> waits) {

            this.parameters = parameters;
            this.waits = waits;

            this.schema = schema;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.converter = converter;
        }

        public PCollection<Void> expand(final PCollection<T> input) {
            final String projectId = this.parameters.getProjectId();
            final String instanceId = this.parameters.getInstanceId();
            final String databaseId = this.parameters.getDatabaseId();
            final Set<String> fields = OptionUtil.toSet(this.parameters.getFields());
            final Set<String> maskFields = OptionUtil.toSet(this.parameters.getMaskFields());

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
            final SpannerIO.Write write = createWrite(parameters);

            final PCollection<Mutation> mutations = input
                    .apply("ToMutation", ParDo.of(new SpannerMutationDoFn<>(
                            parameters.getTable(), parameters.getMutationOp(), parameters.getKeyFields(), parameters.getCommitTimestampFields(),
                            excludeFields, maskFields, inputSchema, schemaConverter, converter)))
                    .setCoder(SerializableCoder.of(Mutation.class));

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
            if(OptionUtil.isDirectRunner(input)) {
                if(waits == null) {
                    return mutationTableReady.apply("WriteSpanner", ParDo
                            .of(new WriteMutationDoFn(projectId, instanceId, databaseId, 500, parameters.getEmulator())));
                } else {
                    final List<PCollection<?>> wait = waits.stream().map(FCollection::getCollection).collect(Collectors.toList());
                    return mutationTableReady
                            .apply("Wait", Wait.on(wait))
                            .setCoder(mutationTableReady.getCoder())
                            .apply("WriteSpanner", ParDo
                                    .of(new WriteMutationDoFn(projectId, instanceId, databaseId, 500, parameters.getEmulator())));
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
                                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getRebalancingMinite())));
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
                    if(parameters.getRevertNodeCount() != null) {
                        targetNodeCount = input.getPipeline()
                                .apply("SupplyRevertNodeCount", Create.of(parameters.getRevertNodeCount()));
                    } else {
                        targetNodeCount = nodeCount;
                    }
                    targetNodeCount
                            .apply("WaitForWriteSpanner", Wait.on(writeResult.getOutput()))
                            .apply("ScaleDownSpanner", ParDo.of(new SpannerScaleDoFn(
                                    parameters.getProjectId(), parameters.getInstanceId(), 0)));
                }
            } else {
                writeResult = mutationTableReady.apply("WriteSpanner", write);
            }

            return writeResult.getOutput();
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

    }

    public static class SpannerWriteMulti extends PTransform<PCollectionTuple, PCollection<Void>> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteMulti.class);

        private final Map<TupleTag<?>, String> tagNames;
        private final Map<TupleTag<?>, DataType> dataTypes;
        private final Map<TupleTag<?>, String> avroSchemaStrings;
        private final SpannerSinkParameters parameters;
        private final List<PCollection<?>> waits;

        private SpannerWriteMulti(final FCollection<?> collection,
                             final SpannerSinkParameters parameters,
                             final List<FCollection<?>> waits) {

            this.tagNames = collection.getAvroSchemas()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getName()));
            this.dataTypes = collection.getDataTypes();
            this.avroSchemaStrings = collection.getAvroSchemas()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().toString()));
            this.parameters = parameters;
            this.waits = waits == null ? new ArrayList<>() : waits
                    .stream()
                    .map(f -> f.getCollection())
                    .collect(Collectors.toList());
        }

        public PCollection<Void> expand(final PCollectionTuple input) {

            final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas = avroSchemaStrings
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey(),
                            e -> AvroSchemaUtil.convertSchema(e.getValue())));
            final Map<String, TupleTag<?>> tags = tagNames
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getValue,
                            Map.Entry::getKey));
            final Map<TupleTag<?>, List<TupleTag<?>>> pedigree = calcPedigree(tags, avroSchemas);

            final List<PCollection<?>> waitList = new ArrayList<>();
            if(parameters.getCreateTable()) {
                final PCollection<String> ddls = executeDDLs(input.getPipeline(), waits, parameters, pedigree, avroSchemas);
                waitList.add(ddls);
            } else {
                waitList.addAll(waits);
            }

            final List<PCollection<Void>> outputs = new ArrayList<>();
            final List<TupleTag<?>> parents = new ArrayList<>();
            parents.add(null);
            int level = 1;
            while(parents.size() > 0) {
                PCollectionList<Mutation> mutationsList = PCollectionList.empty(input.getPipeline());
                final List<TupleTag<?>> childrenTags = new ArrayList<>();
                for(final TupleTag<?> parent : parents) {
                    final List<TupleTag<?>> children = pedigree.getOrDefault(parent, Collections.emptyList());
                    for(final TupleTag<?> child : children) {
                        final String name = tagNames.get(child);
                        final DataType dataType = dataTypes.get(child);
                        final org.apache.avro.Schema avroSchema = avroSchemas.get(child);
                        final PCollection<?> collection = input.get(child);
                        final FCollection<?> fCollection = FCollection.of(name, collection, dataType, avroSchema);
                        final PCollection<Mutation> mutations = collection
                                .apply("ToMutation." + name, DataTypeTransform.spannerMutation(
                                        fCollection, parameters.getTable(), "",
                                        parameters.getKeyFields(), null, null));

                        mutationsList = mutationsList.and(mutations);
                        childrenTags.add(child);
                    }
                }

                if(childrenTags.size() == 0) {
                    break;
                }

                final SpannerIO.Write write = createWrite(parameters);
                final PCollection<Mutation> mutations = mutationsList
                        .apply("Flatten." + level, Flatten.pCollections());
                final SpannerWriteResult writeResult;
                final PCollection<Void> v;
                if(waitList.size() > 0) {
                    writeResult = mutations
                            .apply("Wait." + level, Wait.on(waitList))
                            .apply("Write." + level, write);
                } else {
                    writeResult = mutations
                            .apply("Write." + level, write);
                }

                parents.clear();
                waitList.clear();
                parents.addAll(childrenTags);
                waitList.add(writeResult.getOutput());

                outputs.add(writeResult.getOutput());
                level += 1;
            }

            PCollectionList<Void> outputsList = PCollectionList.empty(input.getPipeline());
            for(final PCollection<Void> output : outputs) {
                outputsList = outputsList.and(output);
            }

            return outputsList
                    .apply("FlattenOutput", Flatten.pCollections())
                    .setCoder(VoidCoder.of());
        }

        private Map<TupleTag<?>, List<TupleTag<?>>> calcPedigree(
                final Map<String, TupleTag<?>> tagNames,
                final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas) {

            final Map<TupleTag<?>, List<TupleTag<?>>> pedigree = new HashMap<>();
            for(final Map.Entry<TupleTag<?>, org.apache.avro.Schema> entry : avroSchemas.entrySet()) {
                final org.apache.avro.Schema schema = entry.getValue();
                final String childName = schema.getName();
                final String parentName = schema.getProp("spannerParent");
                final TupleTag<?> childTag = tagNames.get(childName);
                final TupleTag<?> parentTag = tagNames.get(parentName);
                if(!pedigree.containsKey(parentTag)) {
                    pedigree.put(parentTag, new ArrayList<>());
                }
                if(childTag != null) {
                    pedigree.get(parentTag).add(childTag);
                }
            }
            return pedigree;
        }

        private PCollection<String> executeDDLs(
                final Pipeline pipeline,
                final List<PCollection<?>> waits,
                final SpannerSinkParameters parameters,
                final Map<TupleTag<?>, List<TupleTag<?>>> pedigree,
                final Map<TupleTag<?>, org.apache.avro.Schema> avroSchemas) {

            final List<PCollection<String>> outputs = new ArrayList<>();

            final List<PCollection<?>> waitList = new ArrayList<>(waits);
            final List<TupleTag<?>> parents = new ArrayList<>();
            parents.add(null);
            int level = 1;
            while(parents.size() > 0) {
                final List<String> ddls = new ArrayList<>();
                final List<TupleTag<?>> childrenTags = new ArrayList<>();
                for(final TupleTag<?> parent : parents) {
                    final List<TupleTag<?>> children = pedigree.getOrDefault(parent, Collections.emptyList());
                    for(final TupleTag<?> child : children) {
                        final org.apache.avro.Schema avroSchema = avroSchemas.get(child);
                        ddls.add(avroSchema.toString());
                        childrenTags.add(child);
                    }
                }

                if(ddls.size() == 0) {
                    break;
                }

                final PCollection<String> ddlResult;
                if(waitList.size() > 0) {
                    ddlResult = pipeline
                            .apply("SupplyDDL." + level, Create.of(KV.of("", ddls)).withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))))
                            .apply("WaitDDL." + level, Wait.on(waitList))
                            .apply("ExecuteDDL." + level, ParDo.of(new DDLDoFn(
                                    parameters.getProjectId(),
                                    parameters.getInstanceId(),
                                    parameters.getDatabaseId(),
                                    parameters.getEmulator())));
                } else {
                    ddlResult = pipeline
                            .apply("SupplyDDL." + level, Create.of(KV.of("", ddls)).withCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of()))))
                            .apply("ExecuteDDL." + level, ParDo.of(new DDLDoFn(
                                    parameters.getProjectId(),
                                    parameters.getInstanceId(),
                                    parameters.getDatabaseId(),
                                    parameters.getEmulator())));

                }

                parents.clear();
                waitList.clear();
                parents.addAll(childrenTags);
                waitList.add(ddlResult);

                outputs.add(ddlResult);
                level += 1;
            }

            return PCollectionList
                    .of(outputs)
                    .apply("Flatten", Flatten.pCollections());
        }

        private static class DDLDoFn extends DoFn<KV<String,List<String>>, String> {

            private final String projectId;
            private final String instanceId;
            private final String databaseId;
            private final Boolean emulator;

            private transient Spanner spanner;

            DDLDoFn(final String projectId, final String instanceId, final String databaseId, final Boolean emulator) {
                this.projectId = projectId;
                this.instanceId = instanceId;
                this.databaseId = databaseId;
                this.emulator = emulator;
            }


            @Setup
            public void setup() {
                this.spanner = SpannerUtil.connectSpanner(
                        projectId, 1, 1, 1, false, emulator);
            }
            @ProcessElement
            public void processElement(final ProcessContext c) {
                final List<String> schemaStrings = c.element().getValue();
                final List<String> ddls = new ArrayList<>();
                for(final String schemaString : schemaStrings) {
                    LOG.info("Table avro schema: " + schemaString);
                    final org.apache.avro.Schema schema = AvroSchemaUtil.convertSchema(schemaString);
                    ddls.addAll(createDDLs(schema));
                }
                LOG.info("Execute DDLs: " + ddls);
                SpannerUtil.executeDDLs(spanner, instanceId, databaseId, ddls, 3600, 5);

                c.output("ok");
            }

            @Teardown
            public void teardown() {
                this.spanner.close();
            }

            private List<String> createDDLs(org.apache.avro.Schema schema) {
                if(schema == null) {
                    throw new IllegalArgumentException("avro schema must not be null for creating spanner tables");
                }
                final String table = schema.getName();
                final String parent = schema.getProp("spannerParent");
                final String onDeleteAction = schema.getProp("spannerOnDeleteAction");
                final String primaryKey = schema.getProp("spannerPrimaryKey");

                final Map<Integer, String> primaryKeys = new TreeMap<>();
                final Map<Integer, String> indexes = new TreeMap<>();
                final Map<Integer, String> foreignKeys = new TreeMap<>();
                final Map<Integer, String> checkConstraints = new TreeMap<>();
                for(final Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
                    if(entry.getKey().startsWith("spannerPrimaryKey_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerPrimaryKey_", ""));
                        primaryKeys.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerIndex_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerIndex_", ""));
                        indexes.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerForeignKey_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerForeignKey_", ""));
                        foreignKeys.put(n, entry.getValue().toString());
                    } else if(entry.getKey().startsWith("spannerCheckConstraint_")) {
                        final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerCheckConstraint_", ""));
                        checkConstraints.put(n, entry.getValue().toString());
                    }
                }

                final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE %s ( %n", table));
                for(final org.apache.avro.Schema.Field field : schema.getFields()) {
                    final String sqlType = field.getProp("sqlType");
                    final String generationExpression = field.getProp("generationExpression");

                    final String defaultExpression = field.getProp("defaultExpression");
                    final String stored;
                    if(generationExpression != null) {
                        stored = field.getProp("stored");
                    } else {
                        stored = null;
                    }

                    final Map<Integer, String> fieldOptions = new TreeMap<>();
                    for(final Map.Entry<String, Object> entry : field.getObjectProps().entrySet()) {
                        if(entry.getKey().startsWith("spannerOption_")) {
                            final Integer n = Integer.valueOf(entry.getKey().replaceFirst("spannerOption_", ""));
                            fieldOptions.put(n, entry.getValue().toString());
                        }
                    }

                    final String columnExpression = String.format("`%s` %s%s%s%s,%n",
                            field.name(),
                            sqlType,
                            AvroSchemaUtil.isNullable(field.schema()) ? "" : " NOT NULL",
                            defaultExpression == null ? (stored == null ? "" : " AS ("+ stored +") STORED") : " DEFAULT (" + defaultExpression +")",
                            fieldOptions.size() == 0 ? "" : " OPTIONS (" + String.join(",", fieldOptions.values()) + ")");

                    sb.append(columnExpression);
                }

                for(String foreignKey : foreignKeys.values()) {
                    final String foreignKeyExpression = String.format("%s,%n", foreignKey);
                    sb.append(foreignKeyExpression);
                }
                for(String checkConstraint : checkConstraints.values()) {
                    final String checkConstraintExpression = String.format("%s,%n", checkConstraint);
                    sb.append(checkConstraintExpression);
                }

                //sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                if(primaryKey != null) {
                    sb.append(String.format(" PRIMARY KEY ( %s )", primaryKey));
                }
                if(parent != null) {
                    sb.append(",");
                    sb.append("INTERLEAVE IN PARENT ");
                    sb.append(parent);
                    sb.append(String.format(" ON DELETE %s", "cascade".equalsIgnoreCase(onDeleteAction) ? "CASCADE" : "NO ACTION"));
                }

                final List<String> ddls = new ArrayList<>();
                ddls.add(sb.toString());
                ddls.addAll(indexes.values());
                return ddls;
            }

        }

    }

    public static class SpannerWriteMutationGroup extends PTransform<PCollection<MutationGroup>, PCollection<Void>> {

        private final String name;
        private final SpannerSinkParameters parameters;

        SpannerWriteMutationGroup(final String name, final SpannerSinkParameters parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        @Override
        public PCollection<Void> expand(PCollection<MutationGroup> input) {
            final SpannerIO.Write write = createWrite(parameters);
            switch (parameters.getWriteMode()) {
                case simple: {
                    return input
                            .apply("WriteSimpleMutationGroup", ParDo
                                    .of(new WriteMutationGroupDoFn(
                                            name, parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), false)));
                }
                case normal:
                default: {
                    final SpannerWriteResult writeResult;
                    if(parameters.getCommitTimestampFields() != null && parameters.getCommitTimestampFields().size() > 0)  {
                        writeResult = input
                                .apply("WithCommitTimestampFields", ParDo.of(new WithCommitTimestampDoFn(parameters.getCommitTimestampFields())))
                                .apply("WriteMutationGroup", write.grouped());
                    } else {
                        writeResult = input
                                .apply("WriteMutationGroup", write.grouped());
                    }
                    return writeResult.getOutput();
                }
            }
        }

        private class WithCommitTimestampDoFn extends DoFn<MutationGroup, MutationGroup> {

            private final List<String> commitTimestampFields;

            public WithCommitTimestampDoFn(final List<String> commitTimestampFields) {
                this.commitTimestampFields = commitTimestampFields;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MutationGroup input = c.element();
                if(Mutation.Op.DELETE.equals(input.primary().getOperation())) {
                    c.output(input);
                    return;
                }
                final Mutation withCommitTimestampPrimary = addCommitTimestampFields(input.primary());
                final List<Mutation> withCommitTimestampAttachedList = new ArrayList<>();
                for(final Mutation attached : input.attached()) {
                    final Mutation withCommitTimestampAttached = addCommitTimestampFields(attached);
                    withCommitTimestampAttachedList.add(withCommitTimestampAttached);
                }
                final MutationGroup output = MutationGroup.create(withCommitTimestampPrimary, withCommitTimestampAttachedList);
                c.output(output);
            }

            private Mutation addCommitTimestampFields(final Mutation input) {
                final Mutation.WriteBuilder builder;
                switch (input.getOperation()) {
                    case UPDATE:
                        builder = Mutation.newUpdateBuilder(input.getTable());
                        break;
                    case INSERT:
                        builder = Mutation.newInsertBuilder(input.getTable());
                        break;
                    case INSERT_OR_UPDATE:
                        builder = Mutation.newInsertOrUpdateBuilder(input.getTable());
                        break;
                    case REPLACE:
                        builder = Mutation.newReplaceBuilder(input.getTable());
                        break;
                    case DELETE:
                    default:
                        return input;
                }
                for(final Map.Entry<String, Value> entry : input.asMap().entrySet()) {
                    builder.set(entry.getKey()).to(entry.getValue());
                }
                for(final String commitTimestampField : commitTimestampFields) {
                    builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);                }
                return builder.build();
            }

        }

    }

    private static SpannerIO.Write createWrite(final SpannerSinkParameters parameters) {
        SpannerIO.Write write = SpannerIO.write()
                .withProjectId(parameters.getProjectId())
                .withInstanceId(parameters.getInstanceId())
                .withDatabaseId(parameters.getDatabaseId())
                .withMaxNumRows(parameters.getMaxNumRows())
                .withMaxNumMutations(parameters.getMaxNumMutations())
                .withBatchSizeBytes(parameters.getBatchSizeBytes())
                .withGroupingFactor(parameters.getGroupingFactor());

        if(parameters.getFailFast()) {
            write = write.withFailureMode(SpannerIO.FailureMode.FAIL_FAST);
        } else {
            write = write.withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES);
        }

        switch (parameters.getPriority()) {
            case LOW: {
                write = write.withLowPriority();
                break;
            }
            case HIGH: {
                write = write.withHighPriority();
                break;
            }
        }

        return write;
    }

    private static class SpannerMutationDoFn<InputSchemaT,RuntimeSchemaT,T> extends DoFn<T, Mutation> {

        private final String table;
        private final String mutationOp;
        private final List<String> keyFields;
        private final List<String> allowCommitTimestampFields;
        private final Set<String> excludeFields;
        private final Set<String> maskFields;

        private final InputSchemaT inputSchema;
        private final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter;
        private final SpannerMutationConverter<RuntimeSchemaT, T> converter;

        private transient RuntimeSchemaT runtimeSchema;

        private SpannerMutationDoFn(final String table,
                                    final String mutationOp,
                                    final List<String> keyFields,
                                    final List<String> allowCommitTimestampFields,
                                    final Set<String> excludeFields,
                                    final Set<String> maskFields,
                                    final InputSchemaT inputSchema,
                                    final SchemaUtil.SchemaConverter<InputSchemaT, RuntimeSchemaT> schemaConverter,
                                    final SpannerMutationConverter<RuntimeSchemaT, T> converter) {
            this.table = table;
            this.mutationOp = mutationOp;
            this.keyFields = keyFields;// == null ? null : Arrays.asList(keyFields.split(","));
            this.allowCommitTimestampFields = allowCommitTimestampFields;
            this.excludeFields = excludeFields;
            this.maskFields = maskFields;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.converter = converter;
        }

        @Setup
        public void setup() {
            if(this.inputSchema == null || this.schemaConverter == null) {
                this.runtimeSchema = null;
            } else {
                this.runtimeSchema = this.schemaConverter.convert(inputSchema);
            }
        }

        @ProcessElement
        public void processElement(final @Element T input, final OutputReceiver<Mutation> receiver) {
            final Mutation mutation = converter.convert(runtimeSchema, input, table, mutationOp, keyFields, allowCommitTimestampFields, excludeFields, maskFields);
            receiver.output(mutation);
        }

    }

    public interface SpannerMutationConverter<SchemaT, InputT> extends Serializable {
        Mutation convert(final SchemaT schema,
                         final InputT element,
                         final String table,
                         final String mutationOp,
                         final Iterable<String> keyFields,
                         final List<String> allowCommitTimestampFields,
                         final Set<String> excludeFields,
                         final Set<String> maskFields);
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
        private final Integer rebalancingMinite;

        public SpannerScaleDoFn(
                final String projectId,
                final String instanceId,
                final Integer rebalancingMinite) {

            this.projectId = projectId;
            this.instanceId = instanceId;
            this.rebalancingMinite = rebalancingMinite;
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

                int waitingMinutes = 0;
                while(waitingMinutes < rebalancingMinite) {
                    Thread.sleep(60 * 1000L);
                    LOG.info("waiting rebalancing minute: " + waitingMinutes + "/" + rebalancingMinite);
                    waitingMinutes += 1;
                }
                LOG.info("finished waiting.");
                c.output(currentNodeCount);
            }
        }
    }

    private static class WriteMutationDoFn extends DoFn<Mutation, Void> {

        private static final Logger LOG = LoggerFactory.getLogger(WriteMutationDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Integer bufferSize;
        private final Boolean emulator;

        private transient Spanner spanner;
        private transient DatabaseClient client;
        private transient List<Mutation> buffer;
        private transient Integer count;

        public WriteMutationDoFn(final String projectId, final String instanceId, final String databaseId,
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

    private static class WriteMutationGroupDoFn extends DoFn<MutationGroup, Void> {

        private final String name;
        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Boolean emulator;

        private transient Spanner spanner;
        private transient DatabaseClient client;

        WriteMutationGroupDoFn(final String name,
                               final String projectId,
                               final String instanceId,
                               final String databaseId,
                               final Boolean emulator) {

            this.name = name;
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.emulator = emulator;
        }


        @Setup
        public void setup() {
            LOG.info("SpannerSink: " + name + " setup");
            this.spanner = SpannerUtil
                    .connectSpanner(projectId, 1, 1, 1, !emulator, emulator);
            this.client = this.spanner
                    .getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        }

        @ProcessElement
        public void processElement(final ProcessContext c){
            final MutationGroup mutationGroup = c.element();
            final List<Mutation> mutations = new ArrayList<>();
            if(mutationGroup == null) {
                return;
            }
            mutations.add(mutationGroup.primary());
            if(mutationGroup.attached() != null && mutationGroup.attached().size() > 0) {
                mutations.addAll(mutationGroup.attached());
            }
            this.client.writeAtLeastOnce(mutations);
        }

        @Teardown
        public void teardown() {
            LOG.info("SpannerSink: " + name + " teardown");
            this.spanner.close();
        }

    }

}
