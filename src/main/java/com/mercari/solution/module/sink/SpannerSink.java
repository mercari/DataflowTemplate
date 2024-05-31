package com.mercari.solution.module.sink;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.spanner.admin.instance.v1.UpdateInstanceMetadata;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.*;
import com.mercari.solution.util.domain.finance.trading.event.FailureMessage;
import com.mercari.solution.util.pipeline.mutation.UnifiedMutation;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class SpannerSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerSink.class);

    private static class SpannerSinkParameters implements Serializable {

        private String projectId;
        private String instanceId;
        private String databaseId;
        private String table;
        private String mutationOp;
        private List<String> keyFields;
        private List<String> commitTimestampFields;
        private Boolean failFast;
        private Boolean flattenFailures;
        private Boolean flattenGroup;

        private Boolean emulator;
        private Long maxNumRows;
        private Long maxNumMutations;
        private Long batchSizeBytes;
        private Integer groupingFactor;
        private Options.RpcPriority priority;

        private List<String> fields;
        private List<String> maskFields;
        private Boolean exclude;

        private Boolean createTable;
        private Boolean emptyTable;
        private String interleavedIn;
        private Boolean cascade;
        private Boolean recreate;
        private Integer nodeCount;
        private Integer revertNodeCount;
        private Integer rebalancingMinite;

        private Boolean isDirectRunner;

        @Deprecated
        private String primaryKeyFields;

        public String getProjectId() {
            return projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public String getTable() {
            return table;
        }

        public String getMutationOp() {
            return mutationOp;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getCommitTimestampFields() {
            return commitTimestampFields;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public Boolean getFlattenFailures() {
            return flattenFailures;
        }

        public Boolean getFlattenGroup() {
            return flattenGroup;
        }

        public Boolean getEmulator() {
            return emulator;
        }

        public Boolean getCreateTable() {
            return createTable;
        }

        public Boolean getEmptyTable() {
            return emptyTable;
        }

        public String getInterleavedIn() {
            return interleavedIn;
        }

        public Boolean getCascade() {
            return cascade;
        }

        public Boolean getRecreate() {
            return recreate;
        }

        public List<String> getFields() {
            return fields;
        }

        public List<String> getMaskFields() {
            return maskFields;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public Long getMaxNumRows() {
            return maxNumRows;
        }

        public Long getMaxNumMutations() {
            return maxNumMutations;
        }

        public Long getBatchSizeBytes() {
            return batchSizeBytes;
        }

        public Integer getGroupingFactor() {
            return groupingFactor;
        }

        public Options.RpcPriority getPriority() {
            return priority;
        }

        public Integer getNodeCount() {
            return nodeCount;
        }

        public Integer getRevertNodeCount() {
            return revertNodeCount;
        }

        public Integer getRebalancingMinite() {
            return rebalancingMinite;
        }

        public Boolean getDirectRunner() {
            return isDirectRunner;
        }

        public String getPrimaryKeyFields() {
            return primaryKeyFields;
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
                    case MUTATION, MUTATIONGROUP, UNIFIEDMUTATION -> {}
                    default -> errorMessages.add("Parameter must contain table");
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

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }
        public void setDefaults(final PInput input) {
            //
            if(this.keyFields == null && this.primaryKeyFields != null) {
                this.keyFields = Arrays
                        .stream(this.getPrimaryKeyFields().split(","))
                        .map(String::trim)
                        .toList();
            }

            //
            if(this.mutationOp == null) {
                this.mutationOp = Mutation.Op.INSERT_OR_UPDATE.name();
            }
            if(this.priority == null) {
                this.priority = Options.RpcPriority.MEDIUM;
            }

            if(this.emulator == null) {
                this.emulator = false;
            }
            if(this.commitTimestampFields == null) {
                this.commitTimestampFields = new ArrayList<>();
            }
            if(this.failFast == null) {
                this.failFast = true;
            }

            if(this.maxNumRows == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L383
                this.maxNumRows = 500L;
            }
            if(this.maxNumMutations == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L381
                this.maxNumMutations = 5000L;
            }
            if(this.batchSizeBytes == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L379
                this.batchSizeBytes = 1024L * 1024L;
            }
            if(this.groupingFactor == null) {
                // https://github.com/apache/beam/blob/v2.49.0/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/SpannerIO.java#L385
                this.groupingFactor = 1000;
            }

            if(this.flattenFailures == null) {
                this.flattenFailures = true;
            }
            if(this.flattenGroup == null) {
                this.flattenGroup = false;
            }

            if(this.createTable == null) {
                this.createTable = false;
            }
            if(this.emptyTable == null) {
                this.emptyTable = false;
            }
            if(this.cascade == null) {
                this.cascade = true;
            }
            if(this.recreate == null) {
                this.recreate = false;
            }
            if(this.exclude == null) {
                this.exclude = false;
            }
            if(this.nodeCount == null) {
                this.nodeCount = -1;
            }
            if(this.rebalancingMinite == null) {
                this.rebalancingMinite = 0;
            }

        }
    }

    public String getName() { return "spanner"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.size() != 1) {
            throw new IllegalArgumentException("spanner sink module requires input parameter");
        }

        final SpannerSinkParameters parameters = new Gson().fromJson(config.getParameters(), SpannerSinkParameters.class);
        if(parameters == null) {
            throw new IllegalArgumentException("spanner sink parameter must not be empty!");
        }

        final FCollection<?> sample = inputs.get(0);
        parameters.setDefaults(sample.getCollection());
        parameters.validate(sample.getCollection(), sample.getDataType(), sample.getIsTuple());

        if(sample.getIsTuple()) {
            final FCollection<?> r = writeTuple(sample, config, parameters, waits);
            return Collections.singletonMap(config.getName(), r);
        } else {
            return switch (sample.getDataType()) {
                case MUTATION -> writeMutations((FCollection<Mutation>) sample, config, parameters, waits);
                case MUTATIONGROUP -> writeMutationGroup((FCollection<MutationGroup>) sample, config, parameters);
                case UNIFIEDMUTATION -> writeUnifiedMutations((FCollection<UnifiedMutation>) sample, config, parameters, waits);
                default -> {
                    final FCollection<?> r = writeSingle(sample, config, parameters, waits);
                    yield Collections.singletonMap(config.getName(), r);
                }
            };
        }
    }

    private static FCollection<?> writeSingle(final FCollection<?> collection,
                                             final SinkConfig config,
                                             final SpannerSinkParameters parameters,
                                             final List<FCollection<?>> waits) {

        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final PCollection output = switch (collection.getDataType()) {
            case AVRO -> {
                final PCollection<GenericRecord> input = (PCollection<GenericRecord>) collection.getCollection();
                final SpannerWriteSingle<String, org.apache.avro.Schema, GenericRecord> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getAvroSchema().toString(),
                        AvroSchemaUtil::convertSchema,
                        RecordToMutationConverter::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            case ROW -> {
                final PCollection<Row> input = (PCollection<Row>) collection.getCollection();
                final SpannerWriteSingle<Schema, Schema, Row> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSchema(),
                        s -> s,
                        RowToMutationConverter::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            case STRUCT -> {
                final PCollection<Struct> input = (PCollection<Struct>) collection.getCollection();
                final SpannerWriteSingle<Schema, Schema, Struct> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSchema(),
                        s -> s,
                        StructToMutationConverter::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            case DOCUMENT -> {
                final PCollection<Document> input = (PCollection<Document>) collection.getCollection();
                final SpannerWriteSingle<Type, Type, Document> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSpannerType(),
                        s -> s,
                        DocumentToMutationConverter::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            case ENTITY -> {
                final PCollection<Entity> input = (PCollection<Entity>) collection.getCollection();
                final SpannerWriteSingle<Type, Type, Entity> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSpannerType(),
                        s -> s,
                        EntityToMutationConverter::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            case MUTATION -> {
                final PCollection<Mutation> input = (PCollection<Mutation>) collection.getCollection();
                final SpannerWriteSingle<Type, Type, Mutation> write = new SpannerWriteSingle<>(
                        collection.getSchema(),
                        parameters,
                        collection.getSpannerType(),
                        s -> s,
                        StructSchemaUtil::convert,
                        waits);
                yield input.apply(config.getName(), write);
            }
            default -> throw new IllegalArgumentException();
        };

        return FCollection.update(collection, output);
    }

    private static FCollection<?> writeTuple(final FCollection<?> collection,
                                            final SinkConfig config,
                                            final SpannerSinkParameters parameters,
                                            final List<FCollection<?>> waits) {

        final SpannerWriteMulti write = new SpannerWriteMulti(collection, parameters, waits);
        final PCollection<?> output = collection.getTuple().apply(config.getName(), write);
        final org.apache.avro.Schema schema = null;
        return FCollection.of(config.getName(), output, DataType.MUTATION, schema);
    }

    private static Map<String, FCollection<?>> writeMutationGroup(
            final FCollection<MutationGroup> collection,
            final SinkConfig config,
            final SpannerSinkParameters parameters) {

        final SpannerWriteMutationGroup write = new SpannerWriteMutationGroup(config.getName(), parameters);
        final PCollectionTuple outputs = collection.getCollection()
                .apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }

        final Schema outputFailureSchema = FailedMutationConvertDoFn.createFailureSchema(parameters.getFlattenFailures());
        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final Map.Entry<String, TupleTag<?>> entry : write.getOutputTags().entrySet()) {
            if(entry.getKey().contains(".")) {
                final PCollection<Row> output = outputs.get((TupleTag<Row>) entry.getValue());
                results.put(entry.getKey(), FCollection.of(entry.getKey(), output, DataType.ROW, outputFailureSchema));
            } else {
                final PCollection<Void> output = outputs.get((TupleTag<Void>) entry.getValue());
                results.put(entry.getKey(), FCollection.of(entry.getKey(), output, DataType.ROW, outputFailureSchema));
            }
        }
        return results;
    }

    private static Map<String, FCollection<?>> writeUnifiedMutations(
            final FCollection<UnifiedMutation> collection,
            final SinkConfig config,
            final SpannerSinkParameters parameters,
            final List<FCollection<?>> waits) {

        final SpannerWriteUnifiedMutations write = new SpannerWriteUnifiedMutations(parameters, waits);
        final PCollectionTuple tuple = collection.getCollection().apply(config.getName(), write);

        final PCollection<Void> output = tuple.get(write.getOutputTag());
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        outputs.put(config.getName(), FCollection.of(config.getName(), output, DataType.UNIFIEDMUTATION, FailureMessage.schema));

        if(!parameters.getFailFast()) {
            final String failuresName = config.getName() + ".failures";
            final PCollection<Row> failures = tuple.get(write.getFailuresTag());
            outputs.put(failuresName, FCollection.of(failuresName, failures, DataType.ROW, FailureMessage.schema));
        }

        return outputs;
    }

    private static Map<String, FCollection<?>> writeMutations(
            final FCollection<Mutation> collection,
            final SinkConfig config,
            final SpannerSinkParameters parameters,
            final List<FCollection<?>> waits) {

        final SpannerWriteMutations write = new SpannerWriteMutations(parameters, waits);
        final PCollectionTuple tuple = collection.getCollection().apply(config.getName(), write);

        final PCollection<Void> output = tuple.get(write.getOutputTag());
        final Map<String, FCollection<?>> outputs = new HashMap<>();
        outputs.put(config.getName(), FCollection.of(config.getName(), output, DataType.MUTATION, FailureMessage.schema));

        if(!parameters.getFailFast()) {
            final String failuresName = config.getName() + ".failures";
            final PCollection<Row> failures = tuple.get(write.getFailuresTag());
            outputs.put(failuresName, FCollection.of(failuresName, failures, DataType.ROW, FailureMessage.schema));
        }

        return outputs;
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
            if(!fields.isEmpty()) {
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
                if(ddl == null || ddl.isEmpty()) {
                    ddls = new ArrayList<>();
                } else {
                    ddls = List.of(ddl);
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
            if(ddls.isEmpty() && parameters.getEmptyTable()) {
                final PCollection<String> wait = mutations
                        .apply("Wait", Sample.any(1))
                        .apply("EmptyTable", ParDo.of(new TableEmptyDoFn<>(projectId, instanceId, databaseId, parameters.getTable(), parameters.getEmulator())));
                mutationTableReady = mutations
                        .apply("WaitToEmptyTable", Wait.on(wait))
                        .setCoder(SerializableCoder.of(Mutation.class));
            } else if(ddls.isEmpty()) {
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
            if((waits != null && !waits.isEmpty()) || parameters.getNodeCount() > 0) {

                final List<PCollection<?>> wait = new ArrayList<>();
                if(waits != null && !waits.isEmpty()) {
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
                    final List<String> primaryKeyFields = keyFields;
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
            if(ddl.isEmpty()) {
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
            while(!parents.isEmpty()) {
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

                if(childrenTags.isEmpty()) {
                    break;
                }

                final SpannerIO.Write write = createWrite(parameters);
                final PCollection<Mutation> mutations = mutationsList
                        .apply("Flatten." + level, Flatten.pCollections());
                final SpannerWriteResult writeResult;
                final PCollection<Void> v;
                if(!waitList.isEmpty()) {
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
            while(!parents.isEmpty()) {
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

                if(ddls.isEmpty()) {
                    break;
                }

                final PCollection<String> ddlResult;
                if(!waitList.isEmpty()) {
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

    public static class SpannerWriteMutationGroup extends PTransform<PCollection<MutationGroup>, PCollectionTuple> {

        private final String name;
        private final SpannerSinkParameters parameters;

        private final Map<String, TupleTag<?>> outputTags;

        public Map<String, TupleTag<?>> getOutputTags() {
            return outputTags;
        }


        SpannerWriteMutationGroup(final String name, final SpannerSinkParameters parameters) {
            this.name = name;
            this.parameters = parameters;

            this.outputTags = new HashMap<>();
        }

        @Override
        public PCollectionTuple expand(PCollection<MutationGroup> input) {

            PCollectionTuple tuple = PCollectionTuple.empty(input.getPipeline());
            final SpannerIO.Write write = createWrite(parameters);
            final PCollection<MutationGroup> withCommitTimestamp;
            if(parameters.getCommitTimestampFields() != null && parameters.getCommitTimestampFields().size() > 0)  {
                withCommitTimestamp = input
                        .apply("WithCommitTimestamp", ParDo.of(new WithCommitTimestampDoFn(parameters.getCommitTimestampFields())));
            } else {
                withCommitTimestamp = input;
            }

            final SpannerWriteResult writeResult;
            if(parameters.getFlattenGroup()) {
                writeResult = withCommitTimestamp
                        .apply("FlattenMutationGroup", ParDo.of(new FlattenGroupDoFn()))
                        .apply("WriteMutation", write);
            } else {
                writeResult = withCommitTimestamp
                        .apply("WriteMutationGroup", write.grouped());
            }

            final TupleTag<Void> tag = new TupleTag<>() {};
            final PCollection<Void> output = writeResult.getOutput();
            tuple = tuple.and(tag, output);
            outputTags.put(name, tag);

            if(!parameters.getFailFast()) {
                final TupleTag<Row> failureTag = new TupleTag<>() {};
                outputTags.put(name + ".failures", failureTag);
                final PCollection<Row> failures = writeResult
                        .getFailedMutations()
                        .apply("ConvertFailures", ParDo.of(new FailedMutationConvertDoFn(
                                parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getFlattenFailures())))
                        .setCoder(RowCoder.of(FailedMutationConvertDoFn.createFailureSchema(parameters.getFlattenFailures())));
                tuple = tuple.and(failureTag, failures);
            }
            return tuple;
        }

        private static class WithCommitTimestampDoFn extends DoFn<MutationGroup, MutationGroup> {

            private final List<String> commitTimestampFields;

            public WithCommitTimestampDoFn(final List<String> commitTimestampFields) {
                this.commitTimestampFields = commitTimestampFields;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MutationGroup input = c.element();
                if(input == null) {
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
                if(Mutation.Op.DELETE.equals(input.getOperation())) {
                    return input;
                } else {
                    return addWriteCommitTimestampFields(input);
                }
            }

            private Mutation addWriteCommitTimestampFields(final Mutation input) {
                final Mutation.WriteBuilder builder = switch (input.getOperation()) {
                    case UPDATE -> Mutation.newUpdateBuilder(input.getTable());
                    case INSERT -> Mutation.newInsertBuilder(input.getTable());
                    case INSERT_OR_UPDATE -> Mutation.newInsertOrUpdateBuilder(input.getTable());
                    case REPLACE -> Mutation.newReplaceBuilder(input.getTable());
                    case DELETE -> throw new IllegalStateException("Not supported writeCommitTimestampFields for DELETE OP");
                };
                for(final Map.Entry<String, Value> entry : input.asMap().entrySet()) {
                    builder.set(entry.getKey()).to(entry.getValue());
                }
                for(final String commitTimestampField : commitTimestampFields) {
                    builder.set(commitTimestampField).to(Value.COMMIT_TIMESTAMP);
                }
                return builder.build();
            }

        }

        private static class FlattenGroupDoFn extends DoFn<MutationGroup, Mutation> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MutationGroup mutationGroup = c.element();
                if(mutationGroup == null) {
                    return;
                }

                final Mutation primary = mutationGroup.primary();
                c.output(primary);

                for(final Mutation attached : mutationGroup.attached()) {
                    c.output(attached);
                }
            }

        }

    }

    public static class SpannerWriteMutations extends PTransform<PCollection<Mutation>, PCollectionTuple> {

        private final SpannerSinkParameters parameters;
        private final List<FCollection<?>> waitsFCollections;

        private final Map<String, Type> types;
        private final Map<String, Set<String>> parentTables;
        private final Map<String, TupleTag<Mutation>> tags;
        private final TupleTag<Void> outputTag;
        private final TupleTag<Row> failureTag;

        public TupleTag<Void> getOutputTag() {
            return this.outputTag;
        }

        public TupleTag<Row> getFailuresTag() {
            return this.failureTag;
        }

        SpannerWriteMutations(
                final SpannerSinkParameters parameters,
                final List<FCollection<?>> waits) {

            this.parameters = parameters;
            this.waitsFCollections = waits;

            this.types = SpannerUtil.getTypesFromDatabase(
                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getEmulator());
            this.parentTables = SpannerUtil.getParentTables(
                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getEmulator());

            LOG.info("parent tables: " + this.parentTables);

            final Set<String> tables = parentTables.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            this.tags = new HashMap<>();
            for(String table : tables) {
                this.tags.put(table, new TupleTag<>(){});
            }

            this.outputTag = new TupleTag<>() {};
            this.failureTag = new TupleTag<>() {};
        }
        @Override
        public PCollectionTuple expand(PCollection<Mutation> input) {

            final List<TupleTag<?>> tagList = new ArrayList<>(tags.values());

            final TupleTag<Mutation> ftag = new TupleTag<>() {};
            final PCollectionTuple tuple = input.apply("WithTableTag", ParDo
                    .of(new WithTableTagDoFn())
                    .withOutputTags(ftag, TupleTagList.of(tagList)));
            List<PCollection<?>> waits;
            if(waitsFCollections == null || waitsFCollections.size() == 0) {
                waits = List.of(input.getPipeline().apply("Create", Create.empty(VoidCoder.of())));
            } else {
                waits = waitsFCollections.stream().map(FCollection::getCollection).collect(Collectors.toList());
            }

            PCollectionList<Void> outputsList = PCollectionList.empty(input.getPipeline());
            PCollectionList<MutationGroup> failuresList = PCollectionList.empty(input.getPipeline());

            int level = 0;
            final Set<String> parents = new HashSet<>();
            parents.add("");
            while(parents.size() > 0) {
                PCollectionList<Mutation> mutationsList = PCollectionList.empty(input.getPipeline());
                Set<String> nextParents = new HashSet<>();
                for(final String parent : parents) {
                    final Set<String> children = parentTables.getOrDefault(parent, new HashSet<>());
                    for(final String child : children) {
                        final TupleTag<Mutation> tag = tags.get(child);
                        final PCollection<Mutation> mutations = tuple.get(tag);
                        mutationsList = mutationsList.and(mutations);
                    }
                    nextParents.addAll(children);
                }

                final SpannerIO.Write write = createWrite(parameters);

                final SpannerWriteResult result = mutationsList
                        .apply("Flatten" + level, Flatten.pCollections())
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Wait" + level, Wait.on(waits))
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Write" + level, write);

                waits = List.of(result.getOutput());
                outputsList = outputsList.and(result.getOutput());
                if(!parameters.getFailFast()) {
                    failuresList = failuresList.and(result.getFailedMutations());
                }

                parents.clear();
                parents.addAll(nextParents);
                level += 1;
            }

            final PCollection<Void> outputs = outputsList.apply("FlattenOutputs", Flatten.pCollections());

            if(parameters.getFailFast()) {
                return PCollectionTuple
                        .of(outputTag, outputs);
            } else {
                final PCollection<Row> failures = failuresList
                        .apply("FlattenFailures", Flatten.pCollections())
                        .apply("ConvertFailures", ParDo.of(new FailedMutationConvertDoFn(
                                parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getFlattenFailures())))
                        .setCoder(RowCoder.of(FailedMutationConvertDoFn.createFailureSchema(parameters.getFlattenFailures())));
                return PCollectionTuple
                        .of(outputTag, outputs)
                        .and(failureTag, failures);
            }
        }

        private class WithTableTagDoFn extends DoFn<Mutation, Mutation> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Mutation mutation = c.element();
                if(mutation == null) {
                    return;
                }

                final String table = mutation.getTable();
                final TupleTag<Mutation> tag = tags.get(table);
                if(tag == null) {
                    LOG.warn("Destination table tag is missing for table: {}, mutation: {}", table, mutation);
                    return;
                }

                final Type type = types.get(table);
                if(type == null) {
                    LOG.warn("Destination table type is missing for table: {}, mutation: {}", table, mutation);
                    return;
                }

                if(StructSchemaUtil.validate(type, mutation)) {
                    c.output(tag, mutation);
                } else {
                    final Mutation adjusted = StructSchemaUtil.adjust(type, mutation);
                    c.output(tag, adjusted);
                }
            }

        }
    }

    public static class SpannerWriteUnifiedMutations extends PTransform<PCollection<UnifiedMutation>, PCollectionTuple> {

        private final SpannerSinkParameters parameters;
        private final List<FCollection<?>> waitsFCollections;

        private final Map<String, Type> types;
        private final Map<String, Set<String>> parentTables;
        private final Map<String, TupleTag<Mutation>> tags;
        private final TupleTag<Void> outputTag;
        private final TupleTag<Row> failureTag;

        public TupleTag<Void> getOutputTag() {
            return this.outputTag;
        }

        public TupleTag<Row> getFailuresTag() {
            return this.failureTag;
        }

        SpannerWriteUnifiedMutations(
                final SpannerSinkParameters parameters,
                final List<FCollection<?>> waits) {

            this.parameters = parameters;
            this.waitsFCollections = waits;

            this.types = SpannerUtil.getTypesFromDatabase(
                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getEmulator());
            this.parentTables = SpannerUtil.getParentTables(
                    parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getEmulator());

            LOG.info("parent tables: " + this.parentTables);

            final Set<String> tables = parentTables.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            this.tags = new HashMap<>();
            for(String table : tables) {
                this.tags.put(table, new TupleTag<>(){});
            }

            this.outputTag = new TupleTag<>() {};
            this.failureTag = new TupleTag<>() {};
        }
        @Override
        public PCollectionTuple expand(PCollection<UnifiedMutation> input) {

            final List<TupleTag<?>> tagList = new ArrayList<>(tags.values());

            final TupleTag<Mutation> ftag = new TupleTag<>() {};
            final PCollectionTuple tuple = input.apply("WithTableTag", ParDo
                    .of(new WithTableTagDoFn())
                    .withOutputTags(ftag, TupleTagList.of(tagList)));
            List<PCollection<?>> waits;
            if(waitsFCollections == null || waitsFCollections.size() == 0) {
                waits = List.of(input.getPipeline().apply("Create", Create.empty(VoidCoder.of())));
            } else {
                waits = waitsFCollections.stream().map(FCollection::getCollection).collect(Collectors.toList());
            }

            PCollectionList<Void> outputsList = PCollectionList.empty(input.getPipeline());
            PCollectionList<MutationGroup> failuresList = PCollectionList.empty(input.getPipeline());

            int level = 0;
            final Set<String> parents = new HashSet<>();
            parents.add("");
            while(!parents.isEmpty()) {
                PCollectionList<Mutation> mutationsList = PCollectionList.empty(input.getPipeline());
                Set<String> nextParents = new HashSet<>();
                for(final String parent : parents) {
                    final Set<String> children = parentTables.getOrDefault(parent, new HashSet<>());
                    for(final String child : children) {
                        final TupleTag<Mutation> tag = tags.get(child);
                        final PCollection<Mutation> mutations = tuple.get(tag);
                        mutationsList = mutationsList.and(mutations);
                    }
                    nextParents.addAll(children);
                }

                final SpannerIO.Write write = createWrite(parameters);

                final SpannerWriteResult result = mutationsList
                        .apply("Flatten" + level, Flatten.pCollections())
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Wait" + level, Wait.on(waits))
                        .setCoder(SerializableCoder.of(Mutation.class))
                        .apply("Write" + level, write);

                waits = List.of(result.getOutput());
                outputsList = outputsList.and(result.getOutput());
                if(!parameters.getFailFast()) {
                    failuresList = failuresList.and(result.getFailedMutations());
                }

                parents.clear();
                parents.addAll(nextParents);
                level += 1;
            }

            final PCollection<Void> outputs = outputsList.apply("FlattenOutputs", Flatten.pCollections());

            if(parameters.getFailFast()) {
                return PCollectionTuple
                        .of(outputTag, outputs);
            } else {
                final PCollection<Row> failures = failuresList
                        .apply("FlattenFailures", Flatten.pCollections())
                        .apply("ConvertFailures", ParDo.of(new FailedMutationConvertDoFn(
                                parameters.getProjectId(), parameters.getInstanceId(), parameters.getDatabaseId(), parameters.getFlattenFailures())))
                        .setCoder(RowCoder.of(FailedMutationConvertDoFn.createFailureSchema(parameters.getFlattenFailures())));
                return PCollectionTuple
                        .of(outputTag, outputs)
                        .and(failureTag, failures);
            }
        }

        private class WithTableTagDoFn extends DoFn<UnifiedMutation, Mutation> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnifiedMutation mutation = c.element();
                if(mutation == null) {
                    return;
                }

                final String table = mutation.getTable();
                final TupleTag<Mutation> tag = tags.get(table.trim());
                if(tag == null) {
                    LOG.warn("Destination table tag is missing for table: " + table + ", mutation: " + mutation + " in tags: " + tags.keySet());
                    return;
                }

                final Type type = types.get(table);
                if(type == null) {
                    LOG.warn("Destination table type is missing for table: " + table + ", mutation: " + mutation + " in types: " + types.keySet());
                    return;
                }

                if(StructSchemaUtil.validate(type, mutation.getSpannerMutation())) {
                    c.output(tag, mutation.getSpannerMutation());
                } else {
                    final Mutation adjusted = StructSchemaUtil.adjust(type, mutation.getSpannerMutation());
                    c.output(tag, adjusted);
                }
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

        write = switch (parameters.getPriority()) {
            case LOW -> write.withLowPriority();
            case HIGH -> write.withHighPriority();
            default -> write;
        };

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
            this.keyFields = keyFields;
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

    private static class FailedMutationConvertDoFn extends DoFn<MutationGroup, Row> {

        private final Schema schema;
        private final Schema childSchema;

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final Boolean flatten;


        FailedMutationConvertDoFn(final String projectId, final String instanceId, final String databaseId, final Boolean flatten) {
            this.schema = createFailureSchema(flatten);
            this.childSchema = createFailureMutationSchema();
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.flatten = flatten;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MutationGroup input = c.element();
            final Instant timestamp = Instant.now();

            if(flatten) {
                final Row primaryFailedRow = convertFailedFlatRow(input.primary(), timestamp);
                c.output(primaryFailedRow);
                for(final Mutation attached : input.attached()) {
                    final Row attachedFailedRow = convertFailedFlatRow(attached, timestamp);
                    c.output(attachedFailedRow);
                }
            } else {
                final List<Row> contents = new ArrayList<>();
                final Mutation primary = input.primary();
                contents.add(convertFailedRow(primary));
                for(final Mutation attached : input.attached()) {
                    contents.add(convertFailedRow(attached));
                }

                final Row output = Row.withSchema(schema)
                        .withFieldValue("timestamp", timestamp)
                        .withFieldValue("project", projectId)
                        .withFieldValue("instance", instanceId)
                        .withFieldValue("database", databaseId)
                        .withFieldValue("mutations", contents)
                        .build();

                c.output(output);
            }
        }

        private Row convertFailedRow(final Mutation mutation) {
            final JsonObject jsonObject = MutationToJsonConverter.convert(mutation);
            return Row.withSchema(childSchema)
                    .withFieldValue("table", mutation.getTable())
                    .withFieldValue("op", mutation.getOperation().name())
                    .withFieldValue("mutation", jsonObject.toString())
                    .build();
        }

        private Row convertFailedFlatRow(final Mutation mutation, final Instant timestamp) {
            final JsonObject jsonObject = MutationToJsonConverter.convert(mutation);
            return Row.withSchema(schema)
                    .withFieldValue("id", UUID.randomUUID().toString())
                    .withFieldValue("timestamp", timestamp)
                    .withFieldValue("project", projectId)
                    .withFieldValue("instance", instanceId)
                    .withFieldValue("database", databaseId)
                    .withFieldValue("table", mutation.getTable())
                    .withFieldValue("op", mutation.getOperation().name())
                    .withFieldValue("mutation", jsonObject.toString())
                    .build();
        }

        public static Schema createFailureSchema(boolean flatten) {
            if(flatten) {
                return Schema.builder()
                        .addField("id", Schema.FieldType.STRING)
                        .addField("timestamp", Schema.FieldType.DATETIME)
                        .addField("project", Schema.FieldType.STRING)
                        .addField("instance", Schema.FieldType.STRING)
                        .addField("database", Schema.FieldType.STRING)
                        .addField("table", Schema.FieldType.STRING.withNullable(true))
                        .addField("op", Schema.FieldType.STRING.withNullable(true))
                        .addField(Schema.Field.of("mutation", Schema.FieldType.STRING).withOptions(Schema.Options.builder().setOption("sqlType", Schema.FieldType.STRING, "JSON").build()))
                        .build();
            } else {
                return Schema.builder()
                        .addField("timestamp", Schema.FieldType.DATETIME)
                        .addField("project", Schema.FieldType.STRING)
                        .addField("instance", Schema.FieldType.STRING)
                        .addField("database", Schema.FieldType.STRING)
                        .addField("mutations", Schema.FieldType.array(Schema.FieldType.row(createFailureMutationSchema())))
                        .build();
            }
        }

        private static Schema createFailureMutationSchema() {
            return Schema.builder()
                    .addField("table", Schema.FieldType.STRING.withNullable(true))
                    .addField("op", Schema.FieldType.STRING.withNullable(true))
                    .addField(Schema.Field.of("mutation", Schema.FieldType.STRING).withOptions(Schema.Options.builder().setOption("sqlType", Schema.FieldType.STRING, "JSON").build()))
                    .build();
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
            if(!this.buffer.isEmpty()) {
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
            if(mutationGroup.attached() != null && !mutationGroup.attached().isEmpty()) {
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
