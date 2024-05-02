package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.domain.search.Neo4jUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.Neo4jSchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class LocalNeo4jTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(LocalNeo4jTransform.class);

    public static class LocalNeo4jTransformParameters implements Serializable {

        private List<String> groupFields;
        private IndexDefinition index;
        private List<QueryDefinition> queries;

        public List<String> getGroupFields() {
            return groupFields;
        }

        public IndexDefinition getIndex() {
            return index;
        }

        public List<QueryDefinition> getQueries() {
            return queries;
        }


        public static LocalNeo4jTransformParameters of(
                final JsonElement jsonElement,
                final String name,
                final List<String> inputNames,
                final List<Schema> inputSchemas) {

            final LocalNeo4jTransformParameters parameters = new Gson().fromJson(jsonElement, LocalNeo4jTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("LocalNeo4jTransform config parameters must not be empty!");
            }

            parameters.validate(name, inputNames);
            parameters.setDefaults(inputNames, inputSchemas);

            return parameters;
        }

        public List<String> validate(final String name, final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.index == null) {
                errorMessages.add("localNeo4j[" + name + "].index must not be null.");
            } else {
                errorMessages.addAll(this.index.validate(name, inputNames));
            }
            if(this.queries == null || this.queries.size() == 0) {
                errorMessages.add("localNeo4j.queries must not be null or size zero.");
            } else {
                for(int i=0; i<queries.size(); i++) {
                    errorMessages.addAll(queries.get(i).validate(name, i, inputNames));
                }
            }
            return errorMessages;
        }

        public void setDefaults(final List<String> inputNames, final List<Schema> inputSchemas) {
            if(this.groupFields == null) {
                this.groupFields = new ArrayList<>();
            }
            this.index.setDefaults();
            for(final QueryDefinition queryDefinition : this.queries) {
                int index = inputNames.indexOf(queryDefinition.getInput());
                queryDefinition.setDefaults(inputSchemas.get(index));
            }
        }

    }

    public static class IndexDefinition implements Serializable {

        private String path;
        private String database;
        private String conf;
        private List<Neo4jUtil.NodeConfig> nodes;
        private List<Neo4jUtil.RelationshipConfig> relationships;
        private List<String> setupCyphers;
        private List<String> teardownCyphers;
        private Boolean useGDS;
        private Boolean mutable;
        private Integer bufferSize;

        public String getPath() {
            return path;
        }

        public String getDatabase() {
            return database;
        }

        public String getConf() {
            return conf;
        }

        public List<Neo4jUtil.NodeConfig> getNodes() {
            return nodes;
        }

        public List<Neo4jUtil.RelationshipConfig> getRelationships() {
            return relationships;
        }

        public List<String> getSetupCyphers() {
            return setupCyphers;
        }

        public List<String> getTeardownCyphers() {
            return teardownCyphers;
        }

        public Boolean getUseGDS() {
            return useGDS;
        }

        public Boolean getMutable() {
            return mutable;
        }

        public Integer getBufferSize() {
            return bufferSize;
        }

        public List<String> validate(final String name, final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.path == null) {
                errorMessages.add("localNeo4j[" + name + "].index.path must not be null.");
            }
            if((nodes == null || nodes.isEmpty()) && (relationships == null || relationships.isEmpty())) {
                errorMessages.add("localNeo4j[" + name + "] transform module requires `nodes` or `relationships` parameter.");
            } else {
                if(nodes != null) {
                    for(int i=0; i<nodes.size(); i++) {
                        errorMessages.addAll(nodes.get(i).validate(i));
                        if(!inputNames.contains(nodes.get(i).getInput())) {
                            errorMessages.add("localNeo4j[" + name + "].nodes[" + i + "].input does not exist in module inputs: " + inputNames);
                        }
                    }
                }
                if(relationships != null) {
                    for(int i=0; i<relationships.size(); i++) {
                        errorMessages.addAll(relationships.get(i).validate(i));
                        if(!inputNames.contains(relationships.get(i).getInput())) {
                            errorMessages.add("localNeo4j[" + name + "].relationships[" + i + "].input does not exist in module inputs: " + inputNames);
                        }
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(database == null) {
                database = Neo4jUtil.DEFAULT_DATABASE_NAME;
            }
            if(nodes == null) {
                nodes = new ArrayList<>();
            } else {
                nodes.forEach(Neo4jUtil.NodeConfig::setDefaults);
            }
            if(relationships == null) {
                relationships = new ArrayList<>();
            } else {
                relationships.forEach(Neo4jUtil.RelationshipConfig::setDefaults);
            }
            if(setupCyphers == null) {
                setupCyphers = new ArrayList<>();
            }
            if(teardownCyphers == null) {
                teardownCyphers = new ArrayList<>();
            }
            if(useGDS == null) {
                useGDS = false;
            }
            if(mutable == null) {
                mutable = nodes.isEmpty() && relationships.isEmpty();
            }
            if(bufferSize == null) {
                bufferSize = 500;
            }
        }

    }

    public static class QueryDefinition implements Serializable {

        private String name;
        private String input;
        private List<String> fields;
        private String cypher;
        private SourceConfig.InputSchema schema;
        private List<String> requiredFields;
        private transient Template cypherTemplate;

        public String getName() {
            return name;
        }

        public String getInput() {
            return input;
        }

        public List<String> getFields() {
            return fields;
        }

        public String getCypher() {
            return cypher;
        }

        public SourceConfig.InputSchema getSchema() {
            return schema;
        }

        public List<String> getRequiredFields() {
            return requiredFields;
        }

        public Template getCypherTemplate() {
            return cypherTemplate;
        }

        public List<String> validate(String name, int i, final List<String> inputNames) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.name == null) {
                errorMessages.add("localNeo4j[" + name + "].queries[" + i + "].name must not be null.");
            }
            if(this.input == null) {
                errorMessages.add("localNeo4j[" + name + "].queries[" + i + "].input must not be null.");
            } else if(!inputNames.contains(this.input)) {
                errorMessages.add("localNeo4j[" + name + "].queries[" + i + "].input does not exists in module inputs: " + inputNames);
            }
            if(this.cypher == null) {
                errorMessages.add("localNeo4j[" + name + "].queries[" + i + "].cypher must not be null.");
            }
            if(this.schema == null) {
                errorMessages.add("localNeo4j[" + name + "].queries[" + i + "].schema must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults(final Schema inputSchema) {
            if(fields == null) {
                fields = new ArrayList<>();
            }
            requiredFields = new ArrayList<>(this.fields);
            final List<String> cypherTemplateArgs = TemplateUtil.extractTemplateArgs(cypher, inputSchema);
            for(final String arg : cypherTemplateArgs) {
                if(!requiredFields.contains(arg)) {
                    requiredFields.add(arg);
                }
            }
        }

        public void setup() {
            this.cypherTemplate = TemplateUtil.createStrictTemplate(name + "Cypher", cypher);
        }

    }

    @Override
    public String getName() {
        return "localNeo4j";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();
        final List<Schema> inputSchemas = new ArrayList<>();

        final DataType outputType = OptionUtil.isStreaming(inputs.get(0).getCollection()) ? DataType.ROW : DataType.AVRO;

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            final TupleTag tag = new TupleTag<>() {};
            tags.add(tag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            inputSchemas.add(input.getSchema());
            tuple = tuple.and(tag, input.getCollection());
        }

        final LocalNeo4jTransformParameters parameters = LocalNeo4jTransformParameters.of(
                config.getParameters(),
                config.getName(),
                inputNames,
                inputSchemas);

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        switch (outputType) {
            case ROW -> {
                final Transform<Schema, Row> transform = new Transform<>(
                        config.getName(),
                        parameters,
                        s -> s,
                        RowSchemaUtil::create,
                        Neo4jSchemaUtil::convert,
                        Instant::ofEpochMilli,
                        tags,
                        inputNames,
                        inputTypes,
                        inputSchemas);

                final PCollectionTuple output = tuple.apply(config.getName(), transform);
                final Map<TupleTag<Row>, Schema> outputSchemas = transform.getOutputSchemas();
                for (final Map.Entry<TupleTag<Row>, String> entry : transform.getOutputNames().entrySet()) {
                    final String outputName = config.getName() + "." + entry.getValue();
                    final Schema outputSchema = outputSchemas.get(entry.getKey());
                    final PCollection<Row> pCollection = output.get(entry.getKey());
                    final FCollection<?> fCollection = FCollection.of(outputName, pCollection.setCoder(RowCoder.of(outputSchema)), DataType.ROW, outputSchema);
                    outputs.put(outputName, fCollection);
                    if(transform.getOutputNames().size() == 1) {
                        outputs.put(config.getName(), fCollection);
                    }
                }
            }
            case AVRO -> {
                final Transform<org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                        config.getName(),
                        parameters,
                        RowToRecordConverter::convertSchema,
                        AvroSchemaUtil::create,
                        Neo4jSchemaUtil::convert,
                        (long millis) -> millis * 1000L,
                        tags,
                        inputNames,
                        inputTypes,
                        inputSchemas);

                final PCollectionTuple output = tuple.apply(config.getName(), transform);
                final Map<TupleTag<GenericRecord>, Schema> outputSchemas = transform.getOutputSchemas();
                for (final Map.Entry<TupleTag<GenericRecord>, String> entry : transform.getOutputNames().entrySet()) {
                    final String outputName = config.getName() + "." + entry.getValue();
                    final org.apache.avro.Schema outputSchema = RowToRecordConverter.convertSchema(outputSchemas.get(entry.getKey()));
                    final PCollection<GenericRecord> pCollection = output.get(entry.getKey());
                    final FCollection<GenericRecord> fCollection = FCollection.of(outputName, pCollection.setCoder(AvroCoder.of(outputSchema)), DataType.AVRO, outputSchema);
                    outputs.put(outputName, fCollection);
                    if(transform.getOutputNames().size() == 1) {
                        outputs.put(config.getName(), fCollection);
                    }
                }
            }
            default -> throw new IllegalArgumentException("Not supported outputType: " + outputType);
        }

        return outputs;
    }

    public static class Transform<RuntimeSchemaT, T> extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String name;
        private final List<String> groupFields;
        private final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator;
        private final TimestampConverter timestampConverter;

        private final IndexDefinition index;
        private final List<QueryDefinition> queries;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;

        //
        private final Map<TupleTag<T>, String> outputNames;
        private final Map<TupleTag<T>, Schema> outputSchemas;
        private final TupleTag<T> outputFailureTag;
        private final Schema outputFailureSchema;

        public Map<TupleTag<T>, String> getOutputNames() {
            return outputNames;
        }

        public Map<TupleTag<T>, Schema> getOutputSchemas() {
            return outputSchemas;
        }

        Transform(final String name,
                  final LocalNeo4jTransformParameters parameters,
                  final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                  final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                  final TimestampConverter timestampConverter,
                  final List<TupleTag<?>> inputTags,
                  final List<String> inputNames,
                  final List<DataType> inputTypes,
                  final List<Schema> inputSchemas) {

            this.name = name;
            this.groupFields = parameters.getGroupFields();
            this.schemaConverter = schemaConverter;
            this.valueCreator = valueCreator;
            this.neo4jValueCreator = neo4jValueCreator;
            this.timestampConverter = timestampConverter;

            this.index = parameters.getIndex();
            this.queries = parameters.getQueries();

            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;

            final Map<String, Schema> inputSchemasMap = new HashMap<>();
            for(int i=0; i<Math.min(inputNames.size(), inputSchemas.size()); i++) {
                inputSchemasMap.put(inputNames.get(i), inputSchemas.get(i));
            }
            final List<KV<TupleTag<T>, KV<String, Schema>>> outputNameAndTagsAndSchemas = createOutputTagsAndSchemas(
                    parameters.getQueries(), inputSchemasMap);
            this.outputNames = outputNameAndTagsAndSchemas.stream()
                    .collect(Collectors.toMap(KV::getKey, kv -> kv.getValue().getKey()));
            this.outputSchemas = outputNameAndTagsAndSchemas.stream()
                    .collect(Collectors.toMap(KV::getKey, kv -> kv.getValue().getValue()));

            this.outputFailureTag = new TupleTag<>() {};
            this.outputFailureSchema = createFailureSchema();
            this.outputNames.put(this.outputFailureTag, "failures");
            this.outputSchemas.put(this.outputFailureTag, this.outputFailureSchema);
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple inputs) {

            final Map<String, TupleTag<T>> outputTags = outputNames.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            final Map<String, Schema> outputInputSchemas = new HashMap<>();
            for(final Map.Entry<TupleTag<T>, String> entry : outputNames.entrySet()) {
                outputInputSchemas.put(entry.getValue(), outputSchemas.get(entry.getKey()));
            }

            final String processName;
            final DoFn<KV<String, UnionValue>, T> dofn;
            if(index.getMutable()) {
                processName = "Query";
                dofn = new Neo4jQueryDoFn(name,
                        inputNames, outputInputSchemas, index, queries,
                        schemaConverter, valueCreator, neo4jValueCreator, timestampConverter);
            } else {
                processName = "IndexAndQuery";
                final Coder<UnionValue> unionCoder = Union.createUnionCoder(inputs, inputTags);
                if(OptionUtil.isStreaming(inputs)) {
                    dofn = new Neo4jQueryAndIndexStreamingDoFn(name,
                            inputNames, outputInputSchemas, index, queries,
                            schemaConverter, valueCreator, neo4jValueCreator, timestampConverter,
                            unionCoder);
                } else {
                    dofn = new Neo4jQueryAndIndexBatchDoFn(name,
                            inputNames, outputInputSchemas, index, queries,
                            schemaConverter, valueCreator, neo4jValueCreator, timestampConverter,
                            unionCoder);
                }
            }

            final TupleTagList tupleTagList = TupleTagList.of(outputTags.values().stream()
                    .filter(t -> !t.getId().equals(outputFailureTag.getId()))
                    .collect(Collectors.toList()));

            return inputs
                    .apply("Union", Union.withKey(inputTags, inputTypes, groupFields, inputNames))
                    .apply("WithWindow", Window
                            .<KV<String, UnionValue>>into(new GlobalWindows())
                            .triggering(Repeatedly
                                    .forever(AfterPane.elementCountAtLeast(1)))
                            .withTimestampCombiner(TimestampCombiner.LATEST)
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.ZERO))
                    .apply(processName, ParDo.of(dofn)
                            .withOutputTags(outputFailureTag, tupleTagList));
        }

        private List<KV<TupleTag<T>, KV<String, Schema>>> createOutputTagsAndSchemas(
                final List<QueryDefinition> queries,
                final Map<String, Schema> inputSchemas) {

            final List<KV<TupleTag<T>, KV<String,Schema>>> outputs = new ArrayList<>();
            for(final QueryDefinition query : queries) {
                final TupleTag<T> tag = new TupleTag<>(){};
                final Schema resultSchema = SourceConfig.convertSchema(query.getSchema());
                final Schema outputSchema = createOutputSchema(query, inputSchemas.get(query.getInput()), resultSchema);
                outputs.add(KV.of(tag, KV.of(query.getName(), outputSchema)));
            }
            return outputs;
        }

        private Schema createOutputSchema(final QueryDefinition query, final Schema inputSchema, final Schema resultSchema) {
            Schema.Builder builder = Schema.builder();
            for(final String field : query.getFields()) {
                builder = builder.addField(field, inputSchema.getField(field).getType().withNullable(true));
            }
            return builder
                    .addField("cypher", Schema.FieldType.STRING.withNullable(true))
                    .addField("results", Schema.FieldType.array(Schema.FieldType.row(resultSchema)).withNullable(true))
                    .addField("timestamp", Schema.FieldType.DATETIME)
                    .build();
        }

        private Schema createFailureSchema() {
            return Schema.builder()
                    .addField("input", Schema.FieldType.STRING.withNullable(true))
                    .addField("type", Schema.FieldType.STRING.withNullable(true))
                    .addField("cypher", Schema.FieldType.STRING.withNullable(true))
                    .addField("message", Schema.FieldType.STRING.withNullable(true))
                    .addField("timestamp", Schema.FieldType.DATETIME)
                    .build();
        }


        private class Neo4jDoFn extends DoFn<KV<String,UnionValue>, T> {

            private static final String NEO4J_HOME = "/neo4j/";

            private final String name;
            private final String indexPath;
            private final List<String> inputNames;
            private final Map<String, Schema> outputSchemas;
            private final IndexDefinition index;
            private final List<QueryDefinition> queries;
            private final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator;
            private final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator;
            private final TimestampConverter timestampConverter;

            private static GraphDatabaseService graphDB;
            private transient Map<String, RuntimeSchemaT> outputRuntimeSchemas;
            private transient Map<String, RuntimeSchemaT> outputResultRuntimeSchemas;
            private transient RuntimeSchemaT outputFailureRuntimeSchema;

            Neo4jDoFn(final String name,
                      final List<String> inputNames,
                      final Map<String, Schema> outputSchemas,
                      final IndexDefinition index,
                      final List<QueryDefinition> queries,
                      final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                      final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                      final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                      final TimestampConverter timestampConverter) {

                this.name = name;
                this.indexPath = NEO4J_HOME + name + "/";
                this.inputNames = inputNames;
                this.outputSchemas = outputSchemas;
                this.index = index;
                this.queries = queries;
                this.schemaConverter = schemaConverter;
                this.valueCreator = valueCreator;
                this.neo4jValueCreator = neo4jValueCreator;
                this.timestampConverter = timestampConverter;
            }

            synchronized protected void setupIndex() throws IOException {
                setupIndex(this.indexPath, this.index);
            }

            protected void setupQuery() {
                this.outputRuntimeSchemas = this.outputSchemas.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> schemaConverter.convert(e.getValue())));
                this.outputResultRuntimeSchemas = this.outputSchemas.entrySet().stream()
                        .filter(e -> e.getValue().hasField("results"))
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> schemaConverter.convert(e.getValue().getField("results").getType().getCollectionElementType().getRowSchema())));
                this.outputFailureRuntimeSchema = schemaConverter.convert(this.outputSchemas.get("failures"));
                for(final QueryDefinition queryDefinition : queries) {
                    queryDefinition.setup();
                }
            }

            synchronized protected void teardownIndex() throws IOException {
                if(!index.getTeardownCyphers().isEmpty()) {
                    try(final Transaction tx = graphDB.beginTx()) {
                        for(final String teardownCypher : index.getTeardownCyphers()) {
                            final Result result = tx.execute(teardownCypher);
                            LOG.info("teardown cypher query: " + teardownCypher + ". result: " + result.resultAsString());
                        }
                        tx.commit();
                    }
                }

                //ZipFileUtil.uploadZipFile(this.indexPath, index.path);
                final ByteArrayOutputStream os = new ByteArrayOutputStream();
                Neo4jUtil.dump(this.indexPath, index.getDatabase(), os);
                os.flush();
                os.close();
                StorageUtil.writeBytes(this.index.getPath(), os.toByteArray(), "application/zstd", new HashMap<>(), new HashMap<>());
            }

            protected void query(final ProcessContext c) {
                final UnionValue unionValue = c.element().getValue();
                final Instant timestamp = c.timestamp();
                final String inputName = inputNames.get(unionValue.getIndex());
                try(final Transaction tx = graphDB.beginTx()){
                    for(final QueryDefinition query : queries) {
                        if(!inputName.equals(query.getInput())) {
                            continue;
                        }

                        final Map<String, Object> input = unionValue.getMap(query.getRequiredFields());
                        final String cypher = TemplateUtil.executeStrictTemplate(query.getCypherTemplate(), input);

                        try(final Result result = tx.execute(cypher)) {
                            final TupleTag<T> outputTag = outputNames.entrySet().stream().filter(e -> e.getValue().equals(query.getName())).map(e -> e.getKey()).findAny().get();
                            final T output = createOutput(query, input, cypher, result, timestamp.getMillis());
                            c.output(outputTag, output);
                        } catch (final Throwable e) {
                            final String message = "Failed to execute cypher: " + cypher + ". cause: " + e.getMessage();
                            LOG.error(message);
                            final T outputFailure = createFailure(inputName, "query", cypher, timestamp.getMillis(), e);
                            c.output(outputFailure);
                        }
                    }
                } catch (final Throwable e) {
                    LOG.error("Failed to begin transaction cause: " + e.getMessage());
                    final T outputFailure  = createFailure(inputName, "query", null, timestamp.getMillis(), e);
                    c.output(outputFailure);
                }
            }

            protected void index(final List<UnionValue> buffer) {
                Neo4jUtil.index(graphDB, buffer, index.getNodes(), index.getRelationships(), inputNames);
                buffer.clear();
            }

            protected boolean isQueryInput(UnionValue input) {
                for(final QueryDefinition query : queries) {
                    if(inputNames.get(input.getIndex()).equals(query.getInput())) {
                        return true;
                    }
                }
                return false;
            }

            private T createOutput(QueryDefinition query, Map<String, Object> input, String cypher, Result result, long epochMillis) {
                // create results
                final RuntimeSchemaT outputResultSchema = outputResultRuntimeSchemas.get(query.getName());
                final List<T> outputResults = new ArrayList<>();
                while(result.hasNext()) {
                    final Map<String, Object> resultValues = result.next();
                    final T outputResult = neo4jValueCreator.create(outputResultSchema, resultValues);
                    outputResults.add(outputResult);
                }

                // create output
                final Map<String, Object> outputValues = new HashMap<>();
                for(final String field : query.getFields()) {
                    outputValues.put(field, input.get(field));
                }
                outputValues.put("cypher", cypher);
                outputValues.put("results", outputResults);
                outputValues.put("timestamp", timestampConverter.toTimestampValue(epochMillis));
                final RuntimeSchemaT outputSchema = outputRuntimeSchemas.get(query.getName());
                return valueCreator.create(outputSchema, outputValues);
            }

            private T createFailure(final String input, final String type, final String cypher, final long epochMillis, final Throwable e) {
                final Map<String, Object> values = new HashMap<>();
                values.put("input", input);
                values.put("type", type);
                values.put("cypher", cypher);
                values.put("message", e.getMessage());
                for(final StackTraceElement stackTraceElement : e.getStackTrace()) {

                }
                values.put("timestamp", timestampConverter.toTimestampValue(epochMillis));
                return this.valueCreator.create(outputFailureRuntimeSchema, values);
            }

            static synchronized protected void setupIndex(String indexPath, IndexDefinition index) throws IOException {

                LOG.info("Start setup database PID: " + ProcessHandle.current().pid() + ", ThreadID:" + Thread.currentThread().getId());

                final Path indexDirPath = Paths.get(indexPath);
                final File indexDir = indexDirPath.toFile();
                if(!indexDir.exists()) {
                    indexDir.mkdir();
                    if(StorageUtil.exists(index.getPath())) {
                        Neo4jUtil.load(indexPath, index.getDatabase(), index.getPath());
                        //ZipFileUtil.downloadZipFiles(index.getPath(), indexPath);
                        LOG.info("Load Neo4j initial database file from: " + index.getPath() + " to " + indexPath);
                    } else if(index.getPath() != null) {
                        LOG.warn("Not found Neo4j initial database file: " + index.getPath());
                    }
                }

                if(graphDB == null) {
                    LOG.info("Start neo4j database");
                    final DatabaseManagementService service = new DatabaseManagementServiceBuilder(indexDirPath).build();
                    graphDB = service.database(index.getDatabase());
                    if(index.getUseGDS()) {
                        Neo4jUtil.setupGds(graphDB);
                    }
                    if(!index.getSetupCyphers().isEmpty()) {
                        try(final Transaction tx = graphDB.beginTx()) {
                            for(final String setupCypher : index.getSetupCyphers()) {
                                final Result result = tx.execute(setupCypher);
                                LOG.info("setup cypher query: " + setupCypher + ". result: " + result.resultAsString());
                            }
                            tx.commit();
                        }
                    }
                    Neo4jUtil.registerShutdownHook(service);
                }

                LOG.info("Finish setup database");
            }

            static synchronized protected void teardownIndex(String indexPath, IndexDefinition index) {
                if(graphDB != null) {
                    graphDB = null;
                }
            }

        }

        private class Neo4jQueryDoFn extends Neo4jDoFn {

            Neo4jQueryDoFn(final String name,
                           final List<String> inputNames,
                           final Map<String, Schema> outputSchemas,
                           final IndexDefinition index,
                           final List<QueryDefinition> queries,
                           final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                           final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                           final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                           final TimestampConverter timestampConverter) {

                super(name, inputNames, outputSchemas,
                        index, queries,
                        schemaConverter, valueCreator, neo4jValueCreator, timestampConverter);
            }

            @Setup
            synchronized public void setup() throws IOException {
                super.setupIndex();
                super.setupQuery();
            }

            @Teardown
            public void teardown() {

            }

            @ProcessElement
            public void processElement(final ProcessContext c) {

                final UnionValue input = c.element().getValue();
                if(isQueryInput(input)) {
                    query(c);
                } else {
                    LOG.info("not query input: " + input + " for inputNames: " + inputNames);
                }
            }

        }

        private class Neo4jQueryAndIndexDoFn extends Neo4jDoFn {

            protected static final String STATE_ID_INDEX_BUFFER = "indexBuffer";
            protected static final String STATE_ID_INTERVAL_COUNTER = "intervalCounter";

            Neo4jQueryAndIndexDoFn(final String name,
                                   final List<String> inputNames,
                                   final Map<String, Schema> outputSchemas,
                                   final IndexDefinition index,
                                   final List<QueryDefinition> queries,
                                   final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                                   final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                                   final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                                   final TimestampConverter timestampConverter) {

                super(name, inputNames, outputSchemas,
                        index, queries,
                        schemaConverter, valueCreator, neo4jValueCreator, timestampConverter);
            }

            protected void setup() throws IOException {
                super.setupIndex();
                super.setupQuery();
            }

            protected void teardown() throws IOException {
                //super.uploadIndex();
            }

            protected void processElement(final ProcessContext c,
                                          final ValueState<List<UnionValue>> bufferIndexBufferValueState,
                                          final ValueState<Integer> bufferUpdateIntervalValueState) {

                final List<UnionValue> buffer = Optional
                        .ofNullable(bufferIndexBufferValueState.read())
                        .orElseGet(ArrayList::new);

                final UnionValue input = c.element().getValue();
                buffer.add(input);

                final boolean isQueryInput = isQueryInput(input);

                // indexing
                if(isQueryInput || buffer.size() > 100) {
                    index(buffer);
                    bufferIndexBufferValueState.clear();
                } else {
                    bufferIndexBufferValueState.write(buffer);
                }

                // query
                if(isQueryInput) {
                    query(c);
                }

            }

        }

        protected class Neo4jQueryAndIndexBatchDoFn extends Neo4jQueryAndIndexDoFn {

            @StateId(STATE_ID_INDEX_BUFFER)
            private final StateSpec<ValueState<List<UnionValue>>> indexBufferSpec;
            @StateId(STATE_ID_INTERVAL_COUNTER)
            private final StateSpec<ValueState<Integer>> bufferIntervalCounterSpec;

            Neo4jQueryAndIndexBatchDoFn(
                    final String name,
                    final List<String> inputNames,
                    final Map<String, Schema> outputSchemas,
                    final IndexDefinition index,
                    final List<QueryDefinition> queries,
                    final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                    final TimestampConverter timestampConverter,
                    final Coder<UnionValue> unionCoder) {

                super(name, inputNames, outputSchemas,
                        index, queries,
                        schemaConverter, valueCreator, neo4jValueCreator, timestampConverter);

                this.indexBufferSpec = StateSpecs.value(ListCoder.of(unionCoder));
                this.bufferIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
            }

            @Setup
            public void setup() throws IOException {
                super.setup();
            }

            @Teardown
            public void teardown() throws IOException {
                super.teardown();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<UnionValue>> bufferIndexBufferValueState,
                    final @AlwaysFetched @StateId(STATE_ID_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

                super.processElement(c, bufferIndexBufferValueState, bufferUpdateIntervalValueState);
            }

            @OnWindowExpiration
            public void onWindowExpiration(
                    @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<UnionValue>> bufferIndexBufferValueState) {

                LOG.info("onWindowExpiration");

                final List<UnionValue> buffer = Optional
                        .ofNullable(bufferIndexBufferValueState.read())
                        .orElseGet(ArrayList::new);

                index(buffer);
            }

        }

        protected class Neo4jQueryAndIndexStreamingDoFn extends Neo4jQueryAndIndexDoFn {

            @StateId(STATE_ID_INDEX_BUFFER)
            private final StateSpec<ValueState<List<UnionValue>>> indexBufferSpec;
            @StateId(STATE_ID_INTERVAL_COUNTER)
            private final StateSpec<ValueState<Integer>> bufferIntervalCounterSpec;

            Neo4jQueryAndIndexStreamingDoFn(
                    final String name,
                    final List<String> inputNames,
                    final Map<String, Schema> outputSchemas,
                    final IndexDefinition index,
                    final List<QueryDefinition> queries,
                    final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT, T> valueCreator,
                    final SchemaUtil.ValueCreator<RuntimeSchemaT, T> neo4jValueCreator,
                    final TimestampConverter timestampConverter,
                    final Coder<UnionValue> unionCoder) {

                super(name, inputNames, outputSchemas,
                        index, queries,
                        schemaConverter, valueCreator, neo4jValueCreator, timestampConverter);

                this.indexBufferSpec = StateSpecs.value(ListCoder.of(unionCoder));
                this.bufferIntervalCounterSpec = StateSpecs.value(VarIntCoder.of());
            }

            @Setup
            public void setup() throws IOException {
                super.setup();
            }

            @Teardown
            public void teardown() throws IOException {
                super.teardown();
            }

            @ProcessElement
            public void processElement(
                    final ProcessContext c,
                    final @AlwaysFetched @StateId(STATE_ID_INDEX_BUFFER) ValueState<List<UnionValue>> bufferIndexBufferValueState,
                    final @AlwaysFetched @StateId(STATE_ID_INTERVAL_COUNTER) ValueState<Integer> bufferUpdateIntervalValueState) {

                super.processElement(c, bufferIndexBufferValueState, bufferUpdateIntervalValueState);

            }

        }

    }

    private interface TimestampConverter extends Serializable {
        Object toTimestampValue(long epochMillis);
    }

}