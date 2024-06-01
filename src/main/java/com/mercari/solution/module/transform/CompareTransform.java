package com.mercari.solution.module.transform;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.pipeline.union.Union;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CompareTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(CompareTransform.class);

    private static class CompareTransformParameters implements Serializable {

        private List<String> outputs;
        private List<String> primaryKeyFields;


        public List<String> getOutputs() {
            return outputs;
        }

        public List<String> getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(primaryKeyFields == null || primaryKeyFields.isEmpty()) {
                errorMessages.add("compare transform module[" + name + "] primaryKeyFields parameter is required");
            }
            if (!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        public void setDefaults() {
            if (this.outputs == null) {
                this.outputs = new ArrayList<>();
            }
            if (this.primaryKeyFields == null) {
                this.primaryKeyFields = new ArrayList<>();
            }
        }

        public static CompareTransformParameters of(TransformConfig config) {
            final CompareTransformParameters parameters = new Gson().fromJson(config.getParameters(), CompareTransformParameters.class);
            if (parameters == null) {
                throw new IllegalArgumentException("compare transform module[" + config.getName() + "] parameters must not be empty!");
            }
            parameters.validate(config.getName());
            parameters.setDefaults();
            return parameters;
        }

    }

    @Override
    public String getName() {
        return "compare";
    }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        if(OptionUtil.isStreaming(inputs.get(0).getCollection())) {
            throw new IllegalArgumentException("compare transform module does not support streaming mode.");
        }
        return transformSingle(inputs, config);
    }

    public static Map<String, FCollection<?>> transformSingle(final List<FCollection<?>> inputs, final TransformConfig config) {
        final CompareTransformParameters parameters = CompareTransformParameters.of(config);

        final List<String> inputNames = new ArrayList<>();
        final List<TupleTag<?>> tags = new ArrayList<>();
        final List<Schema> inputSchemas = new ArrayList<>();
        final List<DataType> dataTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for (final FCollection<?> input : inputs) {
            inputNames.add(input.getName());
            inputSchemas.add(input.getAvroSchema());
            dataTypes.add(input.getDataType());
            final TupleTag inputTag = new TupleTag();
            tags.add(inputTag);
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final TransformSingle transform = new TransformSingle(
                config.getName(), inputNames, tags, inputSchemas, dataTypes, parameters);
        final PCollectionTuple outputs = tuple.apply(config.getName(), transform);

        return Map.of(
                config.getName(), FCollection.of(config.getName(), outputs.get(transform.outputComparingSchemaResultsTag), DataType.AVRO, createComparingRowResultSchema())
        );
    }

    private static class TransformSingle extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final String table;
        private final List<String> groupFields;

        private final List<String> inputNames;
        private final List<TupleTag<?>> tags;
        private final List<String> inputSchemas;
        private final List<DataType> dataTypes;

        public final TupleTag<GenericRecord> outputComparingRowResultsTag;
        public final TupleTag<GenericRecord> outputComparingSchemaResultsTag;
        public final TupleTag<GenericRecord> outputComparingRowResultsSummaryTag;

        TransformSingle(
                final String name,
                final List<String> inputNames,
                final List<TupleTag<?>> inputTags,
                final List<Schema> inputSchemas,
                final List<DataType> inputTypes,
                final CompareTransformParameters parameters) {

            this.table = name;
            this.groupFields = parameters.getPrimaryKeyFields();

            this.inputNames = inputNames;
            this.tags = inputTags;
            this.inputSchemas = inputSchemas.stream().map(Schema::toString).collect(Collectors.toList());
            this.dataTypes = inputTypes;

            this.outputComparingRowResultsTag = new TupleTag<>(){};
            this.outputComparingSchemaResultsTag = new TupleTag<>(){};
            this.outputComparingRowResultsSummaryTag = new TupleTag<>(){};
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple tuple) {
            final PCollection<GenericRecord> comparingRowResults = tuple
                    .apply("UnionWithKey", Union.withKey(tags, dataTypes, groupFields, inputNames))
                    .apply("GroupByKeys", GroupByKey.create())
                    .apply("Compare", ParDo.of(new CompareDoFn(table, inputSchemas.get(0), inputNames)))
                    .setCoder(AvroCoder.of(createComparingRowResultSchema()));

            return PCollectionTuple
                    .of(outputComparingRowResultsTag, comparingRowResults)
                    .and(outputComparingSchemaResultsTag, comparingRowResults);
        }
    }

    private static class TransformAll extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final List<String> groupFields;
        private final Map<String, Map<String, KV<TupleTag, Schema>>> tags;

        public final TupleTag<GenericRecord> outputComparingRowResultsTag;
        public final TupleTag<GenericRecord> outputComparingSchemaResultsTag;
        public final TupleTag<GenericRecord> outputComparingRowResultsSummaryTag;

        private final Map<TupleTag, DataType> dataTypes;
        TransformAll(
                final Map<String, Map<String, KV<TupleTag, Schema>>> inputTags,
                final Map<TupleTag, DataType> inputTypes,
                final CompareTransformParameters parameters) {

            this.groupFields = parameters.getPrimaryKeyFields();
            this.tags = inputTags;
            this.dataTypes = inputTypes;

            this.outputComparingRowResultsTag = new TupleTag<>(){};
            this.outputComparingSchemaResultsTag = new TupleTag<>(){};
            this.outputComparingRowResultsSummaryTag = new TupleTag<>(){};
        }

        @Override
        public PCollectionTuple expand(PCollectionTuple tuple) {

            PCollectionList<GenericRecord> comparingRowResultsList = PCollectionList.empty(tuple.getPipeline());
            for(final Map.Entry<String, Map<String, KV<TupleTag, Schema>>> tableAndTags : tags.entrySet()) {
                final String table = tableAndTags.getKey();

                final List<TupleTag<?>> inputTags = new ArrayList<>();
                final List<String> inputNames = new ArrayList<>();
                final List<DataType> inputTypes = new ArrayList<>();

                PCollectionTuple tableTuple = PCollectionTuple.empty(tuple.getPipeline());
                for(final Map.Entry<String, KV<TupleTag, Schema>> inputAndTagSchema : tableAndTags.getValue().entrySet()) {
                    final String inputName = inputAndTagSchema.getKey();
                    final TupleTag inputTag = inputAndTagSchema.getValue().getKey();
                    final Schema schema = inputAndTagSchema.getValue().getValue();

                    inputTags.add(inputTag);
                    inputNames.add(inputName);
                    inputTypes.add(dataTypes.get(inputTag));
                    //inputSchemas.put(inputName, schema);

                    tableTuple = tableTuple.and(inputTag, tuple.get(inputTag));
                }

                final PCollection<GenericRecord> results = tableTuple
                        .apply("Union" + table, Union.withKey(inputTags, inputTypes, groupFields, inputNames))
                        .apply("GroupByKeys", GroupByKey.create())
                        .apply("Compare", ParDo.of(new CompareDoFn(table, null, inputNames)))
                        .setCoder(AvroCoder.of(createComparingRowResultSchema()));
                comparingRowResultsList = comparingRowResultsList.and(results);
            }

            final PCollection<GenericRecord> comparingRowResults = comparingRowResultsList
                    .apply("Flatten", Flatten.pCollections());


            return PCollectionTuple
                    .of(outputComparingRowResultsTag, comparingRowResults)
                    .and(outputComparingSchemaResultsTag, comparingRowResults);
        }

    }

    private static class CompareDoFn extends DoFn<KV<String, Iterable<UnionValue>>, GenericRecord> {

        private final String table;
        private final List<String> inputNames;
        private final String inputSchemaJson;
        private transient Schema inputSchema;
        private transient Schema outputSchema;

        CompareDoFn(final String table, final String inputSchemaJson, final List<String> inputNames) {
            this.table = table;
            this.inputSchemaJson = inputSchemaJson;
            this.inputNames = inputNames;
        }

        @Setup
        public void setup() {
            this.inputSchema = AvroSchemaUtil.convertSchema(this.inputSchemaJson);
            this.outputSchema = createComparingRowResultSchema();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {

            final String key = c.element().getKey();
            final List<UnionValue> unionValues = Lists.newArrayList(c.element().getValue());
            final List<Integer> indexes = unionValues.stream().map(UnionValue::getIndex).toList();

            final List<String> missingInputs = new ArrayList<>();
            final List<String> duplicatedInputs = new ArrayList<>();

            final long indexUniqueSize = indexes.stream().distinct().count();
            if(inputNames.size() != indexUniqueSize) {
                final Set<String> n = new HashSet<>(inputNames);
                for(final Integer index : indexes) {
                    n.remove(inputNames.get(index));
                }
                missingInputs.addAll(n);
            }
            if(indexes.size() > indexUniqueSize) {
                duplicatedInputs.add("");
            }
            final List<Map<String,Object>> valuesList = unionValues.stream().map(UnionValue::asPrimitiveMap).toList();

            final List<GenericRecord> differences = new ArrayList<>();

            for(final Schema.Field field : inputSchema.getFields()) {
                final long count = valuesList.stream()
                        .map(v -> v.get(field.name()))
                        .map(v -> v instanceof byte[] ? Base64.getEncoder().encodeToString((byte[])v) : v)
                        .distinct()
                        .count();
                if(count != 1) {
                    final Map<String, String> differenceValues = new HashMap<>();
                    for(final UnionValue unionValue : unionValues) {
                        final String value = UnionValue.getAsString(unionValue, field.name());
                        final String inputName = inputNames.get(unionValue.getIndex());
                        differenceValues.put(inputName, value);
                    }

                    final GenericRecord difference = new GenericRecordBuilder(outputSchema.getField("differences").schema().getElementType())
                            .set("field", field.name())
                            .set("values", differenceValues)
                            .build();
                    differences.add(difference);
                }
            }

            if(!missingInputs.isEmpty() || !duplicatedInputs.isEmpty() || !differences.isEmpty()) {
                final GenericRecord result = new GenericRecordBuilder(outputSchema)
                        .set("table", table)
                        .set("keys", key)
                        .set("missingInputs", missingInputs)
                        .set("duplicatedInputs", duplicatedInputs)
                        .set("differences", differences)
                        .build();

                c.output(result);
                LOG.error("NG table: {}, for key: {}", table, key);
            }
        }

    }

    private static Schema createComparingSchemaResultSchema() {
        return SchemaBuilder.record("ComparingSchemaResult")
                .fields()
                .name("table").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("inputs").type(Schema.createArray(Schema.create(Schema.Type.STRING))).noDefault()
                .name("missingInputs").type(Schema.createArray(Schema.create(Schema.Type.STRING))).noDefault()
                .name("differences").type(Schema.createArray(
                        SchemaBuilder.builder().record("SchemaDifference").fields()
                                .name("field").type(Schema.create(Schema.Type.STRING)).noDefault()
                                .name("types").type(Schema.createMap(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)))).noDefault()
                                .endRecord())).noDefault()
                .endRecord();
    }

    private static Schema createComparingRowResultSchema() {
        return SchemaBuilder.record("ComparingRowResult")
                .fields()
                .name("table").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("keys").type(Schema.create(Schema.Type.STRING)).noDefault()
                .name("missingInputs").type(Schema.createArray(Schema.create(Schema.Type.STRING))).noDefault()
                .name("duplicatedInputs").type(Schema.createArray(Schema.create(Schema.Type.STRING))).noDefault()
                .name("differences").type(Schema.createArray(
                        SchemaBuilder.builder().record("RowDifference").fields()
                                .name("field").type(Schema.create(Schema.Type.STRING)).noDefault()
                                .name("values").type(Schema.createMap(Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)))).noDefault()
                        .endRecord())).noDefault()
                .endRecord();
    }
}