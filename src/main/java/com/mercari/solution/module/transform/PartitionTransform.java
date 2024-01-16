package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.converter.RowToMutationConverter;
import com.mercari.solution.util.converter.RowToRecordConverter;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


public class PartitionTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionTransform.class);

    private static class PartitionTransformParameters implements Serializable {

        private Boolean exclusive;
        private List<PartitionParameter> partitions;
        private String separator;

        public Boolean getExclusive() {
            return exclusive;
        }

        public List<PartitionParameter> getPartitions() {
            return partitions;
        }

        public String getSeparator() {
            return separator;
        }

        private static class PartitionParameter implements Serializable {

            private String output;
            private JsonElement filters;
            private JsonArray select;

            public String getOutput() {
                return output;
            }

            public JsonElement getFilters() {
                return filters;
            }

            public JsonArray getSelect() {
                return select;
            }

            private List<String> validate(String name, int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.output == null) {
                    errorMessages.add("Partition transform module: " + name + ".partitions[" + index + "].output must not be null.");
                }
                return errorMessages;
            }

            private void setDefaults() {

            }

        }

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.getPartitions() == null || this.getPartitions().size() == 0) {
                errorMessages.add("PartitionTransform module: " + name + " partitions parameter is missing.");
            } else {
                for(int index=0; index<this.partitions.size(); index++) {
                    errorMessages.addAll(partitions.get(index).validate(name, index));
                }
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(String.join("\n", errorMessages));
            }
        }

        private void setDefaults() {

            for(final PartitionParameter partitionParameter : partitions) {
                partitionParameter.setDefaults();
            }
            if(this.exclusive == null) {
                this.exclusive = true;
            }
            if(this.separator == null) {
                this.separator = ".";
            }

        }

    }

    public String getName() { return "partition"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final PartitionTransformParameters parameters = new Gson()
                .fromJson(config.getParameters(), PartitionTransformParameters.class);

        if(parameters == null) {
            throw new IllegalArgumentException("PartitionTransform module: " + config.getName() + " parameters must not be empty!");
        }

        parameters.validate(config.getName());
        parameters.setDefaults();

        final List<String> conditionJsons = new ArrayList<>();
        final List<String> outputNames = new ArrayList<>();
        for(int i=0; i<parameters.getPartitions().size(); i++) {
            final var p = parameters.getPartitions().get(i);
            conditionJsons.add(p.getFilters().toString());
            outputNames.add(p.getOutput());
        }

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String prefix = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());

            final List<List<SelectFunction>> selectFunctionsList = new ArrayList<>();
            for(int i=0; i<parameters.getPartitions().size(); i++) {
                final var p = parameters.getPartitions().get(i);
                if(p.getSelect() != null && p.getSelect().isJsonArray()) {
                    final List<SelectFunction> selectFunctions = SelectFunction
                            .of(p.getSelect(), input.getSchema().getFields(), input.getDataType());
                    selectFunctionsList.add(selectFunctions);
                } else {
                    selectFunctionsList.add(new ArrayList<>());
                }
            }

            switch (input.getDataType()) {
                case ROW -> {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Schema, Row> transform = new Transform<>(
                            conditionJsons, selectFunctionsList, parameters.getExclusive(),
                            RowSchemaUtil::getValue, RowSchemaUtil::create, s -> s, outputNames, input.getDataType());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Row>, String> outputTags = transform.getOutputTags();
                    for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Row> tag = (TupleTag<Row>) entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Schema outputSchema = transform.getOutputSchemas().getOrDefault(tag, inputCollection.getSchema());
                        final PCollection<Row> output = ((PCollection<Row>) entry.getValue()).setCoder(RowCoder.of(outputSchema));
                        results.put(name, FCollection.of(name, output, DataType.ROW, outputSchema));
                    }
                }
                case AVRO -> {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<org.apache.avro.Schema, GenericRecord> transform = new Transform<>(
                            conditionJsons, selectFunctionsList, parameters.getExclusive(),
                            AvroSchemaUtil::getValue, AvroSchemaUtil::create,
                            RowToRecordConverter::convertSchema, outputNames, input.getDataType());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<GenericRecord>, String> outputTags = transform.getOutputTags();
                    for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<GenericRecord> tag = (TupleTag<GenericRecord>) entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final org.apache.avro.Schema outputSchema = Optional.ofNullable(transform.getOutputSchemas().get(tag))
                                .map(RowToRecordConverter::convertSchema)
                                .orElse(inputCollection.getAvroSchema());
                        final PCollection<GenericRecord> output = ((PCollection<GenericRecord>) entry.getValue()).setCoder(AvroCoder.of(outputSchema));
                        results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    }
                }
                case STRUCT -> {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Type, Struct> transform = new Transform<>(
                            conditionJsons, selectFunctionsList, parameters.getExclusive(),
                            StructSchemaUtil::getValue, StructSchemaUtil::create,
                            RowToMutationConverter::convertSchema, outputNames, input.getDataType());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Struct>, String> outputTags = transform.getOutputTags();
                    for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Struct> tag = (TupleTag<Struct>) entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Type outputType = Optional.ofNullable(transform.getOutputSchemas().get(tag))
                                .map(RowToMutationConverter::convertSchema)
                                .orElse(inputCollection.getSpannerType());
                        final PCollection<Struct> output = ((PCollection<Struct>) entry.getValue())
                                .setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(name, output, DataType.STRUCT, outputType));
                    }
                }
                case DOCUMENT -> {
                    final FCollection<Document> inputCollection = (FCollection<Document>) input;
                    final Transform<Schema, Document> transform = new Transform<>(
                            conditionJsons, selectFunctionsList, parameters.getExclusive(),
                            DocumentSchemaUtil::getValue, DocumentSchemaUtil::create,
                            s -> s, outputNames, input.getDataType());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Document>, String> outputTags = transform.getOutputTags();
                    for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Document> tag = (TupleTag<Document>) entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Schema outputSchema = transform.getOutputSchemas().getOrDefault(tag, inputCollection.getSchema());
                        final PCollection<Document> output = ((PCollection<Document>) entry.getValue())
                                .setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(name, output, DataType.DOCUMENT, outputSchema));
                    }
                }
                case ENTITY -> {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Schema, Entity> transform = new Transform<>(
                            conditionJsons, selectFunctionsList, parameters.getExclusive(),
                            EntitySchemaUtil::getValue, EntitySchemaUtil::create,
                            s -> s, outputNames, input.getDataType());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Entity>, String> outputTags = transform.getOutputTags();
                    for (final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Entity> tag = (TupleTag<Entity>) entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Schema outputSchema = transform.getOutputSchemas().getOrDefault(tag, inputCollection.getSchema());
                        final PCollection<Entity> output = ((PCollection<Entity>) entry.getValue())
                                .setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(name, output, DataType.ENTITY, outputSchema));
                    }
                }
                default -> throw new IllegalArgumentException("Partition transform not supported type: " + input.getDataType());
            }
        }

        return results;
    }

    public static class Transform<RuntimeSchemaT,T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final List<KV<TupleTag<T>, String>> conditionJsons;
        private final Map<TupleTag<T>, List<SelectFunction>> selectFunctionsMap;
        private final boolean exclusive;
        private final SchemaUtil.ValueGetter<T> valueGetter;
        private final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator;
        private final SchemaUtil.SchemaConverter<Schema, RuntimeSchemaT> schemaConverter;


        private final TupleTag<T> defaultOutputTag = new TupleTag<>(){};
        private final Map<TupleTag<T>, String> outputTags;
        private final Map<TupleTag<T>, Schema> outputSchemas;
        private final DataType inputType;


        public Map<TupleTag<T>, String> getOutputTags() {
            return outputTags;
        }

        public Map<TupleTag<T>, Schema> getOutputSchemas() {
            return outputSchemas;
        }

        private Transform(final List<String> conditionJsons,
                          final List<List<SelectFunction>> selectFunctionsList,
                          final boolean exclusive,
                          final SchemaUtil.ValueGetter<T> valueGetter,
                          final SchemaUtil.ValueCreator<RuntimeSchemaT,T> valueCreator,
                          final SchemaUtil.SchemaConverter<Schema,RuntimeSchemaT> schemaConverter,
                          final List<String> outputNames,
                          final DataType inputType) {

            this.outputTags = new HashMap<>();
            this.outputSchemas = new HashMap<>();

            this.conditionJsons = new ArrayList<>();
            this.selectFunctionsMap = new HashMap<>();
            for(int i=0; i<conditionJsons.size(); i++) {
                final TupleTag<T> tag = new TupleTag<>() {};
                final String conditionJson = conditionJsons.get(i);
                final String outputName = outputNames.get(i);
                this.outputTags.put(tag, outputName);
                this.conditionJsons.add(KV.of(tag, conditionJson));
                this.selectFunctionsMap.put(tag, selectFunctionsList.get(i));
                if(selectFunctionsList.get(i).size() > 0) {
                    outputSchemas.put(tag, SelectFunction.createSchema(selectFunctionsList.get(i)));
                }
            }
            this.exclusive = exclusive;

            this.outputTags.put(defaultOutputTag, "defaults");
            this.valueGetter = valueGetter;
            this.valueCreator = valueCreator;
            this.schemaConverter = schemaConverter;
            this.inputType = inputType;
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {
            return input
                    .apply("Partition", ParDo
                            .of(new PartitionDoFn())
                            .withOutputTags(defaultOutputTag, TupleTagList.of(conditionJsons.stream()
                                    .map(KV::getKey)
                                    .collect(Collectors.toList()))));
        }

        private class PartitionDoFn extends DoFn<T, T> {

            private transient List<KV<TupleTag<T>, Filter.ConditionNode>> conditions;
            private transient Map<TupleTag<T>, RuntimeSchemaT> runtimeOutputSchemas;

            @Setup
            public void setup() {
                this.conditions = conditionJsons.stream()
                        .map(kv -> KV.of(
                                kv.getKey(),
                                Filter.parse(new Gson().fromJson(kv.getValue(), JsonElement.class))))
                        .collect(Collectors.toList());
                for(List<SelectFunction> selectFunctions : selectFunctionsMap.values()) {
                    for(SelectFunction selectFunction: selectFunctions) {
                        selectFunction.setup();
                    }
                }

                this.runtimeOutputSchemas = outputSchemas.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> schemaConverter.convert(e.getValue())));
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                boolean output = false;
                for (final KV<TupleTag<T>, Filter.ConditionNode> condition : conditions) {
                    if (condition.getKey() == null) {
                        continue;
                    }
                    if (Filter.filter(element, valueGetter, condition.getValue())) {
                        final List<SelectFunction> selectFunctions = selectFunctionsMap.get(condition.getKey());
                        final T result;
                        if(selectFunctions.size() > 0) {
                            final RuntimeSchemaT schema = runtimeOutputSchemas.get(condition.getKey());
                            final Map<String, Object> values = SelectFunction.apply(selectFunctions, element, inputType, inputType);
                            result = valueCreator.create(schema, values);
                        } else {
                            result = element;
                        }
                        c.output(condition.getKey(), result);
                        output = true;
                        if (exclusive) {
                            return;
                        }
                    }
                }
                if(!output) {
                    c.output(element);
                }
            }
        }

    }

}
