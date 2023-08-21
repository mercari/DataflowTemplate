package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
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

    private class PartitionTransformParameters implements Serializable {

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

        private class PartitionParameter implements Serializable {

            private String output;
            private JsonElement filters;

            public String getOutput() {
                return output;
            }

            public JsonElement getFilters() {
                return filters;
            }

            private List<String> validate(String name, int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.output == null) {
                    errorMessages.add("PartitionTransform module: " + name + " parameters.partitions[" + index + "].output is missing.");
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
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord> transform = new Transform<>(
                            conditionJsons, AvroSchemaUtil::getValue, parameters.getExclusive(), outputNames);
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<GenericRecord>, String> outputTags = transform.getOutputTags();
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<GenericRecord> tag = (TupleTag<GenericRecord>)entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Coder<GenericRecord> coder = (Coder<GenericRecord>) input.getCollection().getCoder();
                        final PCollection<GenericRecord> output = ((PCollection<GenericRecord>)entry.getValue()).setCoder(coder);
                        results.put(name, FCollection.of(name, output, DataType.AVRO, inputCollection.getAvroSchema()));
                    }
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row> transform = new Transform<>(
                            conditionJsons, RowSchemaUtil::getValue, parameters.getExclusive(), outputNames);
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Row>, String> outputTags = transform.getOutputTags();
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Row> tag = (TupleTag<Row>)entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Coder<Row> coder = inputCollection.getCollection().getCoder();
                        final PCollection<Row> output = ((PCollection<Row>)entry.getValue()).setCoder(coder);
                        results.put(name, FCollection.of(name, output, DataType.ROW, inputCollection.getSchema()));
                    }
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct> transform = new Transform<>(
                            conditionJsons, StructSchemaUtil::getValue, parameters.getExclusive(), outputNames);
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Struct>, String> outputTags = transform.getOutputTags();
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Struct> tag = (TupleTag<Struct>)entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Coder<Struct> coder = inputCollection.getCollection().getCoder();
                        final PCollection<Struct> output = ((PCollection<Struct>) entry.getValue()).setCoder(coder);
                        results.put(name, FCollection.of(name, output.setCoder(coder), DataType.STRUCT, inputCollection.getSpannerType()));
                    }
                    break;
                }
                case DOCUMENT: {
                    final FCollection<Document> inputCollection = (FCollection<Document>) input;
                    final Transform<Document> transform = new Transform<>(
                            conditionJsons, DocumentSchemaUtil::getValue, parameters.getExclusive(), outputNames);
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Document>, String> outputTags = transform.getOutputTags();
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Document> tag = (TupleTag<Document>)entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Coder<Document> coder = inputCollection.getCollection().getCoder();
                        final PCollection<Document> output = ((PCollection<Document>) entry.getValue()).setCoder(coder);
                        results.put(name, FCollection.of(name, output, DataType.DOCUMENT, inputCollection.getSchema()));
                    }
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity> transform = new Transform<>(
                            conditionJsons, EntitySchemaUtil::getValue, parameters.getExclusive(), outputNames);
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    final Map<TupleTag<Entity>, String> outputTags = transform.getOutputTags();
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final TupleTag<Entity> tag = (TupleTag<Entity>)entry.getKey();
                        final String name = String.format("%s%s%s", prefix, parameters.getSeparator(), outputTags.get(tag));
                        final Coder<Entity> coder = inputCollection.getCollection().getCoder();
                        final PCollection<Entity> output = ((PCollection<Entity>) entry.getValue()).setCoder(coder);
                        results.put(name, FCollection.of(name, output, DataType.ENTITY, inputCollection.getSchema()));
                    }
                    break;
                }
                default:
                    break;
            }
        }

        return results;
    }

    public static class Transform<T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final List<KV<TupleTag<T>, String>> conditionJsons;
        private final SchemaUtil.ValueGetter<T> valueGetter;
        private final boolean exclusive;


        private final TupleTag<T> defaultOutputTag = new TupleTag<>(){};
        private final Map<TupleTag<T>, String> outputTags;


        public TupleTag<T> getDefaultOutputTag() {
            return defaultOutputTag;
        }

        public Map<TupleTag<T>, String> getOutputTags() {
            return outputTags;
        }

        private Transform(final List<String> conditionJsons,
                          final SchemaUtil.ValueGetter<T> valueGetter,
                          final boolean exclusive,
                          final List<String> outputNames) {

            this.outputTags = new HashMap<>();
            this.conditionJsons = new ArrayList<>();
            for(int i=0; i<conditionJsons.size(); i++) {
                final TupleTag<T> tag = new TupleTag<>() {};
                final String conditionJson = conditionJsons.get(i);
                final String outputName = outputNames.get(i);
                this.outputTags.put(tag, outputName);
                this.conditionJsons.add(KV.of(tag, conditionJson));
            }
            this.outputTags.put(defaultOutputTag, "defaults");

            this.valueGetter = valueGetter;
            this.exclusive = exclusive;
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {
            return input.apply("Partition", ParDo
                    .of(new PartitionDoFn(valueGetter, exclusive))
                    .withOutputTags(defaultOutputTag, TupleTagList.of(conditionJsons.stream()
                            .map(KV::getKey)
                            .collect(Collectors.toList()))));
        }

        private class PartitionDoFn extends DoFn<T, T> {

            private final SchemaUtil.ValueGetter<T> getter;
            private final boolean exclusive;

            private transient List<KV<TupleTag<T>, Filter.ConditionNode>> conditions;

            PartitionDoFn(final SchemaUtil.ValueGetter<T> getter,
                          final boolean exclusive) {

                this.getter = getter;
                this.exclusive = exclusive;
            }

            @Setup
            public void setup() {
                this.conditions = conditionJsons.stream()
                        .map(kv -> KV.of(
                                kv.getKey(),
                                Filter.parse(new Gson().fromJson(kv.getValue(), JsonElement.class))))
                        .collect(Collectors.toList());
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                boolean output = false;
                for (final KV<TupleTag<T>, Filter.ConditionNode> condition : conditions) {
                    if (Filter.filter(element, getter, condition.getValue())) {
                        c.output(condition.getKey(), element);
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
