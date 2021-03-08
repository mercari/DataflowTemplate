package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.Filter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
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

        public Boolean getExclusive() {
            return exclusive;
        }

        public void setExclusive(Boolean exclusive) {
            this.exclusive = exclusive;
        }

        public List<PartitionParameter> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<PartitionParameter> partitions) {
            this.partitions = partitions;
        }

        private class PartitionParameter implements Serializable {

            private String output;
            private JsonElement filters;

            public String getOutput() {
                return output;
            }

            public void setOutput(String output) {
                this.output = output;
            }

            public JsonElement getFilters() {
                return filters;
            }

            public void setFilters(JsonElement filters) {
                this.filters = filters;
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

        validateParameters(parameters);
        setDefaultParameters(parameters);

        final List<KV<String, String>> conditionJsons = parameters.getPartitions().stream()
                .map(p -> KV.of(p.getOutput(), p.getFilters().toString()))
                .collect(Collectors.toList());

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String prefix = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord> transform = new Transform<>(conditionJsons, AvroSchemaUtil::getValue, parameters.getExclusive());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final String name = prefix + "." + entry.getKey().getId();
                        final PCollection<GenericRecord> collection = entry.getValue()
                                .setCoder((Coder)input.getCollection().getCoder());
                        results.put(name, FCollection.of(name, collection, DataType.AVRO, inputCollection.getAvroSchema()));
                    }
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row> transform = new Transform<>(conditionJsons, RowSchemaUtil::getValue, parameters.getExclusive());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final String name = prefix + "." + entry.getKey().getId();
                        final PCollection<GenericRecord> collection = entry.getValue()
                                .setCoder((Coder)input.getCollection().getCoder());
                        results.put(name, FCollection.of(name, collection, DataType.ROW, inputCollection.getSchema()));
                    }
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct> transform = new Transform<>(conditionJsons, StructSchemaUtil::getValue, parameters.getExclusive());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final String name = prefix + "." + entry.getKey().getId();
                        final PCollection<GenericRecord> collection = entry.getValue()
                                .setCoder((Coder)input.getCollection().getCoder());
                        results.put(name, FCollection.of(name, collection, DataType.STRUCT, inputCollection.getSpannerType()));
                    }
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity> transform = new Transform<>(conditionJsons, EntitySchemaUtil::getValue, parameters.getExclusive());
                    final PCollectionTuple tuple = inputCollection.getCollection().apply(prefix, transform);
                    for(final Map.Entry<TupleTag<?>, PCollection<?>> entry : tuple.getAll().entrySet()) {
                        final String name = prefix + "." + entry.getKey().getId();
                        final PCollection<GenericRecord> collection = entry.getValue()
                                .setCoder((Coder)input.getCollection().getCoder());
                        results.put(name, FCollection.of(name, collection, DataType.ENTITY, inputCollection.getSchema()));
                    }
                    break;
                }
                default:
                    break;
            }
        }

        return results;
    }

    private static void validateParameters(final PartitionTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("PartitionTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getPartitions() == null) {
            errorMessages.add("PartitionTransform config parameters must contain partitions parameter.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(PartitionTransformParameters parameters) {
        if(parameters.getExclusive() == null) {
            parameters.setExclusive(true);
        }
    }

    public static class Transform<T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final List<KV<String, String>> conditionJsons;
        private final Filter.Getter<T> getter;
        private final boolean exclusive;

        private final Map<String, TupleTag<T>> outputTags;
        private final TupleTag<T> defaultOutputTag = new TupleTag<>("defaults"){};

        private Transform(final List<KV<String, String>> conditionJsons,
                          final Filter.Getter<T> getter,
                          final boolean exclusive) {

            this.conditionJsons = conditionJsons;
            this.getter = getter;
            this.exclusive = exclusive;

            this.outputTags = new HashMap<>();
            for(var kv : conditionJsons) {
                outputTags.put(kv.getKey(), new TupleTag<>(kv.getKey()){});
            }
            outputTags.put(defaultOutputTag.getId(), defaultOutputTag);
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {
            return input.apply("Partition", ParDo
                    .of(new PartitionDoFn(conditionJsons, getter, exclusive))
                    .withOutputTags(defaultOutputTag, TupleTagList.of(outputTags.values().stream()
                            .filter(t -> !t.getId().equals(defaultOutputTag.getId()))
                            .collect(Collectors.toList()))));
        }

        private class PartitionDoFn extends DoFn<T, T> {

            private final List<KV<String, String>> conditionJsons;
            private final Filter.Getter<T> getter;
            private final boolean exclusive;

            private transient List<KV<String, Filter.ConditionNode>> conditions;

            PartitionDoFn(final List<KV<String, String>> conditionJsons,
                          final Filter.Getter<T> getter,
                          final boolean exclusive) {
                this.conditionJsons = conditionJsons;
                this.getter = getter;
                this.exclusive = exclusive;
            }

            @Setup
            public void setup() {
                this.conditions = conditionJsons.stream()
                        .map(kv -> KV.of(kv.getKey(), Filter.parse(new Gson().fromJson(kv.getValue(), JsonElement.class))))
                        .collect(Collectors.toList());
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T element = c.element();
                boolean output = false;
                for(KV<String, Filter.ConditionNode> condition : conditions) {
                    if(Filter.filter(element, getter, condition.getValue())) {
                        c.output(outputTags.get(condition.getKey()), element);
                        output = true;
                        if(exclusive) {
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
