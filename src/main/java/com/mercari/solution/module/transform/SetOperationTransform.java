package com.mercari.solution.module.transform;

import com.google.common.base.Functions;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SetOperationTransform {

    private class SetOperationTransformParameters {

        private String type;
        private List<String> keys;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<String> getKeys() {
            return keys;
        }

        public void setKeys(List<String> keys) {
            this.keys = keys;
        }

    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        PCollectionTuple groupbyInputs = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            final TupleTag tag = new TupleTag(input.getName()){};
            groupbyInputs = groupbyInputs.and(tag, input.getCollection());
        }

        final Map<String,FCollection<?>> inputCollections = inputs.stream()
                .collect(Collectors.toMap(FCollection::getName, Functions.identity()));

        final SetOperation transform = new SetOperation(config, inputCollections);
        final PCollectionTuple outputs = groupbyInputs.apply(config.getName(), transform);

        final Map<String, FCollection<?>> collections = new HashMap<>();
        for(final TupleTag tag : transform.tags) {
            final String name = config.getName() + "." + tag.getId();
            final FCollection<?> input = inputCollections.get(tag.getId());
            final PCollection output = outputs.get(tag).setCoder(input.getCollection().getCoder());
            collections.put(name, FCollection.update(input, name, output));
        }
        return collections;
    }

    public static class SetOperation extends PTransform<PCollectionTuple, PCollectionTuple> {

        private final SetOperationTransformParameters parameters;
        private final Map<String, FCollection<?>> inputCollections;

        private List<TupleTag<?>> tags;

        public SetOperationTransformParameters getParameters() {
            return parameters;
        }

        private SetOperation(final TransformConfig config, final Map<String,FCollection<?>> inputCollections) {
            this.parameters = new Gson().fromJson(config.getParameters(), SetOperationTransformParameters.class);
            this.inputCollections = inputCollections;
            validate();
        }

        @Override
        public PCollectionTuple expand(final PCollectionTuple tuple) {
            final List<TupleTag<?>> tags = new ArrayList<>();
            KeyedPCollectionTuple<String> groupbyInputs = KeyedPCollectionTuple.empty(tuple.getPipeline());
            for(final Map.Entry<TupleTag<?>, PCollection<?>> input : tuple.getAll().entrySet()) {
                final FCollection<?> inputCollection = this.inputCollections.get(input.getKey().getId());
                final TupleTag<?> tag = input.getKey();
                final PCollection kv = input.getValue()
                        .apply("WithKeys", DataTypeTransform.withKeys(inputCollection, parameters.getKeys()));
                groupbyInputs = groupbyInputs.and(tag, kv);
                tags.add(tag);
            }

            this.tags = tags;

            return groupbyInputs
                    .apply("CoGroupByKey", CoGroupByKey.create())
                    .apply("AggregateOneRecord", ParDo
                            .of(new SetOperationDoFn(tags, parameters.getType()))
                            .withOutputTags(new TupleTag<>(), TupleTagList.of(tags)));
        }

        private void validate() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("SetOperation module parameter missing!");
            }
            if(this.parameters.getType() == null) {
                throw new IllegalArgumentException("SetOperation module required type parameter!");
            }
            if(this.parameters.getKeys() == null) {
                throw new IllegalArgumentException("SetOperation module required keys parameter!");
            }
            if(this.inputCollections == null || this.inputCollections.size() < 2) {
                throw new IllegalArgumentException("GroupBy module inputs size must be over one!");
            }
        }

    }

    private static class SetOperationDoFn extends DoFn<KV<String, CoGbkResult>, String> {

        private final List<TupleTag<?>> tags;
        private final String type;

        public SetOperationDoFn(final List<TupleTag<?>> tags,
                                final String type) {
            this.tags = tags;
            this.type = type;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final CoGbkResult result = c.element().getValue();
            if(result.isEmpty()) {
                return;
            }
            c.output(c.element().getKey());

            final Map<TupleTag, List<Object>> valuesMap = new HashMap<>();
            for(final TupleTag tag : tags) {
                final List<Object> values = new ArrayList<>();
                for(final Object value : result.getAll(tag.getId())) {
                    values.add(value);
                }
                if(values.size() > 0) {
                    valuesMap.put(tag, values);
                }
            }

            if("union".equals(type.trim().toLowerCase())) {
                for(final Map.Entry<TupleTag, List<Object>> entry : valuesMap.entrySet()) {
                    entry.getValue().forEach(v -> c.output(entry.getKey(), v));
                }
            } else if("intersect".equals(type.trim().toLowerCase())) {
                if(tags.size() != valuesMap.size()) {
                    return;
                }
                for(final Map.Entry<TupleTag, List<Object>> entry : valuesMap.entrySet()) {
                    entry.getValue().forEach(v -> c.output(entry.getKey(), v));
                }
            } else if("except".equals(type.trim().toLowerCase())) {
                if(valuesMap.size() > 1) {
                    return;
                }
                final TupleTag firstTag = tags.get(0);
                if(!valuesMap.containsKey(firstTag)) {
                    return;
                }
                valuesMap.get(firstTag).forEach(v -> c.output(firstTag, v));
            }

        }

    }

}
