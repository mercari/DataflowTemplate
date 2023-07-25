package com.mercari.solution.util.pipeline.union;

import com.mercari.solution.module.DataType;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class Union {

    public static UnionFlatten flatten(final List<TupleTag<?>> tags,
                                       final List<DataType> dataTypes,
                                       final List<String> inputNames) {

        final UnionFlatten unionFlat = new UnionFlatten(tags, dataTypes, inputNames);
        return unionFlat;
    }


    public static UnionWithKey withKey(final List<TupleTag<?>> tags,
                                       final List<DataType> dataTypes,
                                       final List<String> commonFields,
                                       final List<String> inputNames) {

        final UnionWithKey unionWithKey = new UnionWithKey(tags, dataTypes, commonFields, inputNames);
        return unionWithKey;
    }

    public static class UnionFlatten extends PTransform<PCollectionTuple, PCollection<UnionValue>> {

        private static final Logger LOG = LoggerFactory.getLogger(UnionFlatten.class);

        private final List<TupleTag<?>> tags;
        private final List<DataType> dataTypes;
        private final List<String> inputNames;

        public UnionFlatten(final List<TupleTag<?>> tags, final List<DataType> dataTypes, final List<String> inputNames) {

            this.tags = tags;
            this.dataTypes = dataTypes;
            this.inputNames = inputNames;
        }

        @Override
        public PCollection<UnionValue> expand(PCollectionTuple inputs) {

            final Coder<UnionValue> unionCoder = createUnionCoder(inputs, tags);

            PCollectionList<UnionValue> list = PCollectionList.empty(inputs.getPipeline());
            for(int index=0; index<tags.size(); index++) {
                final TupleTag<?> tag = tags.get(index);
                final DataType dataType = dataTypes.get(index);
                final PCollection<UnionValue> unified = inputs.get(tag)
                        .apply("Union" + inputNames.get(index), ParDo.of(new UnionDoFn<>(index, dataType)))
                        .setCoder(unionCoder);
                list = list.and(unified);
            }

            return list.apply("Flatten", Flatten.pCollections());
        }

        private static class UnionDoFn<T> extends DoFn<T, UnionValue> {

            private final int index;
            private final DataType dataType;

            UnionDoFn(final int index, final DataType dataType) {
                this.index = index;
                this.dataType = dataType;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(new UnionValue(index, dataType, c.timestamp().getMillis(), c.element()));
            }

        }
    }

    public static class UnionWithKey extends PTransform<PCollectionTuple, PCollection<KV<String,UnionValue>>> {

        private static final Logger LOG = LoggerFactory.getLogger(UnionWithKey.class);

        private final List<TupleTag<?>> tags;
        private final List<DataType> dataTypes;
        private final List<String> commonFields;
        private final List<String> inputNames;

        public UnionWithKey(final List<TupleTag<?>> tags,
                            final List<DataType> dataTypes,
                            final List<String> commonFields,
                            final List<String> inputNames) {

            this.tags = tags;
            this.dataTypes = dataTypes;
            this.commonFields = commonFields;
            this.inputNames = inputNames;
        }

        @Override
        public PCollection<KV<String, UnionValue>> expand(PCollectionTuple inputs) {

            final Coder<UnionValue> unionCoder = createUnionCoder(inputs, tags);
            final KvCoder<String, UnionValue> outputCoder = KvCoder.of(StringUtf8Coder.of(), unionCoder);

            PCollectionList<KV<String, UnionValue>> list = PCollectionList.empty(inputs.getPipeline());
            for(int index=0; index<tags.size(); index++) {
                final TupleTag<?> tag = tags.get(index);
                final DataType dataType = dataTypes.get(index);
                final SerializableFunction<UnionValue, String> groupKeysFunction = SchemaUtil.createGroupKeysFunction(UnionValue::getAsString, commonFields);
                final PCollection<KV<String, UnionValue>> unified = inputs.get(tag)
                        .apply("Union" + inputNames.get(index), ParDo
                                .of(new UnionDoFn<>(index, dataType, groupKeysFunction)))
                        .setCoder(outputCoder);
                list = list.and(unified);
            }

            return list
                    .apply("Flatten", Flatten.pCollections())
                    .setCoder(outputCoder);
        }

        private static class UnionDoFn<T> extends DoFn<T, KV<String, UnionValue>> {

            private final int index;
            private final DataType dataType;
            private final SerializableFunction<UnionValue, String> groupKeysFunction;

            UnionDoFn(final int index, final DataType dataType, final SerializableFunction<UnionValue, String> groupKeysFunction) {
                this.index = index;
                this.dataType = dataType;
                this.groupKeysFunction = groupKeysFunction;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final UnionValue unionValue = new UnionValue(index, dataType, c.timestamp().getMillis(), c.element());
                final String key = groupKeysFunction.apply(unionValue);
                c.output(KV.of(key, unionValue));
            }

        }
    }

    public static UnionCoder createUnionCoder(final PCollectionTuple inputs, final List<TupleTag<?>> tags) {
        final List<Coder<?>> coders = new ArrayList<>();
        for(final TupleTag<?> tag : tags) {
            coders.add(inputs.get(tag).getCoder());
        }
        return UnionCoder.of(coders);
    }

}

