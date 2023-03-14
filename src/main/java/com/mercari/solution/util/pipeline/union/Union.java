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
                                       final List<SchemaUtil.StringGetter<Object>> stringGetters,
                                       final List<String> commonFields,
                                       final List<String> inputNames) {

        final UnionWithKey unionWithKey = new UnionWithKey(tags, dataTypes, stringGetters, commonFields, inputNames);
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

            final List<Coder<?>> coders = new ArrayList<>();
            for(final TupleTag<?> tag : tags) {
                coders.add(inputs.get(tag).getCoder());
            }
            final Coder<UnionValue> unionCoder = UnionCoder.of(coders);


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
        private final List<SchemaUtil.StringGetter<Object>> stringGetters;
        private final List<String> commonFields;
        private final List<String> inputNames;

        public UnionWithKey(final List<TupleTag<?>> tags,
                            final List<DataType> dataTypes,
                            final List<SchemaUtil.StringGetter<Object>> stringGetters,
                            final List<String> commonFields,
                            final List<String> inputNames) {

            this.tags = tags;
            this.dataTypes = dataTypes;
            this.stringGetters = stringGetters;
            this.commonFields = commonFields;
            this.inputNames = inputNames;
        }

        @Override
        public PCollection<KV<String, UnionValue>> expand(PCollectionTuple inputs) {

            final List<Coder<?>> coders = new ArrayList<>();
            for(final TupleTag<?> tag : tags) {
                coders.add(inputs.get(tag).getCoder());
            }
            final UnionCoder unionCoder = UnionCoder.of(coders);
            final KvCoder<String, UnionValue> outputCoder = KvCoder.of(StringUtf8Coder.of(), unionCoder);


            PCollectionList<KV<String, UnionValue>> list = PCollectionList.empty(inputs.getPipeline());
            for(int index=0; index<tags.size(); index++) {
                final TupleTag<?> tag = tags.get(index);
                final DataType dataType = dataTypes.get(index);
                final SerializableFunction<Object, String> groupKeysFunction = SchemaUtil.createGroupKeysFunction(stringGetters.get(index), commonFields);
                final PCollection<KV<String, UnionValue>> unified = inputs.get(tag)
                        .apply("Union" + inputNames.get(index), ParDo
                                .of(new UnionDoFn<>(index, dataType, groupKeysFunction)))
                        .setCoder(outputCoder);
                list = list.and(unified);
            }

            return list.apply("Flatten", Flatten.pCollections());
        }

        private static class UnionDoFn<T> extends DoFn<T, KV<String, UnionValue>> {

            private final int index;
            private final DataType dataType;
            private final SerializableFunction<Object, String> groupKeysFunction;

            UnionDoFn(final int index, final DataType dataType, final SerializableFunction<Object, String> groupKeysFunction) {
                this.index = index;
                this.dataType = dataType;
                this.groupKeysFunction = groupKeysFunction;
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final String key = groupKeysFunction.apply(c.element());
                c.output(KV.of(key, new UnionValue(index, dataType, c.timestamp().getMillis(), c.element())));
            }

        }
    }

}

