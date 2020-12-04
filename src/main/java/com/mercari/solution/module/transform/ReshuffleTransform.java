package com.mercari.solution.module.transform;

import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReshuffleTransform {

    private static final Logger LOG = LoggerFactory.getLogger(ReshuffleTransform.class);

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final WithReshuffle transform = new WithReshuffle(config);
        final Map<String, FCollection<?>> collections = new HashMap<>();
        for(final FCollection input : inputs) {
            final String name = config.getName() + "." + input.getName();
            final Coder coder = input.getCollection().getCoder();
            final PCollection<?> output = ((PCollection<?>) (input.getCollection()).apply(config.getName(), transform))
                    .setCoder(coder);
            collections.put(name, FCollection.update(input, name, output));
        }
        return collections;
    }

    public static class WithReshuffle<T> extends PTransform<PCollection<T>, PCollection<T>> {

        private WithReshuffle(final TransformConfig config) {}

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            return input.apply("WithWindow", Reshuffle.viaRandomKey());
        }

    }

}
