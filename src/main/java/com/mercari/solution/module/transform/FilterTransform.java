package com.mercari.solution.module.transform;

import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.List;

public class FilterTransform {

    private class FilterTransformParameters {

    }

    public static FCollection<?> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        if(inputs.size() > 1) {
            throw new IllegalArgumentException("Filter input must be one, but got " + inputs.size());
        }
        final FCollection<?> inputCollection = inputs.get(0);

        final Filter transform = new Filter(config);
        final PCollection output = (PCollection<?>) (inputCollection.getCollection()).apply(config.getName(), transform);
        return FCollection.update(inputCollection, output);
    }

    public static class Filter<T> extends PTransform<PCollection<T>, PCollection<T>> {

        private final FilterTransformParameters parameters;

        private Filter(final TransformConfig config) {
            this.parameters = new Gson().fromJson(config.getParameters(), FilterTransformParameters.class);
        }

        @Override
        public PCollection<T> expand(final PCollection<T> input) {
            validate();
            setDefaultParameters();

            return null;

        }

        private void validate() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Window module parameter missing!");
            }
        }

        private void setDefaultParameters() {

        }

    }

}
