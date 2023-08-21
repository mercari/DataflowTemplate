package com.mercari.solution.module.transform;

import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReshuffleTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(ReshuffleTransform.class);

    public String getName() { return "reshuffle"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return ReshuffleTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final Map<String, FCollection<?>> outputs = new HashMap<>();
        for(final FCollection input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            final PCollection<?> output = ((PCollection<?>) (input.getCollection())
                    .apply(config.getName(), Reshuffle.viaRandomKey()))
                    .setCoder(input.getCollection().getCoder());
            outputs.put(name, FCollection.update(input, name, output));
        }
        return outputs;
    }

}
