package com.mercari.solution.module.sink;

import com.google.gson.Gson;
import com.mercari.solution.FlexPipeline;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SinkModule;
import com.mercari.solution.util.pipeline.action.Action;
import com.mercari.solution.util.pipeline.action.BigQueryAction;
import com.mercari.solution.util.pipeline.action.DataflowAction;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class ActionSink implements SinkModule {

    private static final Logger LOG = LoggerFactory.getLogger(ActionSink.class);

    private static class ActionSinkParameters implements Serializable {

        private Action.Service service;
        private DataflowAction.Parameters dataflow;
        private BigQueryAction.Parameters bigquery;

        private Map<String, String> labels;

        public Action.Service getService() {
            return service;
        }

        public DataflowAction.Parameters getDataflow() {
            return dataflow;
        }

        public BigQueryAction.Parameters getBigquery() {
            return bigquery;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        private void validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.service == null) {
                errorMessages.add("action sink[" + name + "].parameters.service must not be null");
            } else {
                final List<String> missingServiceParametersErrors = switch (this.service) {
                    case dataflow -> Optional.ofNullable(this.dataflow).isEmpty() ?
                            List.of("action sink[" + name + "].parameters.dataflow must not be null") : this.dataflow.validate(name);
                    default -> throw new IllegalArgumentException();

                };
                errorMessages.addAll(missingServiceParametersErrors);
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(String.join(", ", errorMessages));
            }
        }

        private void setDefaults(final DataflowPipelineOptions options, final String config) {
            switch (this.service) {
                case dataflow -> this.dataflow.setDefaults(options, config);
            }
        }

    }

    @Override
    public String getName() { return "action"; }

    @Override
    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, SinkConfig config, List<FCollection<?>> waits) {
        if(inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("action sink module requires inputs parameter");
        }
        return Collections.singletonMap(config.getName(), action(inputs, config, waits));
    }

    private FCollection<?> action(final List<FCollection<?>> inputs, final SinkConfig config, final List<FCollection<?>> waits) {
        final Pipeline pipeline = inputs.get(0).getCollection().getPipeline();

        final ActionSinkParameters parameters = new Gson().fromJson(config.getParameters(), ActionSinkParameters.class);
        parameters.validate(config.getName());
        parameters.setDefaults(
                pipeline.getOptions().as(DataflowPipelineOptions.class),
                pipeline.getOptions().as(FlexPipeline.FlexPipelineOptions.class).getConfig());

        final List<TupleTag<?>> inputTags = new ArrayList<>();
        final List<String> inputNames = new ArrayList<>();
        final List<DataType> inputTypes = new ArrayList<>();

        PCollectionTuple tuple = PCollectionTuple.empty(pipeline);
        for (final FCollection<?> input : inputs) {
            final TupleTag inputTag = new TupleTag<>() {};
            inputTags.add(inputTag);
            inputNames.add(input.getName());
            inputTypes.add(input.getDataType());
            tuple = tuple.and(inputTag, input.getCollection());
        }

        final Transform write = new Transform(
                config.getName(),
                parameters,
                inputTags,
                inputNames,
                inputTypes,
                waits);

        final PCollection output = tuple.apply(config.getName(), write);
        final FCollection<?> fcollection = FCollection.update(inputs.get(0), output);
        return fcollection;
    }

    private static class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {

        private final String name;
        private final ActionSinkParameters parameters;
        private final List<TupleTag<?>> inputTags;
        private final List<String> inputNames;
        private final List<DataType> inputTypes;
        private final List<FCollection<?>> waits;

        private transient Schema outputSchema;

        private Transform(
                final String name,
                final ActionSinkParameters parameters,
                final List<TupleTag<?>> inputTags,
                final List<String> inputNames,
                final List<DataType> inputTypes,
                final List<FCollection<?>> waits) {

            this.name = name;
            this.parameters = parameters;
            this.inputTags = inputTags;
            this.inputNames = inputNames;
            this.inputTypes = inputTypes;
            this.waits = waits;
        }

        @Override
        public PCollection<Row> expand(PCollectionTuple inputs) {
            final PCollection<String> seed = inputs.getPipeline().begin()
                    .apply("Seed", Create.of("").withCoder(StringUtf8Coder.of()));

            final List<PCollection<?>> list = new ArrayList<>();
            for (final TupleTag<?> tag : inputTags) {
                final PCollection<?> input = inputs.get(tag);
                list.add(input);
            }

            final Action action = switch (parameters.getService()) {
                case dataflow -> DataflowAction.of(parameters.getDataflow(), parameters.getLabels());
                case bigquery -> BigQueryAction.of(parameters.getBigquery());
                default -> throw new IllegalArgumentException("Not supported service: " + parameters.getService());
            };

            this.outputSchema = action.getOutputSchema();

            return seed
                    .apply("Wait", Wait.on(list))
                    .setCoder(seed.getCoder())
                    .apply("Action", ParDo.of(new ActionDoFn<>(name, action)))
                    .setCoder(RowCoder.of(action.getOutputSchema()));
        }

        private static class ActionDoFn<T> extends DoFn<T, Row> {

            private final String name;
            private final Action action;

            ActionDoFn(
                    final String name,
                    final Action action) {

                this.name = name;
                this.action = action;
            }

            @Setup
            public void setup() {
                this.action.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c) throws IOException {
                this.action.action();
                //c.output("");
            }

        }

    }

}
