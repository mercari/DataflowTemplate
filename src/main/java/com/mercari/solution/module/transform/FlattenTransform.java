package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.DataTypeTransform;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlattenTransform {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenTransform.class);

    private class FlattenTransformParameters {

        private String dataType;

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }
    }

    public static FCollection<?> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final FlattenTransformParameters parameters = new Gson()
                .fromJson(config.getParameters(), FlattenTransformParameters.class);
        final DataType dataType = selectDataType(parameters, inputs);

        final Pipeline pipeline = inputs.get(0).getCollection().getPipeline();
        switch (dataType) {
            case AVRO: {
                final KV<FCollection<GenericRecord>, PCollectionList<GenericRecord>> list = createCollectionList(pipeline, dataType, inputs);
                final Flatten<GenericRecord> transform = new Flatten(parameters);
                final PCollection<GenericRecord> output = list.getValue().apply(config.getName(), transform);
                return FCollection.update(list.getKey(), output);
            }
            case ROW: {
                final KV<FCollection<Row>, PCollectionList<Row>> list = createCollectionList(pipeline, dataType, inputs);
                final Flatten<Row> transform = new Flatten(parameters);
                final PCollection<Row> output = list.getValue().apply(config.getName(), transform);
                return FCollection.update(list.getKey(), output);
            }
            case STRUCT: {
                final KV<FCollection<Struct>, PCollectionList<Struct>> list = createCollectionList(pipeline, dataType, inputs);
                final Flatten<Struct> transform = new Flatten(parameters);
                final PCollection<Struct> output = list.getValue().apply(config.getName(), transform);
                return FCollection.update(list.getKey(), output);
            }
            case MUTATION: {
                final KV<FCollection<Mutation>, PCollectionList<Mutation>> list = createCollectionList(pipeline, dataType, inputs);
                final Flatten<Mutation> transform = new Flatten(parameters);
                final PCollection<Mutation> output = list.getValue().apply(config.getName(), transform);
                return FCollection.update(list.getKey(), output);
            }
            default:
                throw new IllegalArgumentException("Not supported output data typ: " + dataType.name());

        }
    }

    public static class Flatten<T> extends PTransform<PCollectionList<T>, PCollection<T>> {

        private final FlattenTransformParameters parameters;

        public FlattenTransformParameters getParameters() {
            return parameters;
        }

        private Flatten(final FlattenTransformParameters parameters) {
            this.parameters = parameters;
            validate();
        }

        @Override
        public PCollection<T> expand(final PCollectionList<T> inputs) {
            return inputs.apply("Flatten", org.apache.beam.sdk.transforms.Flatten.pCollections());
        }

        private void validate() {
            if(this.parameters == null && this.parameters.getDataType() == null) {
                //throw new IllegalArgumentException("Flatten module required dataType parameter!");
            }
        }

    }

    private static DataType selectDataType(
            final FlattenTransformParameters parameters,
            final List<FCollection<?>> inputs) {

        if(parameters.getDataType() != null) {
            return DataType.valueOf(parameters.getDataType().toUpperCase());
        }
        return inputs.get(0).getDataType();
    }

    private static <T> KV<FCollection<T>, PCollectionList<T>> createCollectionList(
            final Pipeline pipeline,
            final DataType dataType,
            final List<FCollection<?>> inputs) {

        if(inputs.size() == 0) {
            return null;
        }

        PCollectionList<T> list = PCollectionList.empty(pipeline);
        FCollection<T> outputCollection = null;
        for(final FCollection<?> input : inputs) {
            final DataTypeTransform.TypeTransform<T> transform = DataTypeTransform.transform(input, dataType);
            final PCollection<T> row = input.getCollection()
                    .apply("ConvertTo" + dataType.name(), transform);
            list = list.and(row);
            outputCollection = transform.getOutputCollection();
        }
        return KV.of(outputCollection, list);
    }

}
