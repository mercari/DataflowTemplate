package com.mercari.solution.module.transform;

import com.google.common.base.Functions;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.sql.udf.JsonFunctions;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BeamSQLTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(BeamSQLTransform.class);

    private class BeamSQLTransformParameters {

        private String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }

    public String getName() { return "beamsql"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return Collections.singletonMap(config.getName(), BeamSQLTransform.transform(inputs, config));
    }

    public static FCollection<Row> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        PCollectionTuple beamsqlInputs = PCollectionTuple.empty(inputs.get(0).getCollection().getPipeline());
        for(final FCollection<?> input : inputs){
            beamsqlInputs = beamsqlInputs.and(input.getName(), input.getCollection());
        }

        final Map<String,FCollection<?>> inputCollections = inputs.stream()
                .collect(Collectors.toMap(FCollection::getName, Functions.identity()));

        final SQLTransform transform = new SQLTransform(config, inputCollections);
        final PCollection<Row> output = beamsqlInputs.apply(config.getName(), transform);
        return FCollection.of(config.getName(), output, DataType.ROW, output.getSchema());
    }

    public static class SQLTransform extends PTransform<PCollectionTuple, PCollection<Row>> {

        private final BeamSQLTransformParameters parameters;
        private final Map<String,FCollection<?>> inputCollections;

        public BeamSQLTransformParameters getParameters() {
            return parameters;
        }

        private SQLTransform(final TransformConfig config, final Map<String,FCollection<?>> inputCollections) {
            this.parameters = new Gson().fromJson(config.getParameters(), BeamSQLTransformParameters.class);
            this.inputCollections = inputCollections;
            validate();
        }

        @Override
        public PCollection<Row> expand(final PCollectionTuple list) {
            PCollectionTuple beamsqlInputs = PCollectionTuple.empty(list.getPipeline());
            for(final Map.Entry<TupleTag<?>, PCollection<?>> input : list.getAll().entrySet()) {
                final FCollection<?> inputCollection = this.inputCollections.get(input.getKey().getId());
                final PCollection<Row> row = input.getValue()
                        .apply("Convert" + input.getKey().getId() + "ToRow",
                                DataTypeTransform.transform(inputCollection, DataType.ROW));

                beamsqlInputs = beamsqlInputs.and(input.getKey().getId(), row);
            }

            final String query;
            if(parameters.getSql().startsWith("gs://")) {
                query = StorageUtil.readString(parameters.getSql());
            } else {
                query = parameters.getSql();
            }

            return beamsqlInputs.apply("SQLTransform", SqlTransform
                    .query(query)
                    .registerUdf("JSON_EXTRACT", JsonFunctions.JsonExtractFn.class));
        }

        private void validate() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("BeamSQL module parameter missing!");
            }
            if(this.parameters.getSql() == null) {
                throw new IllegalArgumentException("BeamSQL module required sql parameter!");
            }
        }

    }

}
