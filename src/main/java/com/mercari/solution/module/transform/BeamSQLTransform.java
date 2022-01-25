package com.mercari.solution.module.transform;

import com.google.common.base.Functions;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.sql.udf.*;
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
        private Planner planner;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public Planner getPlanner() {
            return planner;
        }

        public void setPlanner(Planner planner) {
            this.planner = planner;
        }
    }

    public enum Planner {
        zetasql,
        calcite
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
        private final Map<String, Object> templateArgs;

        public BeamSQLTransformParameters getParameters() {
            return parameters;
        }

        private SQLTransform(final TransformConfig config, final Map<String,FCollection<?>> inputCollections) {
            this.parameters = new Gson().fromJson(config.getParameters(), BeamSQLTransformParameters.class);
            this.inputCollections = inputCollections;
            this.templateArgs = config.getArgs();
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
                final String rawQuery = StorageUtil.readString(parameters.getSql());
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else {
                query = parameters.getSql();
            }

            final SqlTransform transform;
            if(parameters.getPlanner() == null) {
                transform = SqlTransform.query(query);
            } else if(Planner.zetasql.equals(parameters.getPlanner())) {
                transform = SqlTransform.query(query)
                        .withQueryPlannerClass(org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner.class);
            } else if(Planner.calcite.equals(parameters.getPlanner())) {
                transform = SqlTransform.query(query)
                        .withQueryPlannerClass(org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.class);
            } else {
                throw new IllegalArgumentException("beamsql planner not supported: " + parameters.getPlanner());
            }

            return beamsqlInputs.apply("SQLTransform", transform
                    // Math UDFs
                    .registerUdf("MDT_GREATEST_INT64", MathFunctions.GreatestInt64Fn.class)
                    .registerUdf("MDT_GREATEST_FLOAT64", MathFunctions.GreatestFloat64Fn.class)
                    .registerUdf("MDT_LEAST_INT64", MathFunctions.LeastInt64Fn.class)
                    .registerUdf("MDT_LEAST_FLOAT64", MathFunctions.LeastFloat64Fn.class)
                    // Array UDFs
                    .registerUdf("MDT_CONTAINS_ALL_INT64", ArrayFunctions.ContainsAllInt64sFn.class)
                    .registerUdf("MDT_CONTAINS_ALL_STRING", ArrayFunctions.ContainsAllStringsFn.class)
                    // UDAFs
                    .registerUdaf("MDT_ARRAY_AGG_INT64", new AggregateFunctions.ArrayAggInt64Fn())
                    .registerUdaf("MDT_ARRAY_AGG_STRING", new AggregateFunctions.ArrayAggStringFn())
                    );
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
