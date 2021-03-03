package com.mercari.solution.module.transform;

import com.google.cloud.automl.v1beta1.*;
import com.google.gson.Gson;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.converter.RowToAutoMLRowConverter;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class AutoMLTablesTransform implements TransformModule {

    private class AutoMLTablesTransformParameters {

        private String project;
        private String location;
        private String modelId;

        public String getProject() {
            return project;
        }

        public void setProject(String project) {
            this.project = project;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getModelId() {
            return modelId;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }
    }

    public String getName() { return "automl"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        if(config.getInputs().size() != 1) {
            throw new IllegalArgumentException("module mlengine must not be multi input !");
        }
        //rows.put(transformName, rows.get(config.getInputs().get(0)).apply(AutoMLTablesTransform.process(config)));
        return Collections.emptyMap();
    }

    public static AutoMLPredictProcess process(final TransformConfig config) {
        return new AutoMLPredictProcess(config);
    }

    public static class AutoMLPredictProcess extends PTransform<PCollection<Row>, PCollection<Row>> {

        private final TransformConfig config;
        private final AutoMLTablesTransformParameters parameters;

        private AutoMLPredictProcess(final TransformConfig config) {
            this.config = config;
            this.parameters = new Gson().fromJson(config.getParameters(), AutoMLTablesTransformParameters.class);
        }

        public PCollection<Row> expand(final PCollection<Row> input) {
            //final String project = parameters.getProject() == null ?
            //        input.getPipeline().getOptions().as(DataflowPipelineOptions.class).getProject() : parameters.getProject();
            final String project = parameters.getProject();
            final String location = parameters.getLocation();
            final String modelId = parameters.getModelId();
            return input
                    .apply(config.getName(), ParDo.of(new PredictDoFn(project, location, modelId, input.getSchema())))
                    .setCoder(RowCoder.of(input.getSchema()));
        }

        private void validate() {

        }

    }

    private static class PredictDoFn extends DoFn<Row, Row> {

        private static final Logger LOG = LoggerFactory.getLogger(PredictDoFn.class);

        private final String project;
        private final String location;
        private final String modelId;

        private final Schema outputSchema;
        private final Schema predictionSchema;

        private transient Model model;
        private transient ModelName modelName;
        private transient PredictionServiceClient client;
        private transient ColumnSpec targetColumnSpec;
        private transient List<ColumnSpec> columnSpecs;

        private PredictDoFn(final String project, final String location, final String modelId, final Schema inputSchema) {
            this.project = project;
            this.location = location;
            this.modelId = modelId;

            final List<Schema.Field> predictionFields = new ArrayList<>();
            this.predictionSchema = Schema.builder()
                    .addField(Schema.Field.of("value", Schema.FieldType.STRING).withNullable(true))
                    .addField(Schema.Field.of("score", Schema.FieldType.FLOAT).withNullable(true))
                    .addField(Schema.Field.of("predictionInterval", Schema.FieldType.row(Schema.builder()
                            .addField(Schema.Field.of("start", Schema.FieldType.FLOAT))
                            .addField(Schema.Field.of("end", Schema.FieldType.FLOAT))
                            .build())).withNullable(true))
                    .addField(Schema.Field.of("tablesModelColumnInfo", Schema.FieldType.array(Schema.FieldType.row(Schema.builder()
                            .addField(Schema.Field.of("columnSpecName", Schema.FieldType.STRING))
                            .addField(Schema.Field.of("columnDisplayName", Schema.FieldType.STRING))
                            .addField(Schema.Field.of("featureImportance", Schema.FieldType.FLOAT))
                            .build()))).withNullable(true))
                    .build();
            predictionFields.add(Schema.Field.of("predictions", Schema.FieldType.array(Schema.FieldType.row(this.predictionSchema))));
            this.outputSchema = RowSchemaUtil.addSchema(inputSchema, predictionFields);
        }

        @Setup
        public void setup() throws IOException {
            this.modelName = ModelName.of(project, location, modelId);
            this.model = AutoMlClient.create().getModel(modelName);
            this.client = PredictionServiceClient.create();
            this.targetColumnSpec = model.getTablesModelMetadata().getTargetColumnSpec();
            this.columnSpecs = model.getTablesModelMetadata().getInputFeatureColumnSpecsList();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Row input = c.element();
            if(input == null) {
                return;
            }
            final com.google.cloud.automl.v1beta1.Row feature = RowToAutoMLRowConverter.convert(this.columnSpecs, input);
            final PredictResponse response = client
                    .predict(PredictRequest.newBuilder()
                            .setName(modelName.toString())
                            .setPayload(ExamplePayload.newBuilder()
                                    .setRow(feature)
                                    .build())
                            .putParams("feature_importance", "false")
                            .build());

            final List<Row> predictions = new ArrayList<>();
            for(final AnnotationPayload payload : response.getPayloadList()) {
                final TablesAnnotation table = payload.getTables();
                final Row row = Row.withSchema(this.predictionSchema)
                        .addValue(Value.newBuilder().setStringValue(table.getValue().getStringValue()).build()) // value
                        .addValue(Value.newBuilder().setNumberValue(table.getScore()).build()) // score
                        .addValue(Value.newBuilder().setStructValue(Struct.newBuilder() // predictionInterval
                                .putFields("start", Value.newBuilder().setNumberValue(table.getPredictionInterval().getStart()).build())
                                .putFields("end", Value.newBuilder().setNumberValue(table.getPredictionInterval().getEnd()).build())
                                .build()).build())
                        .addValue(Value.newBuilder().setListValue(ListValue.newBuilder() // tablesModelColumnInfo
                                .addAllValues(table.getTablesModelColumnInfoList().stream()
                                        .map(info -> Value.newBuilder().setStructValue(Struct.newBuilder()
                                                .putFields("columnSpecName", Value.newBuilder().setStringValue(info.getColumnSpecName()).build())
                                                .putFields("columnDisplayName", Value.newBuilder().setStringValue(info.getColumnDisplayName()).build())
                                                .putFields("featureImportance", Value.newBuilder().setNumberValue(info.getFeatureImportance()).build())
                                                .build()).build())
                                        .collect(Collectors.toList()))
                                .build()).build())
                        .build();
                predictions.add(row);
            }
            final Row result = Row.withSchema(this.outputSchema)
                    .addValues(input.getValues())
                    .addArray(predictions)
                    .build();
            c.output(result);
        }

    }

}
