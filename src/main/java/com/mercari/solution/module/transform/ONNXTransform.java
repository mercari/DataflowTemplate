package com.mercari.solution.module.transform;

import ai.onnxruntime.*;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.util.converter.RowToONNXTensorConverter;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ONNXTransform {

    private class OnnxTransformParameters {

        private String model;

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

    }


    public static ONNXProcess process(final TransformConfig config) {
        return new ONNXProcess(config);
    }

    public static class ONNXProcess extends PTransform<PCollection<Row>, PCollection<Row>> {

        private final OnnxTransformParameters parameters;

        private ONNXProcess(final TransformConfig config) {
            this.parameters = new Gson().fromJson(config.getParameters(), OnnxTransformParameters.class);
        }

        public PCollection<Row> expand(final PCollection<Row> row) {
            final byte[] model = StorageUtil.readBytes(parameters.getModel());
            return row.apply(ParDo.of(new InferenceDoFn(model)));
        }

        private void validate() {
            if(this.parameters.getModel() == null) {
                throw new IllegalArgumentException("onnx module required model parameter!");
            }
        }

    }

    private static class InferenceDoFn extends DoFn<Row, Row> {

        private static final Logger LOG = LoggerFactory.getLogger(InferenceDoFn.class);

        private final byte[] model;
        private transient OrtEnvironment environment;
        private transient OrtSession.SessionOptions sessionOptions;
        private transient OrtSession session;

        private transient Collection<NodeInfo> inputInfo;
        private transient Collection<NodeInfo> outputInfo;

        private InferenceDoFn(final byte[] model) {
            this.model = model;
        }

        @Setup
        public void setup() {

        }

        @StartBundle
        public void startBundle(final StartBundleContext c) throws OrtException {
            this.environment = OrtEnvironment.getEnvironment();
            this.sessionOptions = new OrtSession.SessionOptions();
            this.sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);

            this.session = this.environment.createSession(model, this.sessionOptions);
            for(final String name : this.session.getInputNames()) {
                LOG.info("inputName: " + name);
            }
            for(final String name : this.session.getOutputNames()) {
                LOG.info("outputName: " + name);
            }

            this.inputInfo = this.session.getInputInfo().values();
            this.outputInfo = this.session.getOutputInfo().values();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) throws Exception {
            final Row row = c.element();
            final Map<String, OnnxTensor> tensors = RowToONNXTensorConverter.convert(this.environment, this.inputInfo, row);
            final OrtSession.Result result = this.session.run(tensors);
            for(final NodeInfo nodeInfo : this.outputInfo) {
                result.get(nodeInfo.getName());
            }
            //result.get(0).getValue()
        }

        @FinishBundle
        public void finishBundle(final FinishBundleContext c) throws OrtException {
            this.session.close();
            this.sessionOptions.close();
            this.environment.close();
        }

    }

}
