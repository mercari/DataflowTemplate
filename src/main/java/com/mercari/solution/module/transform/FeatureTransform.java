package com.mercari.solution.module.transform;

import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.RecordToFeatureRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FeatureTransform implements TransformModule {

    private class FeatureTransformParameters {

        private String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }

    public String getName() { return "feature"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return Collections.singletonMap(config.getName(), FeatureTransform.transform(inputs, config));
    }

    public static FCollection<GenericRecord> transform(final List<FCollection<?>> inputs, final TransformConfig config) {
        final FeatureTrans transform = new FeatureTrans(config, inputs.get(0));
        final PCollection<GenericRecord> output = inputs.get(0).getCollection().apply(config.getName(), transform);
        return FCollection.of(config.getName(), output, DataType.AVRO, transform.outputSchema);
    }

    public static class FeatureTrans extends PTransform<PCollection<?>, PCollection<GenericRecord>> {

        private final FeatureTransformParameters parameters;
        private final FCollection<?> collection;
        private final Schema outputSchema;

        public FeatureTransformParameters getParameters() {
            return parameters;
        }

        private FeatureTrans(final TransformConfig config, final FCollection<?> collection) {
            this.parameters = new Gson().fromJson(config.getParameters(), FeatureTransformParameters.class);
            this.collection = collection;
            this.outputSchema = RecordToFeatureRecordConverter.convertSchema(collection.getAvroSchema());
        }

        @Override
        public PCollection<GenericRecord> expand(final PCollection<?> input) {
            final PCollection<GenericRecord> records = input
                    .apply("Convert" + input.getName() + "ToRecord",
                            DataTypeTransform.transform(collection, DataType.AVRO));

            //PCollectionView<String> modelView = records
            //        .apply("", Combine.globally(new SummaryCombineFn()))
            //        .apply("", View.asSingleton());

            return records
                    .apply(ParDo.of(new ConvertDoFn(outputSchema.toString(), null)))
                    .setCoder(AvroCoder.of(outputSchema));
        }
    }

    private static class SummaryNumberCombineFn<T extends Number> extends Combine.CombineFn<T, SummaryNumber, SummaryNumber> {

        @Override
        public SummaryNumber createAccumulator() {
            return SummaryNumber.EMPTY;
        }

        @Override
        public SummaryNumber addInput(final SummaryNumber summary, final T value) {
            if(value == null) {
                return summary;
            }


            return summary;
        }

        @Override
        public SummaryNumber mergeAccumulators(Iterable<SummaryNumber> summaries) {
            return null;
        }

        @Override
        public SummaryNumber extractOutput(final SummaryNumber summary) {
            return summary;
        }

    }

    private static class SummaryNumber implements Serializable {

        private static final SummaryNumber EMPTY = new SummaryNumber();
        private static final MathContext MATH_CONTEXT = new MathContext(10, RoundingMode.HALF_UP);

        private String name;

        private BigDecimal count;
        private BigDecimal sum;
        private BigDecimal variance;

        private SummaryNumber combineWith(SummaryNumber other) {
            return null;
        }

        private BigDecimal calculateIncrement(SummaryNumber x, SummaryNumber y) {
            BigDecimal m = x.count;
            BigDecimal n = y.count;
            BigDecimal sumN = x.sum;
            BigDecimal sumM = y.sum;

            BigDecimal multiplier = m.divide(n.multiply(m.add(n)), MATH_CONTEXT);

            BigDecimal square = (sumN.multiply(n).divide(m, MATH_CONTEXT)).subtract(sumM).pow(2);
            return multiplier.multiply(square);
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj == null) {
                return false;
            }
            if(getClass() != obj.getClass()) {
                return false;
            }

            SummaryNumber other = (SummaryNumber) obj;

            if(count == null) {
                if(other.count != null) {
                    return false;
                }
            } else if(!count.equals(other.count)) {
                return false;
            }

            if(sum == null) {
                if(other.sum != null) {
                    return false;
                }
            } else if(!sum.equals(other.sum)) {
                return false;
            }

            if(variance == null) {
                if(other.variance != null) {
                    return false;
                }
            } else if(!variance.equals(other.variance)) {
                return false;
            }

            return true;
        }

    }

    private static class SummaryCategory implements Serializable {

        private String name;

        private Long count;
        private Map<String, Long> counts;

    }

    private static class ConvertDoFn extends DoFn<GenericRecord, GenericRecord> {

        private final String schemaString;
        private final PCollectionView<String> modelView;

        private transient Schema schema;
        private transient OrtEnvironment environment;
        private transient OrtSession session;


        public ConvertDoFn(final String schemaString, final PCollectionView<String> modelView) {
            this.schemaString = schemaString;
            this.modelView = modelView;
        }

        @Setup
        public void setup() {
            this.schema = AvroSchemaUtil.convertSchema(schemaString);
            this.environment = OrtEnvironment.getEnvironment();
            //this.session = this.environment.createSession();
            this.session = null;
        }

        @StartBundle
        public void startBundle(StartBundleContext c) {

        }

        @ProcessElement
        public void processElement(ProcessContext c) throws OrtException {
            /*
            if(session == null) {
                final OrtEnvironment environment = OrtEnvironment.getEnvironment();
                final OrtSession.SessionOptions sessionOptions = new OrtSession.SessionOptions();
                sessionOptions.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);
                //sessionOptions.setExecutionMode(OrtSession.SessionOptions.ExecutionMode.PARALLEL);
                this.session = environment.createSession(c.sideInput(modelView).getBytes(), sessionOptions);
            }
            */

            c.output(RecordToFeatureRecordConverter.convert(schema, c.element()));
        }

    }
}