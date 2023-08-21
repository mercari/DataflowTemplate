package com.mercari.solution.util.pipeline.processing.processor.learner;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.ml.LinearModelUtil;
import com.mercari.solution.util.domain.ml.Model;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.schema.RowSchemaUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinearRegression extends Learner {

    private static final String STATE_MODEL_FORMAT = "%s.weights.%d";

    private final RegularizationType regularizationType;

    private final Double alpha; // parameter for ridge, lasso
    private final Integer maxIteration; // parameter for lasso
    private final Double tolerance; // parameter for lasso
    private final Integer components; // parameter for pls


    public static LinearRegression of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new LinearRegression(name, condition, ignore, params);
    }

    LinearRegression(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        super(name, condition, ignore, params);

        if(params.has("regularizationType") && params.get("regularizationType").isJsonPrimitive()) {
            regularizationType = RegularizationType.valueOf(params.get("regularizationType").getAsString());
        } else {
            regularizationType = RegularizationType.none;
        }

        if(params.has("alpha")) {
            final JsonElement element = params.get("alpha");
            if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("linear_regression step: " + name + ", parameter alpha must be numeric. got: " + element);
            }
            alpha = element.getAsDouble();
        } else {
            alpha = 0.2;
        }

        if(params.has("maxIteration")) {
            final JsonElement element = params.get("maxIteration");
            if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("linear_regression step: " + name + ", parameter maxIteration must be numeric. got: " + element);
            }
            maxIteration = element.getAsInt();
        } else {
            maxIteration = 500;
        }

        if(params.has("tolerance")) {
            final JsonElement element = params.get("tolerance");
            if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("linear_regression step: " + name + ", parameter tolerance must be numeric. got: " + element);
            }
            tolerance = element.getAsDouble();
        } else {
            tolerance = 0.01;
        }

        if(params.has("components")) {
            final JsonElement element = params.get("components");
            if(!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("linear_regression step: " + name + ", parameter components must be numeric. got: " + element);
            }
            components = element.getAsInt();
        } else {
            components = 2;
        }
    }

    @Override
    public Op getOp() {
        return Op.linear_regression;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final List<Schema.Field> outputFields = new ArrayList<>();
        if(this.targetFields.size() > 0) {
            for(String targetField : targetFields) {
                final Schema.FieldType targetFieldType = inputTypes.get(targetField);
                if(RowSchemaUtil.isLogicalTypeEnum(targetFieldType)) {
                    for(final Integer horizon : this.horizons) {
                        final String outputFieldName = createOutputName(targetField, horizon);
                        outputFields.add(Schema.Field.of(outputFieldName, Schema.FieldType.DOUBLE.withNullable(true)));
                    }
                } else {
                    for(final Integer horizon : this.horizons) {
                        final String outputFieldName = createOutputName(targetField, horizon);
                        outputFields.add(Schema.Field.of(outputFieldName, Schema.FieldType.DOUBLE.withNullable(true)));
                    }
                }
            }
        } else if(this.targetExpressions.size() > 0) {
            for(int i = 0; i< targetExpressions.size(); i++) {
                for(final Integer horizon : this.horizons) {
                    final String outputFieldName = createOutputName(i, horizon);
                    outputFields.add(Schema.Field.of(outputFieldName, Schema.FieldType.DOUBLE.withNullable(true)));
                }
            }
        } else {
            for(final Integer horizon : this.horizons) {
                final String outputFieldName = createOutputName(horizon);
                outputFields.add(Schema.Field.of(outputFieldName, Schema.FieldType.DOUBLE.withNullable(true)));
            }
        }
        return outputFields;
    }

    @Override
    Model train(double[][] X, double[][] Y) {
        switch (this.trainType) {
            case online: {
                throw new IllegalArgumentException();
            }
            case minibatch: {
                final List<List<Double>> paramsX;
                final List<List<Double>> paramsY;
                if(standardize) {
                    paramsX = LinearModelUtil.calcStandardizeParams(X);
                    paramsY = LinearModelUtil.calcStandardizeParams(Y);
                    X = LinearModelUtil.standardize(X, paramsX);
                    Y = LinearModelUtil.standardize(Y, paramsY, true);
                } else {
                    paramsX = null;
                    paramsY = null;
                }
                final long startMillis = Instant.now().getMillis();
                final RealMatrix beta;
                try {
                    switch (this.regularizationType) {
                        case none:
                            beta = LinearModelUtil.ols(X, Y);
                            break;
                        case ridge:
                            beta = LinearModelUtil.ridge(X, Y, alpha);
                            break;
                        case lasso:
                            beta = LinearModelUtil.lasso(X, Y, alpha, maxIteration, tolerance);
                            break;
                        case pls:
                            beta = LinearModelUtil.pls2(X, Y, components);
                            break;
                        default:
                            throw new IllegalArgumentException("Not supported regularizationType: " + regularizationType);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to train minibatch regularizationType: " + regularizationType, e);
                }

                final LinearModelUtil.LinearModel linearModel;
                if(standardize) {
                    // de-standardize beta
                    double[][] bs = new double[beta.getRowDimension() + 1][beta.getColumnDimension()];
                    for(int y=0; y<beta.getColumnDimension(); y++) {
                        final List<Double> param = paramsX.get(y);
                        double s = 0;
                        for(int col=0; col<beta.getRowDimension(); col++) {
                            final Double avg = param.get(0);
                            final Double std = param.get(1);
                            final Double newWeight = beta.getEntry(col, y) / std;
                            bs[col][y] = newWeight;
                            s += (newWeight * avg);
                        }
                        bs[beta.getRowDimension()][y] = paramsY.get(y).get(0) - s;
                    }
                    final RealMatrix newBeta = MatrixUtils.createRealMatrix(bs);
                    linearModel = LinearModelUtil.LinearModel.of(newBeta);
                } else {
                    linearModel = LinearModelUtil.LinearModel.of(beta);
                }

                LOG.info(String.format(
                        "Trained linear model: %s, regularization: %s, took [%d] millis for data size: %d, details: %s",
                        name, regularizationType, Instant.now().getMillis() - startMillis, X.length, linearModel));

                return linearModel;
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    void save(Model model, ProcessingState state, int horizon) {
        final String stateNameModel = String.format(STATE_MODEL_FORMAT, name, horizon);
        state.linearModels.put(stateNameModel, (LinearModelUtil.LinearModel) model);
        //state.putMatrix(stateNameModel, ((LinearModelUtil.LinearModel)model).getWeights());
    }

    @Override
    Model load(ProcessingState state, int horizon, int inputSize, int outputSize) {
        final String stateNameModel = String.format(STATE_MODEL_FORMAT, name, horizon);
        /*
        final List<List<Double>> weights = state.getMatrix(stateNameWeights);
        if(weights == null) {
            return null;
        }
        return LinearModelUtil.LinearModel.of(inputSize, outputSize, weights);
         */
        return state.linearModels.get(stateNameModel);
    }

    @Override
    Map<String, Object> predict(Model model, ProcessingBuffer buffer, Integer horizon) {
        final Map<String, Object> outputs = new HashMap<>();
        final List<Double> predictions = model.inference(buffer.getRowVector(featureFields, 0));
        for(int i=0; i<predictions.size(); i++) {
            final String outputName;
            if(targetFields.size() > 0) {
                outputName = createOutputName(targetFields.get(i), horizon);
            } else if(targetExpressions.size() > 0) {
                outputName = createOutputName(i, horizon);
            } else {
                outputName = createOutputName(horizon);
            }

            final Double output = predictions.get(i);
            if(Double.isNaN(output) || Double.isInfinite(output)) {
                //LOG.warn("horizon: " + horizon + " output is NaN");
                outputs.put(outputName, null);
            } else {
                outputs.put(outputName, output);
            }
        }
        return outputs;
    }

    private String createOutputName(final Integer horizon) {
        if(isSingleHorizon) {
            return name;
        } else {
            return String.format("%s%s%d", name, DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX, horizon);
        }
    }

    private String createOutputName(final String targetField, final Integer horizon) {
        if(isSingleTargetField && isSingleHorizon) {
            return name;
        } else if(isSingleTargetField) {
            return String.format("%s_%s%d", name, DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX, horizon);
        } else if(isSingleHorizon) {
            return String.format("%s_%s", name, targetField);
        } else {
            return String.format("%s_%s%s%d", name, targetField, DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX, horizon);
        }
    }

    private String createOutputName(final Integer no, final Integer horizon) {
        return String.format("%s%s%d%s%d", name, DEFAULT_OUTPUT_NAME_EXPRESSION_TARGET_SUFFIX, no, DEFAULT_OUTPUT_NAME_HORIZON_SUFFIX, horizon);
    }

    public enum RegularizationType {
        ridge,
        lasso,
        pls,
        none
    }
}
