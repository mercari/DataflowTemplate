package com.mercari.solution.util.domain.ml;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LinearModelUtil implements Serializable {

    public static LinearModel olsModel(double[][] X, double[][] Y) {
        final RealMatrix beta = ols(X, Y);
        return LinearModel.of(beta);
    }

    public static RealMatrix ols(double[][] X, double[][] Y) {
        final RealMatrix matrixX = MatrixUtils.createRealMatrix(X);
        final RealMatrix matrixY = MatrixUtils.createRealMatrix(Y);
        return ols(matrixX, matrixY);
    }

    private static RealMatrix ols(RealMatrix X, RealMatrix Y) {
        return MatrixUtils
                .inverse(X
                        .transpose()
                        .multiply(X))
                .multiply(X.transpose())
                .multiply(Y);
    }

    public static LinearModel ridgeModel(double[][] X, double[][] Y, double alpha) {
        final RealMatrix beta = ridge(X, Y, alpha);
        return LinearModel.of(beta);
    }

    public static RealMatrix ridge(double[][] X, double[][] Y, double alpha) {
        final RealMatrix matrixX = MatrixUtils.createRealMatrix(X);
        final RealMatrix matrixY = MatrixUtils.createRealMatrix(Y);
        return ridge(matrixX, matrixY, alpha);
    }

    private static RealMatrix ridge(RealMatrix X, RealMatrix Y, double alpha) {
        final RealMatrix I = MatrixUtils
                .createRealIdentityMatrix(X.getColumnDimension());
        return MatrixUtils
                .inverse(X
                        .transpose()
                        .multiply(X)
                        .add(I.scalarMultiply(alpha)))
                .multiply(X.transpose())
                .multiply(Y);
    }

    public static LinearModel lassoModel(double[][] X, double[][] Y,
                                         double alpha, int maxIteration, double tolerance) {
        final RealMatrix beta = lasso(X, Y, alpha, maxIteration, tolerance);
        return LinearModel.of(beta);
    }

    public static RealMatrix lasso(double[][] X, double[][] Y,
                                   double alpha, int maxIteration, double tolerance) {
        final RealMatrix matrixX = MatrixUtils.createRealMatrix(X);
        final RealMatrix matrixY = MatrixUtils.createRealMatrix(Y);
        return lasso(matrixX, matrixY, alpha, maxIteration, tolerance);
    }

    private static RealMatrix lasso(RealMatrix X, RealMatrix Y, double alpha, int maxIteration, double tolerance) {
        RealMatrix beta = randomMatrix(Y.getColumnDimension(), X.getColumnDimension());

        for(int i=0; i<maxIteration; i++) {
            final RealMatrix B = MatrixUtils.createRealDiagonalMatrix(beta.getRow(0));
            final RealMatrix pinv = new SingularValueDecomposition(abs(B)).getSolver().getInverse();
            final RealMatrix newBeta = MatrixUtils
                    .inverse(X
                            .transpose()
                            .multiply(X)
                            .add(pinv.scalarMultiply(alpha)))
                    .multiply(X.transpose())
                    .multiply(Y);
            final double epsilon = (beta.subtract(newBeta.transpose())).getNorm();
            if(epsilon < tolerance) {
                break;
            }
            beta = newBeta.transpose();
        }

        return beta.transpose();
    }

    public static RealMatrix pls1(double[][] X, double[] y, int components) {

        RealMatrix matrixX = MatrixUtils.createRealMatrix(X);
        RealMatrix matrixY = MatrixUtils.createColumnRealMatrix(y);

        final RealMatrix W = MatrixUtils.createRealMatrix(matrixX.getColumnDimension(), components);
        final RealMatrix P = MatrixUtils.createRealMatrix(matrixX.getColumnDimension(), components);
        final RealMatrix D = MatrixUtils.createRealMatrix(components, 1);
        final RealMatrix T = MatrixUtils.createRealMatrix(matrixX.getRowDimension(), components);

        for(int r=0; r<components; r++) {
            final RealMatrix xy = matrixX.transpose().multiply(matrixY);
            final RealMatrix w = xy.scalarMultiply(1 / xy.getFrobeniusNorm());
            final RealMatrix t = matrixX.multiply(w);
            final double a = 1 / t.transpose().multiply(t).getEntry(0, 0);
            final RealMatrix p = matrixX.transpose().multiply(t).scalarMultiply(a);
            final RealMatrix d = t.transpose().multiply(matrixY).scalarMultiply(a);

            matrixX = matrixX.subtract(t.multiply(p.transpose()));
            matrixY = matrixY.subtract(t.multiply(d));

            W.setColumnMatrix(r, w);
            P.setColumnMatrix(r, p);
            D.setRowMatrix(r, d.transpose());
            T.setColumnMatrix(r, t);
        }
        return W.multiply(MatrixUtils.inverse(P.transpose().multiply(W)))
                .multiply(D);
    }

    public static LinearModel pls2Model(double[][] X, double[][] Y, int components) {
        final RealMatrix beta = pls2(X, Y, components);
        return LinearModel.of(beta);
    }

    public static RealMatrix pls2(double[][] X, double[][] Y, int components) {

        RealMatrix matrixX = MatrixUtils.createRealMatrix(X);
        RealMatrix matrixY = MatrixUtils.createRealMatrix(Y);

        final int xd = matrixX.getColumnDimension();
        final int yd = matrixY.getColumnDimension();

        final RealMatrix W = MatrixUtils.createRealMatrix(xd, components);
        final RealMatrix P = MatrixUtils.createRealMatrix(xd, components);
        final RealMatrix Q = MatrixUtils.createRealMatrix(yd, components);
        final RealMatrix T = MatrixUtils.createRealMatrix(matrixX.getRowDimension(), components);
        final RealMatrix U = MatrixUtils.createRealMatrix(matrixY.getRowDimension(), components);

        final RealMatrix ssq = MatrixUtils.createRealMatrix(components, 2);
        final double ssqX = sum(matrixX, 2);
        final double ssqY = sum(matrixY, 2);

        //final List<LinearModel> plsList = new ArrayList<>();
        for(int r=0; r<components; r++) {
            final SingularValueDecomposition svd = new SingularValueDecomposition(matrixY.transpose().multiply(matrixX));
            final RealMatrix w = svd.getV().getColumnMatrix(0);
            final RealMatrix t = matrixX.multiply(w);
            final double a = 1 / t.transpose().multiply(t).getEntry(0, 0);
            final RealMatrix p = matrixX.transpose().multiply(t).scalarMultiply(a);
            final RealMatrix q = matrixY.transpose().multiply(t).scalarMultiply(a);
            final RealMatrix u = matrixY.multiply(q);

            matrixX = matrixX.subtract(t.multiply(p.transpose()));
            matrixY = matrixY.subtract(t.multiply(q.transpose()));

            ssq.setEntry(r, 0, (sum(matrixX, 2) / ssqX));
            ssq.setEntry(r, 1, (sum(matrixY, 2) / ssqY));

            W.setColumnMatrix(r, w);
            P.setColumnMatrix(r, p);
            Q.setColumnMatrix(r, q);
            T.setColumnMatrix(r, t);
            U.setColumnMatrix(r, u);

            /*
            if(r > 0) {
                final RealMatrix beta = W.getSubMatrix(0, xd-1, 0, r-1)
                        .multiply(MatrixUtils.inverse(
                                P.getSubMatrix(0, xd-1, 0, r-1).transpose().multiply(W.getSubMatrix(0, xd-1, 0, r-1))))
                        .multiply(Q.getSubMatrix(0, yd-1, 0, r-1).transpose());
                plsList.add(new LinearModel(beta));
            }
             */
        }
        //plsList.add(new LinearModel(beta));

        return W.multiply(MatrixUtils.inverse(P.transpose().multiply(W)))
                .multiply(Q.transpose());
    }


    public static RealMatrix logisticRegressionBinary(RealMatrix X, RealMatrix Y, int maxIteration, double tolerance) {
        double alpha = 1D;
        RealMatrix beta = randomMatrix(Y.getColumnDimension(), X.getColumnDimension());
        RealMatrix gamma = randomMatrix(Y.getColumnDimension(), X.getColumnDimension());

        for(int i=0; i<maxIteration; i++) {

            beta = gamma.copy();
            RealMatrix S = X.multiply(beta);
            RealMatrix V = S.scalarMultiply(-1).multiply(Y);
            RealMatrix U = Y.multiply(V).multiply(V.scalarAdd(1D));

            final RealMatrix B = MatrixUtils.createRealDiagonalMatrix(beta.getRow(0));
            final RealMatrix pinv = new SingularValueDecomposition(abs(B)).getSolver().getInverse();
            final RealMatrix newBeta = MatrixUtils
                    .inverse(X
                            .transpose()
                            .multiply(X)
                            .add(pinv.scalarMultiply(alpha)))
                    .multiply(X.transpose())
                    .multiply(Y);
            final double epsilon = (beta.subtract(newBeta.transpose())).getNorm();
            if(epsilon < tolerance) {
                break;
            }
            beta = newBeta.transpose();
        }

        return beta.transpose();
    }

    public static RealMatrix logisticRegressionMulti(double[][] X, double[][] Y) {
        return null;
    }

    public static List<List<Double>> calcStandardizeParams(double[][] data) {
        if(data == null || data.length == 0 || data[0].length == 0) {
            return new ArrayList<>();
        }
        final List<List<Double>> centers = new ArrayList<>();
        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            double avg = 0;
            double var = 0;
            double count = 0;
            for(int row=0; row<data.length; row++) {
                double value = data[row][col];
                count += 1;
                double delta = value - avg;
                avg += (delta / count);
                var += (delta * (value - avg));
            }
            final double std = Math.sqrt(var / count);
            final List<Double> center = new ArrayList<>();
            center.add(avg);
            center.add(std);
            center.add(var / count);
            centers.add(center);
        }
        return centers;
    }

    public static double[][] standardize(double[][] data, List<List<Double>> params) {
        return standardize(data, params, false);
    }
    public static double[][] standardize(double[][] data, List<List<Double>> params, boolean skipStd) {

        if(data == null) {
            throw new RuntimeException();
        } else if(data.length == 0 || data[0].length == 0) {
            //return new double[0][];
            throw new RuntimeException();
        }

        final double[][] standardized = new double[data.length][data[0].length];

        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            double avg = params.get(col).get(0);
            double std = params.get(col).get(1);
            for(int row=0; row<data.length; row++) {
                double value = data[row][col];
                if(std == 0) {
                    standardized[row][col] = value;
                } else if(skipStd) {
                    standardized[row][col] = (value - avg);
                } else {
                    standardized[row][col] = (value - avg) / std;
                }
            }
        }

        return standardized;
    }

    public static double[][] addIntercept(double[][] data) {

        if(data == null) {
            return null;
        } else if(data.length == 0 || data[0].length == 0) {
            return new double[0][];
        }

        final double[][] standardized = new double[data.length][data[0].length + 1];

        final int cols = data[0].length;
        for(int col=0; col<cols; col++) {
            for(int row=0; row<data.length; row++) {
                standardized[row][col] = data[row][col];
            }
        }
        for(int row=0; row<data.length; row++) {
            standardized[row][cols] = data[row][cols];
        }

        return standardized;
    }

    public static class LinearModel implements Model {

        private final int inputSize;
        private final int outputSize;
        private List<List<Double>> weights;

        public int getInputSize() {
            return inputSize;
        }

        public int getOutputSize() {
            return outputSize;
        }

        public List<List<Double>> getWeights() {
            return weights;
        }

        public RealMatrix getBeta() {
            double[][] bs = new double[weights.size()][weights.get(0).size()];
            for(int i=0; i<bs.length; i++) {
                for(int j=0; j<bs[0].length; j++) {
                    bs[i][j] = weights.get(i).get(j);
                }
            }
            return MatrixUtils.createRealMatrix(bs);
        }

        public void setWeights(List<List<Double>> weights) {
            this.weights = weights;
        }

        public void setWeights(double[][] beta) {
            this.weights.clear();
            for(int i=0; i<beta.length; i++) {
                final List<Double> b = new ArrayList<>();
                for(int j=0; j<beta[0].length; j++) {
                    b.add(beta[i][j]);
                }
                this.weights.add(b);
            }
        }



        LinearModel(final int inputSize, final int outputSize, final List<List<Double>> weights) {
            this.inputSize = inputSize;
            this.outputSize = outputSize;
            this.weights = weights;
        }

        LinearModel(RealMatrix beta) {
            this.inputSize = beta.getRowDimension();
            this.outputSize = beta.getColumnDimension();
            this.weights = new ArrayList<>();
            for(int output=0; output<this.outputSize; output++) {
                final List<Double> b = new ArrayList<>();
                for(int input=0; input<this.inputSize; input++) {
                    b.add(beta.getEntry(input, output));
                }
                weights.add(b);
            }
        }

        public static LinearModel of(final RealMatrix beta) {
            return new LinearModel(beta);
        }

        public static LinearModel of(final int inputSize, final int outputSize, final List<List<Double>> weights) {
            final LinearModel model = new LinearModel(inputSize, outputSize, weights);
            return model;
        }

        @Override
        public void update(List<String> fields, Map<String, Double> values) {

        }

        @Override
        public List<Double> inference(double[] x) {
            final List<Double> results = new ArrayList<>();
            for(final List<Double> b : weights) {
                double r = 0;
                for(int i=0; i<x.length; i++) {
                    r += (b.get(i) * x[i]);
                }
                if(b.size() > x.length) {
                    r += b.get(b.size() - 1);
                }
                results.add(r);
            }
            return results;
        }

        @Override
        public List<Double> inference(List<String> fields, Map<String, Double> values) {
            final List<Double> results = new ArrayList<>();
            for(final List<Double> b : weights) {
                double r = 0;
                for(int i=0; i<fields.size(); i++) {
                    final Double v = values.get(fields.get(i));
                    r += (b.get(i) * v);
                }
                if(b.size() > fields.size()) {
                    r += b.get(b.size() - 1);
                }
                results.add(r);
            }
            return results;
        }

        @Override
        public String toString() {
            return "input size: " + inputSize + ", output size: " + outputSize + ", weights: " + weights.toString();
        }

    }

    public static String toString(double[] values) {
        final List<Double> doubles = new ArrayList<>();
        for(double d : values) {
            doubles.add(d);
        }
        return doubles.toString();
    }

    private static double sum(RealMatrix matrix, int pow) {
        double sum = 0;
        for(int row=0; row<matrix.getRowDimension(); row++) {
            for(int col=0; col<matrix.getColumnDimension(); col++) {
                sum += Math.pow(matrix.getEntry(row, col), pow);
            }
        }
        return sum;
    }

    private static RealMatrix randomMatrix(int row, int col) {
        final double[][] values = new double[row][col];
        for(int i=0; i<row; i++) {
            for(int j=0; j<col; j++) {
                values[i][j] = Math.random();
            }
        }
        return MatrixUtils.createRealMatrix(values);
    }

    private static RealMatrix abs(RealMatrix matrix) {
        for(int i=0; i<matrix.getRowDimension(); i++) {
            for(int j=0; j<matrix.getColumnDimension(); j++) {
                double value = matrix.getEntry(i, j);
                if(value < 0) {
                    matrix.setEntry(i, j, -value);
                }
            }
        }
        return matrix;
    }

    private static double[][] toMatrix(double[] vector) {
        double[][] matrix = new double[vector.length][1];
        for(int i=0; i<vector.length; i++) {
            matrix[i] = new double[]{vector[i]};
        }
        return matrix;
    }

}
