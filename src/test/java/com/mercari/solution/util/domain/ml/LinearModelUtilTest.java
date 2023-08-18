package com.mercari.solution.util.domain.ml;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LinearModelUtilTest {

    private static final double DELTA = 1e-15;

    @Test
    public void test() {
        final double[][] X = getX();
        final double[][] Y = getY();

        final LinearModelUtil.LinearModel ols = LinearModelUtil.olsModel(X, Y);
        final LinearModelUtil.LinearModel ridge = LinearModelUtil.ridgeModel(X, Y, 0.1);
        final LinearModelUtil.LinearModel lasso = LinearModelUtil.lassoModel(X, Y, 0.9, 500, 0.01);
        final LinearModelUtil.LinearModel pls = LinearModelUtil.pls2Model(X, Y, 2);

        Assert.assertEquals(1, ols.getWeights().size());
        Assert.assertEquals(1, ridge.getWeights().size());
        Assert.assertEquals(1, lasso.getWeights().size());
        Assert.assertEquals(1, pls.getWeights().size());

        Assert.assertEquals(3, ols.getWeights().get(0).size());
        Assert.assertEquals(3, ridge.getWeights().get(0).size());
        Assert.assertEquals(3, lasso.getWeights().get(0).size());
        Assert.assertEquals(3, pls.getWeights().get(0).size());

        Assert.assertEquals(Arrays.asList(0.3634706488178612, 0.41624871282274356, -0.3467759307908499), ols.getWeights().get(0));
        Assert.assertEquals(Arrays.asList(0.3398400247536521, 0.37935912658459536, -0.3847992027403095), ridge.getWeights().get(0));
        Assert.assertEquals(Arrays.asList(0.33835661051283145, 0.3691038639153049, -0.4089846505801048), pls.getWeights().get(0));
    }

    @Test
    public void testCalcStandardizeParams() {
        final double[][] X = new double[4][4];
        X[0] = new double[]{  1D,  10D,  90D, 1D};
        X[1] = new double[]{ -1D, -10D, -10D, 1D};
        X[2] = new double[]{  2D,  30D,  50D, 1D};
        X[3] = new double[]{ -2D,  50D,  30D, 1D};

        final List<List<Double>> params = LinearModelUtil.calcStandardizeParams(X);
        Assert.assertEquals(X[0].length, params.size());
        Assert.assertEquals(3, params.get(0).size());
        Assert.assertEquals(3, params.get(1).size());
        Assert.assertEquals(3, params.get(2).size());
        Assert.assertEquals(0.0D,  params.get(0).get(0), DELTA);
        Assert.assertEquals(2.5D,  params.get(0).get(2), DELTA);
        Assert.assertEquals( 20D,  params.get(1).get(0), DELTA);
        Assert.assertEquals(500D,  params.get(1).get(2), DELTA);
        Assert.assertEquals( 40D,  params.get(2).get(0), DELTA);
        Assert.assertEquals(1300D, params.get(2).get(2), DELTA);

        final double[][] standardized = LinearModelUtil.standardize(X, params);
        Assert.assertEquals(X.length, standardized.length);
        Assert.assertEquals(X[0].length, standardized[0].length);
    }

    private static double[][] getX() {
        final double[][] X = new double[4][3];
        X[0] = new double[]{ 0.01D,  0.50D, -0.12D};
        X[1] = new double[]{ 0.97D, -0.63D,  0.02D};
        X[2] = new double[]{ 0.41D,  1.15D, -1.17D};
        X[3] = new double[]{-1.38D, -1.02D,  1.27D};
        return X;
    }

    private static double[][] getY() {
        final double[][] Y = new double[4][1];
        Y[0] = new double[]{0.25};
        Y[1] = new double[]{0.08};
        Y[2] = new double[]{1.03};
        Y[3] = new double[]{-1.37};
        return Y;
    }

}
