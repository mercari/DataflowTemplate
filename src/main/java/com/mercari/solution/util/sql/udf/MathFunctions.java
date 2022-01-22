package com.mercari.solution.util.sql.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;


public class MathFunctions {

    public static class MaxFloat64Fn implements BeamSqlUdf {

        public static Double eval(final Double value1, final Double value2) {

            if (value1 == null) {
                return value2;
            }
            if (value2 == null) {
                return value1;
            }

            if(value1 >= value2) {
                return value1;
            } else {
                return value2;
            }
        }

    }

    public static class MaxInt64Fn implements BeamSqlUdf {

        public static Long eval(final Long value1, final Long value2) {

            if (value1 == null) {
                return value2;
            }
            if (value2 == null) {
                return value1;
            }

            if(value1 >= value2) {
                return value1;
            } else {
                return value2;
            }
        }

    }

    public static class MinFloat64Fn implements BeamSqlUdf {

        public static Double eval(final Double value1, final Double value2) {

            if (value1 == null) {
                return value2;
            }
            if (value2 == null) {
                return value1;
            }

            if(value1 <= value2) {
                return value1;
            } else {
                return value2;
            }
        }

    }

    public static class MinInt64Fn implements BeamSqlUdf {

        public static Long eval(final Long value1, final Long value2) {

            if (value1 == null) {
                return value2;
            }
            if (value2 == null) {
                return value1;
            }

            if(value1 <= value2) {
                return value1;
            } else {
                return value2;
            }
        }

    }
    
}
