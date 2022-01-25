package com.mercari.solution.util.sql.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;

import java.util.List;

public class ArrayFunctions {

    public static class ContainsAllStringsFn implements BeamSqlUdf {

        public static Boolean eval(final List<String> values1, final List<String> values2) {

            if (values1 == null || values2 == null) {
                return false;
            }
            return values1.containsAll(values2);
        }

    }

    public static class ContainsAllInt64sFn implements BeamSqlUdf {

        public static Boolean eval(final List<Long> values1, final List<Long> values2) {

            if (values1 == null || values2 == null) {
                return false;
            }
            return values1.containsAll(values2);
        }

    }

}
