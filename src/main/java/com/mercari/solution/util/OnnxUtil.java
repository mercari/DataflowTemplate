package com.mercari.solution.util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OnnxUtil {

    public static List<Object> flatten(final List<Object> list, final int rank) {
        Stream<Object> stream = list.stream();
        for(int i=1; i<rank; i++) {
            stream = stream.flatMap(v -> ((List<Object>)v).stream());
        }
        return stream.collect(Collectors.toList());
    }

    public static List<Object> reshape(final List<Object> list, final long[] shape) {
        for(long dim : shape) {

        }

        return null;
    }
}
