package com.mercari.solution.util.sql.udf;

import org.apache.beam.sdk.transforms.Combine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggregateFunctions {

    public static class ArrayAggStringFn extends Combine.CombineFn<String, List<String>, List<String>> {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<String> addInput(List<String> accumulator, String input) {
            if(input != null) {
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
            final List<String> mergedAccumulator = new ArrayList<>();
            for(final List<String> accumulator : accumulators) {
                mergedAccumulator.addAll(accumulator);
            }
            return mergedAccumulator;
        }

        @Override
        public List<String> extractOutput(final List<String> accumulator) {
            return accumulator;
        }

    }

    public static class ArrayAggInt64Fn extends Combine.CombineFn<Long, List<Long>, List<Long>> {

        @Override
        public List<Long> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<Long> addInput(List<Long> accumulator, Long input) {
            if(input != null) {
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public List<Long> mergeAccumulators(Iterable<List<Long>> accumulators) {
            final List<Long> mergedAccumulator = new ArrayList<>();
            for(final List<Long> accumulator : accumulators) {
                mergedAccumulator.addAll(accumulator);
            }
            return mergedAccumulator;
        }

        @Override
        public List<Long> extractOutput(final List<Long> accumulator) {
            return accumulator;
        }

    }

    private static abstract class AggDistinctFn<InputT, OutputT> extends Combine.CombineFn<InputT, Set<InputT>, OutputT> {

        @Override
        public Set<InputT> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<InputT> addInput(Set<InputT> accumulator, InputT input) {
            if(input != null) {
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public Set<InputT> mergeAccumulators(Iterable<Set<InputT>> accumulators) {
            final Set<InputT> mergedAccumulator = new HashSet<>();
            for(final Set<InputT> accumulator : accumulators) {
                mergedAccumulator.addAll(accumulator);
            }
            return mergedAccumulator;
        }
    }

    public static class ArrayAggDistinctStringFn extends AggDistinctFn<String, List<String>> {
        @Override
        public List<String> extractOutput(final Set<String> accumulator) {
            return new ArrayList<>(accumulator);
        }
    }

    public static class CountDistinctStringFn extends AggDistinctFn<String, Long>  {

        @Override
        public Long extractOutput(final Set<String> accumulator) {
            if (accumulator == null) {
                return null;
            }
            return (long) accumulator.size();
        }

    }

    public static class ArrayAggDistinctFloat64Fn extends AggDistinctFn<Double, List<Double>> {
        @Override
        public List<Double> extractOutput(final Set<Double> accumulator) {
            return new ArrayList<>(accumulator);
        }

    }

    public static class CountDistinctFloat64Fn extends AggDistinctFn<Double, Long> {
        @Override
        public Long extractOutput(final Set<Double> accumulator) {
            if (accumulator == null) {
                return null;
            }
            return (long) accumulator.size();
        }

    }

    public static class ArrayAggDistinctInt64Fn extends AggDistinctFn<Long, List<Long>> {
        @Override
        public List<Long> extractOutput(final Set<Long> accumulator) {
            return new ArrayList<>(accumulator);
        }

    }

    public static class CountDistinctInt64Fn extends AggDistinctFn<Long, Long> {
        @Override
        public Long extractOutput(final Set<Long> accumulator) {
            if (accumulator == null) {
                return null;
            }
            return (long) accumulator.size();
        }

    }

}
