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

    public static class ArrayAggDistinctStringFn extends Combine.CombineFn<String, Set<String>, List<String>> {

        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> addInput(Set<String> accumulator, String input) {
            if(input != null) {
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public Set<String> mergeAccumulators(Iterable<Set<String>> accumulators) {
            final Set<String> mergedAccumulator = new HashSet<>();
            for(final Set<String> accumulator : accumulators) {
                mergedAccumulator.addAll(accumulator);
            }
            return mergedAccumulator;
        }

        @Override
        public List<String> extractOutput(final Set<String> accumulator) {
            return new ArrayList<>(accumulator);
        }

    }

    public static class ArrayAggDistinctFloat64Fn extends Combine.CombineFn<Double, Set<Double>, List<Double>> {

        @Override
        public Set<Double> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Double> addInput(Set<Double> accumulator, Double input) {
            if(input != null) {
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public Set<Double> mergeAccumulators(Iterable<Set<Double>> accumulators) {
            final Set<Double> mergedAccumulator = new HashSet<>();
            for(final Set<Double> accumulator : accumulators) {
                mergedAccumulator.addAll(accumulator);
            }
            return mergedAccumulator;
        }

        @Override
        public List<Double> extractOutput(final Set<Double> accumulator) {
            return new ArrayList<>(accumulator);
        }

    }

}
