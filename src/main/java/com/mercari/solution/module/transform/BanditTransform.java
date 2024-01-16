package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.gson.Gson;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.TransformModule;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class BanditTransform implements TransformModule {

    private static final Logger LOG = LoggerFactory.getLogger(BanditTransform.class);

    private class BanditTransformParameters implements Serializable {

        private Algorithm algorithm;
        private String armField;
        private String countField;
        private String rewardField;
        private List<String> targetFields;
        private List<String> initialArms;
        private Long intervalSeconds;

        // Algorithm parameters
        //// for egreedy
        private Double epsilon;
        //// for softmax
        private Double tau;
        //// for annealing (egreedy, softmax)
        private Double coolingRate;

        public Algorithm getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        public String getArmField() {
            return armField;
        }

        public void setArmField(String armField) {
            this.armField = armField;
        }

        public String getCountField() {
            return countField;
        }

        public void setCountField(String countField) {
            this.countField = countField;
        }

        public String getRewardField() {
            return rewardField;
        }

        public void setRewardField(String rewardField) {
            this.rewardField = rewardField;
        }

        public List<String> getTargetFields() {
            return targetFields;
        }

        public void setTargetFields(List<String> targetFields) {
            this.targetFields = targetFields;
        }

        public List<String> getInitialArms() {
            return initialArms;
        }

        public void setInitialArms(List<String> initialArms) {
            this.initialArms = initialArms;
        }

        public Long getIntervalSeconds() {
            return intervalSeconds;
        }

        public void setIntervalSeconds(Long intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }

        public Double getEpsilon() {
            return epsilon;
        }

        public void setEpsilon(Double epsilon) {
            this.epsilon = epsilon;
        }

        public Double getTau() {
            return tau;
        }

        public void setTau(Double tau) {
            this.tau = tau;
        }

        public Double getCoolingRate() {
            return coolingRate;
        }

        public void setCoolingRate(Double coolingRate) {
            this.coolingRate = coolingRate;
        }

    }

    private enum Algorithm implements Serializable {
        egreedy,
        softmax,
        ucb,
        ts
    }

    public String getName() {
        return "bandit";
    }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return transform(inputs, config);
    }

    private static final Schema SCHEMA_OUTPUT = SchemaBuilder.record("arms").fields()
            .name("target").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
            .name("selectedArm").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
            .name("timestamp").type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE).noDefault()
            .name("algorithm").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
            .name("states").type(Schema.createArray(SchemaBuilder.record("state").fields()
                    .name("arm").type(AvroSchemaUtil.REQUIRED_STRING).noDefault()
                    .name("count").type(AvroSchemaUtil.REQUIRED_LONG).noDefault()
                    .name("value").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
                    .name("probability").type(AvroSchemaUtil.REQUIRED_DOUBLE).noDefault()
                    .endRecord())).noDefault()
            .endRecord();

    public static Map<String, FCollection<?>> transform(final List<FCollection<?>> inputs, final TransformConfig config) {

        final BanditTransformParameters parameters = new Gson().fromJson(config.getParameters(), BanditTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Schema outputSchema = SCHEMA_OUTPUT;
        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    final Transform<GenericRecord> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            AvroSchemaUtil::getAsString,
                            AvroSchemaUtil::getAsLong,
                            AvroSchemaUtil::getAsDouble);
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    final Transform<Row> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            RowSchemaUtil::getAsString,
                            RowSchemaUtil::getAsLong,
                            RowSchemaUtil::getAsDouble);
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    final Transform<Struct> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            StructSchemaUtil::getAsString,
                            StructSchemaUtil::getAsLong,
                            StructSchemaUtil::getAsDouble);
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    final Transform<Entity> transform = new Transform<>(
                            parameters,
                            outputSchema.toString(),
                            EntitySchemaUtil::getAsString,
                            EntitySchemaUtil::getAsLong,
                            EntitySchemaUtil::getAsDouble);
                    final PCollection<GenericRecord> output = inputCollection.getCollection()
                            .apply(name, transform)
                            .setCoder(AvroCoder.of(outputSchema));
                    results.put(name, FCollection.of(name, output, DataType.AVRO, outputSchema));
                    break;
                }
            }

        }

        return results;
    }

    private static void validateParameters(final BanditTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("BanditTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getArmField() == null) {
            errorMessages.add("BanditTransform config parameters must contain armField parameter.");
        }
        if(parameters.getCountField() == null) {
            errorMessages.add("BanditTransform config parameters must contain countField parameter.");
        }
        if(parameters.getRewardField() == null) {
            errorMessages.add("BanditTransform config parameters must contain rewardField parameter.");
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(BanditTransformParameters parameters) {
        if(parameters.getAlgorithm() == null) {
            parameters.setAlgorithm(Algorithm.ts);
        }
        if(parameters.getTargetFields() == null) {
            parameters.setTargetFields(new ArrayList<>());
        }
        if(parameters.getInitialArms() == null) {
            parameters.setInitialArms(new ArrayList<>());
        }
        if(parameters.getIntervalSeconds() == null) {
            parameters.setIntervalSeconds(10L);
        }
        if(parameters.getEpsilon() == null) {
            parameters.setEpsilon(0.1);
        }
        if(parameters.getTau() == null) {
            parameters.setTau(0.2);
        }
        if(parameters.getCoolingRate() == null) {
            parameters.setCoolingRate(1D);
        }
    }


    public static class Transform<T> extends PTransform<PCollection<T>, PCollection<GenericRecord>> {

        private final BanditTransformParameters parameters;
        private final String outputSchemaString;

        private final StringGetter<T> stringGetter;
        private final LongGetter<T> longGetter;
        private final DoubleGetter<T> doubleGetter;

        private Transform(final BanditTransformParameters parameters, final String outputSchemaString,
                          final StringGetter<T> stringGetter, final LongGetter<T> longGetter, final DoubleGetter<T> doubleGetter) {
            this.parameters = parameters;
            this.outputSchemaString = outputSchemaString;
            this.stringGetter = stringGetter;
            this.longGetter = longGetter;
            this.doubleGetter = doubleGetter;
        }

        @Override
        public PCollection<GenericRecord> expand(final PCollection<T> input) {

            final PCollection<T> withWindow = input.apply("WithFixedWindow", Window
                    .into(FixedWindows.of(Duration.standardSeconds(parameters.getIntervalSeconds()))));

            final PCollection<KV<String, T>> withKey;
            if (parameters.getTargetFields().size() > 0) {
                final List<String> targetFields = parameters.getTargetFields();
                withKey = withWindow.apply("WithTargetFieldsKey", WithKeys.of((T t) -> {
                    final StringBuilder sb = new StringBuilder();
                    for(String field : targetFields) {
                        final String target = stringGetter.getAsString(t, field);
                        sb.append(target == null ? "" : target);
                        sb.append("#");
                    }
                    if(sb.length() > 0) {
                        sb.deleteCharAt(sb.length() - 1);
                    }
                    return sb.toString();
                }));
            } else {
                withKey = withWindow.apply("WithFixedKey", WithKeys.of(""));
            }

            final PCollection<KV<String, List<Reward>>> rewards = withKey
                    .apply("Combine", Combine.perKey(new RewardCombineFn<>(
                            parameters.getArmField(), parameters.getCountField(), parameters.getRewardField(),
                            stringGetter, longGetter, doubleGetter)))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(AvroCoder.of(Reward.class))))
                    .apply("WithGlobalWindow", Window.into(new GlobalWindows()));

            final PCollection<GenericRecord> output;
            if(input.getPipeline().getOptions().as(StreamingOptions.class).isStreaming()) {
                output = rewards.apply("BanditStreamingTransform", ParDo
                        .of(new BanditStreamingDoFn(parameters.getAlgorithm(), parameters.getEpsilon(), parameters.getTau(), parameters.getCoolingRate(), outputSchemaString, parameters.getInitialArms())));
            } else {
                output = rewards.apply("BanditBatchTransform", ParDo
                        .of(new BanditBatchDoFn(parameters.getAlgorithm(), parameters.getEpsilon(), parameters.getTau(), parameters.getCoolingRate(), outputSchemaString, parameters.getInitialArms())));
            }

            return output;
        }

        public static class RewardCombineFn<T> extends Combine.CombineFn<T, RewardCombineFn.Accum, List<Reward>> {

            private final String armField;
            private final String countField;
            private final String rewardField;
            private final StringGetter<T> stringGetter;
            private final LongGetter<T> longGetter;
            private final DoubleGetter<T> doubleGetter;

            RewardCombineFn(final String armField, final String countField, final String rewardField,
                            final StringGetter<T> stringGetter, final LongGetter<T> longGetter, final DoubleGetter<T> doubleGetter) {
                this.armField = armField;
                this.countField = countField;
                this.rewardField = rewardField;
                this.stringGetter = stringGetter;
                this.longGetter = longGetter;
                this.doubleGetter = doubleGetter;
            }

            @DefaultCoder(AvroCoder.class)
            public static class Accum {

                private final Map<String, Long> counts;
                private final Map<String, Double> rewards;

                Accum() {
                    counts = new HashMap<>();
                    rewards = new HashMap<>();
                }
            }

            @Override
            public Accum createAccumulator() { return new Accum(); }

            @Override
            public Accum addInput(Accum accum, T input) {
                final String arm = stringGetter.getAsString(input, armField);
                final Long count = longGetter.getAsLong(input, countField);
                final Double reward = doubleGetter.getAsDouble(input, rewardField);
                accum.counts.merge(arm, count == null ? 0L : count, Long::sum);
                accum.rewards.merge(arm, reward == null ? 0D : reward, Double::sum);
                return accum;
            }

            @Override
            public Accum mergeAccumulators(final Iterable<Accum> accums) {
                final Accum merged = createAccumulator();
                for (final Accum accum : accums) {
                    for (final Map.Entry<String, Long> entry : accum.counts.entrySet()) {
                        merged.counts.merge(entry.getKey(), entry.getValue(), Long::sum);
                    }
                    for (final Map.Entry<String, Double> entry : accum.rewards.entrySet()) {
                        merged.rewards.merge(entry.getKey(), entry.getValue(), Double::sum);
                    }
                }
                return merged;
            }

            @Override
            public List<Reward> extractOutput(final Accum accum) {
                final List<Reward> rewards = new ArrayList<>();
                for(var count : accum.counts.entrySet()) {
                    final Reward reward = new Reward();
                    reward.setArm(count.getKey());
                    reward.setCount(count.getValue());
                    reward.setReward(accum.rewards.getOrDefault(count.getKey(), 0D));
                    rewards.add(reward);
                }
                return rewards;
            }

        }


        @DefaultCoder(AvroCoder.class)
        private static class Reward implements Serializable {

            private String arm;
            private Long count;
            private Double reward;

            public String getArm() {
                return arm;
            }

            public void setArm(String arm) {
                this.arm = arm;
            }

            public Long getCount() {
                return count;
            }

            public void setCount(Long count) {
                this.count = count;
            }

            public Double getReward() {
                return reward;
            }

            public void setReward(Double reward) {
                this.reward = reward;
            }

        }

        private static abstract class BanditDoFn extends DoFn<KV<String, List<Reward>>, GenericRecord> {

            static final String STATEID_COUNTS = "counts";
            static final String STATEID_VALUES = "values";
            static final String STATEID_TEMPERATURE = "temperature";

            private final Algorithm algorithm;
            private final Double epsilon;
            private final Double tau;
            private final Double coolingRate;

            private final List<String> initialArms;

            private final String schemaString;
            private transient Schema schema;

            BanditDoFn(final Algorithm algorithm, final Double epsilon, final Double tau, final Double coolingRate, final String schemaString, final List<String> initialArms) {
                this.algorithm = algorithm;
                this.epsilon = epsilon;
                this.tau = tau;
                this.coolingRate = coolingRate;
                this.schemaString = schemaString;
                this.initialArms = initialArms;
            }

            @Setup
            public void setup() {
                this.schema = AvroSchemaUtil.convertSchema(schemaString);
            }

            private String selectArm(final Map<String, Double> probabilities) {
                final double value = Math.random();
                double cumsum = 0D;
                for(var probability : probabilities.entrySet()) {
                    cumsum += probability.getValue();
                    if(cumsum >= value) {
                        return probability.getKey();
                    }
                }
                return probabilities.entrySet().stream().findAny().map(Map.Entry::getKey).orElse("");
            }

            void process(final ProcessContext c,
                         final ValueState<Map<String, Long>> countsState,
                         final ValueState<Map<String, Double>> valuesState,
                         final ValueState<Double> temperatureState) {

                final String target = c.element().getKey();
                final List<Reward> rewards = c.element().getValue();

                final Map<String, Long> counts = Optional.ofNullable(countsState.read()).orElseGet(() -> initialArms.stream().collect(Collectors.toMap(arm -> arm, arm -> 0L)));
                final Map<String, Double> values = Optional.ofNullable(valuesState.read()).orElseGet(() -> initialArms.stream().collect(Collectors.toMap(arm -> arm, arm -> 0D)));
                final Double temperature = Optional.ofNullable(temperatureState.read()).orElse(1D);

                // Update values (avg of rewards)
                for(final Reward reward : rewards) {
                    final long n = counts.getOrDefault(reward.getArm(), 0L);
                    final long m = reward.getCount();
                    final double n1 = (n + m == 0) ? 0 : n/(n + (double)m);
                    final double m1 = (n + m == 0) ? 0 : 1D/(n + m);
                    values.merge(reward.getArm(), reward.getReward() == null ? 0 : m1 * reward.getReward(), (oldVal, newVal) -> n1 * oldVal + newVal);
                }
                valuesState.write(values);

                // Update counts
                for(final Reward reward : rewards) {
                    counts.merge(reward.getArm(), reward.getCount() == null ? 0 : reward.getCount(), Long::sum);
                }
                countsState.write(counts);

                // Update temperature
                temperatureState.write(temperature * coolingRate);

                // Update probability
                final Map<String, Double> probabilities;
                switch (algorithm) {
                    case egreedy: {
                        probabilities = new HashMap<>();
                        final String maxarm = argmax(values);
                        for(final String arm : counts.keySet()) {
                            if(arm.equals(maxarm)) {
                                if(counts.size() == 1) {
                                    probabilities.put(arm, 1D);
                                } else {
                                    probabilities.put(arm, 1 - (epsilon * temperature));
                                }
                            } else {
                                probabilities.put(arm, (epsilon * temperature) / (counts.size() - 1));
                            }
                        }
                        break;
                    }
                    case softmax: {
                        probabilities = softmax(values, Math.max(0.0001, tau * temperature));
                        break;
                    }
                    case ucb: {
                        probabilities = ucb(counts, values);
                        break;
                    }
                    case ts: {
                        probabilities = new HashMap<>();
                        for(var arm : counts.entrySet()) {
                            final Double value = thompsonSampling(arm.getValue(), arm.getValue() * values.getOrDefault(arm.getKey(), 0D));
                            probabilities.put(arm.getKey(), value);
                        }
                        final double sum = probabilities.values().stream().mapToDouble(v -> v).sum();
                        for(var arm : counts.keySet()) {
                            probabilities.merge(arm, sum, (oldVal, newVal) -> oldVal / newVal);
                        }
                        break;
                    }
                    default:
                        throw new IllegalStateException("Not supported algorithm: " + algorithm.name());
                }

                final List<GenericRecord> states = new ArrayList<>();
                for(var arm : counts.entrySet()) {
                    final GenericRecord state = new GenericRecordBuilder(schema.getField("states").schema().getElementType())
                            .set("arm", arm.getKey())
                            .set("count", arm.getValue())
                            .set("value", values.getOrDefault(arm.getKey(), 0D))
                            .set("probability", probabilities.getOrDefault(arm.getKey(), 0D))
                            .build();
                    states.add(state);
                }

                final GenericRecord record = new GenericRecordBuilder(schema)
                        .set("target", target)
                        .set("selectedArm", algorithm.equals(Algorithm.ucb) ? argmax(probabilities) : selectArm(probabilities))
                        .set("timestamp", c.timestamp().getMillis() * 1000)
                        .set("algorithm", algorithm.name())
                        .set("states", states)
                        .build();

                c.output(record);
            }

            private String argmax(final Map<String, Double> values) {
                String maxKey = null;
                double maxValue = Double.MIN_VALUE;
                for(Map.Entry<String, Double> entry : values.entrySet()) {
                    if(entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }
                    if(entry.getValue() > maxValue) {
                        maxKey = entry.getKey();
                        maxValue = entry.getValue();
                    }
                }
                return maxKey;
            }

            private Map<String, Double> softmax(final Map<String, Double> values, final Double tau) {
                final Map<String,Double> logits = values.entrySet().stream().collect(Collectors
                        .toMap(Map.Entry::getKey, e -> e.getValue()/(tau == 0 ? 1.0 : tau)));
                final double max = logits.values().stream().mapToDouble(v -> v).max().orElse(0);
                final double sum = logits.values().stream().mapToDouble(v -> Math.exp(v - max)).sum();
                return logits.entrySet().stream().collect(Collectors
                        .toMap(Map.Entry::getKey, e -> Math.exp(e.getValue() - max) / sum));
            }

            private Double thompsonSampling(final Long count, final Double value) {
                long win = Math.min(value == null ? 0 : value.longValue(), count == null ? 0 : count);
                return new BetaDistribution(win + 1, count - win + 1).getNumericalMean();
            }

            private Map<String, Double> ucb(final Map<String, Long> counts, final Map<String, Double> values) {
                if(counts.containsValue(0L)) {
                    final long zeroCount = counts.values().stream().filter(v -> v == 0L).count();
                    final double probability = 1.0 / zeroCount;
                    return counts.entrySet().stream().collect(Collectors
                            .toMap(Map.Entry::getKey, e -> e.getValue() == 0 ? probability : 0D));
                }
                final double sum = counts.values().stream().mapToDouble(v -> v).sum();
                final double logsum = Math.log(sum);
                final Map<String, Double> ucb = values.entrySet().stream().collect(Collectors
                        .toMap(Map.Entry::getKey, v -> v.getValue() + Math.sqrt(logsum/(2 * counts.getOrDefault(v.getKey(), 1L)))));
                return softmax(ucb, 0D);
            }

        }

        private static class BanditBatchDoFn extends BanditDoFn {

            @StateId(STATEID_COUNTS)
            private final StateSpec<ValueState<Map<String, Long>>> counts;
            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<Map<String, Double>>> values;
            @StateId(STATEID_TEMPERATURE)
            private final StateSpec<ValueState<Double>> temperature;

            BanditBatchDoFn(final Algorithm algorithm, final Double epsilon, final Double tau, final Double coolingRate, final String schemaString, final List<String> initialArms) {
                super(algorithm, epsilon, tau, coolingRate, schemaString, initialArms);
                this.counts = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
                this.values = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
                this.temperature = StateSpecs.value(DoubleCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            @RequiresTimeSortedInput
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_COUNTS) ValueState<Map<String, Long>> countsState,
                                       final @StateId(STATEID_VALUES) ValueState<Map<String, Double>> valuesState,
                                       final @StateId(STATEID_TEMPERATURE) ValueState<Double> temperatureState) {

                process(c, countsState, valuesState, temperatureState);
            }
        }

        private static class BanditStreamingDoFn extends BanditDoFn {

            @StateId(STATEID_COUNTS)
            private final StateSpec<ValueState<Map<String, Long>>> counts;
            @StateId(STATEID_VALUES)
            private final StateSpec<ValueState<Map<String, Double>>> values;
            @StateId(STATEID_TEMPERATURE)
            private final StateSpec<ValueState<Double>> temperature;

            BanditStreamingDoFn(final Algorithm algorithm, final Double epsilon, final Double tau, final Double coolingRate, final String schemaString, final List<String> initialArms) {
                super(algorithm, epsilon, tau, coolingRate, schemaString, initialArms);
                this.counts = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));
                this.values = StateSpecs.value(MapCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
                this.temperature = StateSpecs.value(DoubleCoder.of());
            }

            @Setup
            public void setup() {
                super.setup();
            }

            @ProcessElement
            public void processElement(final ProcessContext c,
                                       final @StateId(STATEID_COUNTS) ValueState<Map<String, Long>> countsState,
                                       final @StateId(STATEID_VALUES) ValueState<Map<String, Double>> valuesState,
                                       final @StateId(STATEID_TEMPERATURE) ValueState<Double> temperatureState) {

                process(c, countsState, valuesState, temperatureState);
            }
        }

        private interface StringGetter<T> extends Serializable {
            String getAsString(final T value, final String field);
        }

        private interface LongGetter<T> extends Serializable {
            Long getAsLong(final T value, final String field);
        }

        private interface DoubleGetter<T> extends Serializable {
            Double getAsDouble(final T value, final String field);
        }

    }

}
