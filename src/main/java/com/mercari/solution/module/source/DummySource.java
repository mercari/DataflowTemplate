package com.mercari.solution.module.source;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.common.hash.Hashing;
import com.mercari.solution.config.SourceConfig;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.LocalDateTime;

import java.time.temporal.ChronoUnit;
import java.util.*;

public class DummySource {

    public static DummyBatchRead batch(final SourceConfig config) {
        return new DummyBatchRead(config);
    }

    public static DummyMicrobatchRead microbatch(final SourceConfig config) {
        return new DummyMicrobatchRead();
    }

    private static class DummyBatchRead extends PTransform<PBegin, PCollection<Row>> {

        private DummyBatchRead(final SourceConfig config) {

        }

        public PCollection<Row> expand(final PBegin begin) {
            return null;
        }

    }

    private static class DummyMicrobatchRead extends PTransform<PCollection<Long>, PCollection<Row>> {

        public PCollection<Row> expand(final PCollection<Long> beat) {
            return null;
        }

    }

    private interface DummyGenerator {

        Random random = new Random();

        static DummyGenerator of(String fieldType, String fieldName, boolean isPrimary, boolean isNullable, List<String> range, int randomRate) {
            if(fieldType.startsWith("STRING") || fieldType.startsWith("BYTES")) {
                final String sizeString = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));
                final Integer lengthLimit = "MAX".equals(sizeString) ? Integer.MAX_VALUE : Integer.valueOf(sizeString);
                if(fieldType.startsWith("BYTES")) {
                    return new BytesDummyGenerator(fieldName, lengthLimit, isPrimary, isNullable, randomRate);
                }
                return new StringDummyGenerator(fieldName, lengthLimit, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("INT64")) {
                return new IntDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("FLOAT64")) {
                return new FloatDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("BOOL")) {
                return new BoolDummyGenerator(fieldName, isNullable, randomRate);
            } else if(fieldType.equals("DATE")) {
                return new DateDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("TIMESTAMP")) {
                return new TimestampDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.startsWith("ARRAY")) {
                final String arrayType = fieldType.substring(fieldType.indexOf("<") + 1, fieldType.indexOf(">"));
                final DummyGenerator elementGenerator = of(arrayType, fieldName, false, isNullable, range, randomRate);
                return new ArrayDummyGenerator(fieldName, elementGenerator, isNullable, randomRate);
            }
            throw new IllegalArgumentException(String.format("Illegal fieldType: %s, %s", fieldType, fieldName));
        }

        static boolean randomNull(boolean isNullable, int randomRate) {
            return isNullable && random.nextInt(100) < randomRate;
        }

        static <T> List<T> generateValues(DummyGenerator generator, long value) {
            final List<T> randomValues = new ArrayList<>();
            for(int i=0; i<10; i++) {
                final T randomValue = (T)generator.generateValue(value);
                randomValues.add(randomValue);
            }
            return randomValues;
        }

        Object generateValue(long value);

        Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value);

        Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value);

    }

    private static class StringDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final List<String> range;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public StringDummyGenerator(String fieldName, Integer maxLength, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            if(range != null && range.size() == 1 && NumberUtils.isNumber(range.get(0))) {
                List<String> r = new ArrayList<>();
                for(int i=0; i<Integer.valueOf(range.get(0)); i++) {
                    String randomString = UUID.randomUUID().toString() + UUID.randomUUID().toString();
                    randomString = maxLength > randomString.length() ? randomString : randomString.substring(0, maxLength);
                    r.add(randomString);
                }
                this.range = r;
            } else {
                this.range = range;
            }
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public String generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final String randomString = range != null && range.size() > 0 ?
                    range.get(random.nextInt(range.size())) :
                    UUID.randomUUID().toString() + UUID.randomUUID().toString();
            return maxLength > randomString.length() ? randomString : randomString.substring(0, maxLength);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            final String randomString = generateValue(value);
            return builder.set(this.fieldName).to(randomString);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<String> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toStringArray(randomValues);
        }

    }

    private static class BytesDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public BytesDummyGenerator(String fieldName, Integer maxLength, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public ByteArray generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final byte[] randomBytes = this.isPrimary ? Long.toString(value).getBytes() : Hashing.sha512().hashLong(value).asBytes();
            return ByteArray.copyFrom(randomBytes.length < maxLength ? randomBytes : Arrays.copyOf(randomBytes, maxLength));
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<ByteArray> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBytesArray(randomValues);
        }

    }

    private static class IntDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long min;
        private final Long max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public IntDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Long.valueOf(range.get(0)) : 0L;
            this.max = range != null && range.size() > 1 ? Long.valueOf(range.get(1)) : Long.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Long generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? value : Math.abs(random.nextLong()) % (this.max - this.min) + this.min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Long> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toInt64Array(randomValues);
        }
    }

    private static class FloatDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Double min;
        private final Double max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public FloatDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Double.valueOf(range.get(0)) : Double.MIN_VALUE;
            this.max = range != null && range.size() > 1 ? Double.valueOf(range.get(1)) : Double.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Double generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? (double)value : random.nextDouble() * (max - min) + min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Double> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toFloat64Array(randomValues);
        }

    }

    private static class BoolDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Boolean isNullable;
        private final int randomRate;

        public BoolDummyGenerator(String fieldName, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Boolean generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return this.random.nextBoolean();
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Boolean> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBoolArray(randomValues);
        }

    }

    private static class DateDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Integer bound;
        private final LocalDateTime startDate;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public DateDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDate = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime ldt2 = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = 0;// (int)(ChronoUnit.DAYS.between(this.startDate.toLocalDate(), ldt2.toLocalDate()));
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Date generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            //final LocalDateTime nextDate = this.startDate.plusDays(isPrimary ? value : (long)this.random.nextInt(this.bound));
            //return Date.fromYearMonthDay(nextDate.getYear(), nextDate.getMonthValue(), nextDate.getDayOfMonth());
            return null;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Date> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toDateArray(randomValues);
        }

    }

    private static class TimestampDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long bound;
        private final LocalDateTime startDateTime;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public TimestampDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDateTime = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = 0L;//java.time.Duration.between(this.startDateTime, endDateTime).getSeconds();
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Timestamp generateValue(long value) {
            final LocalDateTime nextTime;
            if(this.isPrimary) {
                //nextTime = this.startDateTime.plusSeconds(value);
                //return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            } else if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            } else {
                //nextTime = this.startDateTime.plusSeconds(Math.abs(this.random.nextLong()) % this.bound);
                //return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            }
            return null;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Timestamp> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toTimestampArray(randomValues);
        }

    }


    private static class ArrayDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final DummyGenerator elementGenerator;
        private final Boolean isNullable;
        private final int randomRate;

        public ArrayDummyGenerator(String fieldName, DummyGenerator elementGenerator, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.elementGenerator = elementGenerator;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public List generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return DummyGenerator.generateValues(elementGenerator, value);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return elementGenerator.putDummyValues(builder, value);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            throw new IllegalArgumentException(String.format("Field: %s, Impossible Nested Array (Array in Array) for Cloud Spanner.", fieldName));
        }

    }

}
