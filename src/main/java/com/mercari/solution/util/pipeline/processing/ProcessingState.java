package com.mercari.solution.util.pipeline.processing;

import com.mercari.solution.util.domain.ml.LinearModelUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class ProcessingState implements Serializable {

    public Map<String, Integer> ints;
    public Map<String, Long> longs;
    public Map<String, Double> doubles;
    public Map<String, String> strings;

    public Map<String, List<Double>> doubleLists;

    public Map<String, List<List<Double>>> matrices;

    // TODO
    public Map<String, Integer> types;
    public Map<String, Object> values;
    public Map<String, ByteBuffer> serializedValues;

    public Map<String, LinearModelUtil.LinearModel> linearModels;



    public ProcessingState() {
        this.ints = new HashMap<>();
        this.longs = new HashMap<>();
        this.doubles = new HashMap<>();
        this.strings = new HashMap<>();
        this.doubleLists = new HashMap<>();
        this.matrices = new HashMap<>();


        this.types = new HashMap<>();
        this.values = new HashMap<>();
        this.linearModels = new HashMap<>();
    }

    public ProcessingState(GenericRecord record) {
        this.ints = new HashMap<>();
        this.longs = new HashMap<>();
        this.doubles = new HashMap<>();
        this.strings = new HashMap<>();
        this.doubleLists = new HashMap<>();
        this.matrices = new HashMap<>();


        this.types = new HashMap<>();
        this.values = (Map<String, Object>) record.get("values");
        this.serializedValues = (Map<String, ByteBuffer>) record.get("serializedValues");

        this.linearModels = new HashMap<>();
    }

    public Integer getInt(final String name) {
        return this.ints.get(name);
    }

    public Integer getInt(final String name, final Integer defaultValue) {
        return this.ints.getOrDefault(name, defaultValue);
    }

    public void putInt(final String name, final Integer value) {
        this.ints.put(name, value);
    }

    public Long getLong(final String name) {
        return this.longs.get(name);
    }

    public void putLong(final String name, final Long value) {
        this.longs.put(name, value);
    }

    public Double getDouble(final String name) {
        return this.doubles.get(name);
    }

    public void putDouble(final String name, final Double value) {
        this.doubles.put(name, value);
    }

    public List<List<Double>> getMatrix(final String name) {
        return this.matrices.get(name);
    }

    public void putMatrix(final String name, final List<List<Double>> matrix) {
        this.matrices.put(name, matrix);
    }


    @Override
    public String toString() {
        return linearModels.toString();
    }



    public static Schema schema() {
        final Schema typesSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        final Schema bytesSchema = Schema.createMap(Schema.create(Schema.Type.BYTES));

        final Schema multiValueSchema = Schema.createUnion(
                Schema.create(Schema.Type.DOUBLE),
                Schema.create(Schema.Type.LONG),
                Schema.create(Schema.Type.STRING),
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.INT),
                Schema.create(Schema.Type.FLOAT),
                Schema.create(Schema.Type.BOOLEAN),
                Schema.create(Schema.Type.BYTES),
                Schema.createArray(Schema.createArray(Schema.create(Schema.Type.DOUBLE)))
        );
        final Schema valuesSchema = Schema.createMap(multiValueSchema);

        return SchemaBuilder.record("ProcessingState").fields()
                .name("types").type(typesSchema).noDefault()
                .name("values").type(valuesSchema).noDefault()
                .name("serializedValues").type(bytesSchema).noDefault()
                .endRecord();
    }

    public static Coder<ProcessingState> coder() {
        //final org.apache.avro.Schema schema = schema();
        //return AvroCoder.of(ProcessingBuffer.class, schema, true);
        return ProcessingStateCoder.of();
    }

    public static class ProcessingStateCoder extends StructuredCoder<ProcessingState> {

        private final AvroCoder<ProcessingState> coder;

        public ProcessingStateCoder() {
            this.coder = AvroCoder.of(ProcessingState.class, schema(), false);
        }

        public static ProcessingStateCoder of() {
            return new ProcessingStateCoder();
        }

        @Override
        public void encode(ProcessingState value, OutputStream outStream) throws IOException {
            encode(value, outStream, Context.NESTED);
        }


        @Override
        public void encode(ProcessingState value, OutputStream outStream, Context context) throws IOException {
            coder.encode(value, outStream, context);
        }

        @Override
        public ProcessingState decode(InputStream inStream) throws IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, Context.NESTED);
            return new ProcessingState(record);
        }

        @Override
        public ProcessingState decode(InputStream inStream, Context context) throws  IOException {
            final GenericRecord record = (GenericRecord) coder.decode(inStream, context);
            return new ProcessingState(record);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public List<? extends Coder<?>> getComponents() {
            final List<Coder<?>> coders = new ArrayList<>();
            coders.add(coder);
            return coders;
        }

        @Override
        public void registerByteSizeObserver(ProcessingState value, ElementByteSizeObserver observer) throws Exception {
            coder.registerByteSizeObserver(value, observer);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            verifyDeterministic(this, "ProcessingStateCoder is deterministic if all coders are deterministic");
        }
    }
}
