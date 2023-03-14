package com.mercari.solution.util.pipeline.union;

import com.mercari.solution.module.DataType;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;


public class UnionCoder extends StructuredCoder<UnionValue> {

    private final List<Coder<?>> coders;

    public UnionCoder(List<Coder<?>> coders) {
        if(coders == null) {
            throw new IllegalStateException("UnionValue coders must not null");
        }
        this.coders = coders;
    }

    public static UnionCoder of(List<Coder<?>> coders) {
        return new UnionCoder(coders);
    }

    private int getIndex(UnionValue value) {
        if(value == null) {
            throw new IllegalStateException();
        }
        int index = value.getIndex();
        if(index < 0) {
            throw new IllegalStateException("UnionValue index must over zero. actual index is: " + index);
        }
        if(index >= coders.size()) {
            throw new IllegalStateException("UnionValue index must over coders size. actual index is: " + index + ", coders size is: " + coders.size());
        }
        return index;
    }

    public List<? extends Coder<?>> getCoders() {
        return coders;
    }

    @Override
    public void encode(UnionValue value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }


    @Override
    public void encode(UnionValue value, OutputStream outStream, Context context) throws IOException {
        final int index = getIndex(value);
        VarInt.encode(index, outStream);
        VarInt.encode(value.getType().getId(), outStream);
        VarInt.encode(value.getEpochMillis(), outStream);

        final Coder<Object> coder = (Coder<Object>) coders.get(index);
        coder.encode(value.getValue(), outStream, context);
    }

    @Override
    public UnionValue decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public UnionValue decode(InputStream inStream, Context context) throws  IOException {
        final int index = VarInt.decodeInt(inStream);
        final int dataTypeId = VarInt.decodeInt(inStream);
        final DataType dataType = DataType.of(dataTypeId);
        final long epochMillis = VarInt.decodeLong(inStream);
        final Object value = coders.get(index).decode(inStream, context);
        return new UnionValue(index, dataType, epochMillis, value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
        return coders;
    }

    @Override
    public void registerByteSizeObserver(UnionValue value, ElementByteSizeObserver observer) throws Exception {
        final int index = getIndex(value);
        observer.update(VarInt.getLength(index));
        final Coder<Object> coder = (Coder<Object>) coders.get(index);
        coder.registerByteSizeObserver(value.getValue(), observer);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(this, "UnionCoder is deterministic if all coders are deterministic");
    }
}
