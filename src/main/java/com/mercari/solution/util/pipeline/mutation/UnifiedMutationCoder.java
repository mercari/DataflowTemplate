package com.mercari.solution.util.pipeline.mutation;

import com.google.cloud.spanner.Mutation;
import com.mercari.solution.module.DataType;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class UnifiedMutationCoder extends StructuredCoder<UnifiedMutation> {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedMutationCoder.class);

    private final StringUtf8Coder stringUtf8Coder = StringUtf8Coder.of();
    private final Coder<Mutation> spannerMutationCoder = SerializableCoder.of(Mutation.class);

    public UnifiedMutationCoder() {

    }

    public static UnifiedMutationCoder of() {
        return new UnifiedMutationCoder();
    }


    @Override
    public void encode(UnifiedMutation mutation, OutputStream outStream) throws IOException {
        encode(mutation, outStream, Context.NESTED);
    }


    @Override
    public void encode(UnifiedMutation value, OutputStream outStream, Context context) throws IOException {
        VarInt.encode(value.getType().getId(), outStream);
        VarInt.encode(value.getOp().getId(), outStream);
        VarInt.encode(value.getCommitTimestampMicros(), outStream);
        VarInt.encode(value.getSequence(), outStream);
        switch (value.getType()) {
            case MUTATION -> {
                if(value.getValue() == null) {
                    throw new IllegalArgumentException("Mutation must not be null");
                }
                spannerMutationCoder.encode((Mutation) value.getValue(), outStream);
            }
            default -> throw new IllegalArgumentException();
        }
        this.stringUtf8Coder.encode(value.getTable(), outStream);
    }

    @Override
    public UnifiedMutation decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public UnifiedMutation decode(InputStream inStream, Context context) throws  IOException {
        final int typeId = VarInt.decodeInt(inStream);
        final int opId = VarInt.decodeInt(inStream);
        final long commitTimestampMicros = VarInt.decodeLong(inStream);
        final int sequence = VarInt.decodeInt(inStream);

        final MutationOp op = MutationOp.of(opId);
        final DataType type = DataType.of(typeId);

        final Object mutation = switch (type) {
            case MUTATION -> spannerMutationCoder.decode(inStream);
            default -> throw new IllegalArgumentException();
        };
        final String table = this.stringUtf8Coder.decode(inStream);

        if(!table.equals(table.trim())) {
            LOG.error("table not match: [{}] <> [{}]", table, table.trim());
        }

        return new UnifiedMutation(type, op, table.trim(), commitTimestampMicros, sequence, mutation);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
        return Collections.emptyList();
    }

    @Override
    public void registerByteSizeObserver(UnifiedMutation value, ElementByteSizeObserver observer) throws Exception {
        observer.update(VarInt.getLength(value.getType().getId()));
        observer.update(VarInt.getLength(value.getOp().getId()));
        observer.update(VarInt.getLength(value.getCommitTimestampMicros()));
        observer.update(VarInt.getLength(value.getSequence()));
        switch (value.getType()) {
            case MUTATION -> {
                this.spannerMutationCoder.registerByteSizeObserver((Mutation) value.getValue(), observer);
            }
        }
        observer.update(this.stringUtf8Coder.getEncodedElementByteSize(value.getTable()));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(this, "MutationCoder is deterministic if all coders are deterministic");
    }
}
