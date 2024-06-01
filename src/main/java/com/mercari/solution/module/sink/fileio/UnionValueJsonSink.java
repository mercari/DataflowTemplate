package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.util.pipeline.union.UnionValue;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;


public class UnionValueJsonSink implements FileIO.Sink<KV<String, UnionValue>> {

    private final Schema schema;
    private final boolean fitSchema;
    private final Boolean bom;

    private transient @Nullable PrintWriter writer;

    public static UnionValueJsonSink of(
            final Schema schema,
            final boolean fitSchema,
            final boolean bom) {

        return new UnionValueJsonSink(schema, fitSchema, bom);
    }

    UnionValueJsonSink(
            final Schema schema,
            final boolean fitSchema,
            final boolean bom) {

        this.schema = schema;
        this.fitSchema = fitSchema;
        this.bom = bom;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        final OutputStream os = Channels.newOutputStream(channel);
        if(this.bom) {
            os.write(0xef);
            os.write(0xbb);
            os.write(0xbf);
        }
        this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
    }

    @Override
    public void write(KV<String, UnionValue> element) throws IOException {
        final UnionValue input = element.getValue();
        final String json = UnionValue.getAsJson(input);
        writer.println(json);
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}
