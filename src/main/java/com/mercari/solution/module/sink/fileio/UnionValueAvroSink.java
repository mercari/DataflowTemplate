package com.mercari.solution.module.sink.fileio;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import com.mercari.solution.module.DataType;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;


public class UnionValueAvroSink implements FileIO.Sink<KV<String, UnionValue>> {

    private final String jsonSchema;
    private final CodecFactory codecFactory;
    private final Map<String, String> metadata;
    private final boolean fitSchema;

    private transient @Nullable Schema schema;
    private transient @Nullable DataFileWriter<GenericRecord> writer;

    public static UnionValueAvroSink of(
            final Schema schema,
            final CodecFactory codecFactory,
            final boolean fitSchema) {

        return new UnionValueAvroSink(schema.toString(), codecFactory, fitSchema);
    }

    UnionValueAvroSink(
            final String jsonSchema,
            final CodecFactory codecFactory,
            final boolean fitSchema) {

        this.jsonSchema = jsonSchema;
        this.codecFactory = codecFactory;
        this.metadata = new HashMap<>();
        this.fitSchema = fitSchema;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(jsonSchema);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        this.writer = new DataFileWriter<>(datumWriter)
                .setCodec(codecFactory);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            Object v = entry.getValue();
            if (v instanceof String) {
                writer.setMeta(entry.getKey(), (String) v);
            } else if (v instanceof Long) {
                writer.setMeta(entry.getKey(), (Long) v);
            } else if (v instanceof byte[]) {
                writer.setMeta(entry.getKey(), (byte[]) v);
            } else {
                throw new IllegalStateException(
                        "Metadata value type must be one of String, Long, or byte[]. Found "
                                + v.getClass().getSimpleName());
            }
        }
        writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(KV<String, UnionValue> element) throws IOException {
        final UnionValue input = element.getValue();
        final GenericRecord record = UnionValue.getAsRecord(schema, input);
        if(fitSchema && DataType.AVRO.equals(input.getType())) {
            final GenericRecord fitted = AvroSchemaUtil.toBuilder(schema, record).build();
            writer.append(fitted);
        } else {
            writer.append(record);
        }
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}