package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.module.DataType;
import com.mercari.solution.module.sink.StorageSink;
import com.mercari.solution.util.pipeline.union.UnionValue;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

public class UnionValueParquetSink implements FileIO.Sink<KV<String, UnionValue>> {

    private final String jsonSchema;
    private final StorageSink.CodecName codecName;
    private final boolean fitSchema;

    private transient Schema schema;
    private transient ParquetWriter<GenericRecord> writer;


    public static UnionValueParquetSink of(
            final Schema schema,
            final StorageSink.CodecName codecName,
            final boolean fitSchema) {

        return new UnionValueParquetSink(schema.toString(), codecName, fitSchema);
    }

    private UnionValueParquetSink(
            final String jsonSchema,
            final StorageSink.CodecName codecName,
            final boolean fitSchema) {

        this.jsonSchema = jsonSchema;
        this.codecName = codecName;
        this.fitSchema = fitSchema;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(this.jsonSchema);
        final BeamParquetOutputFile beamParquetOutputFile =
                new BeamParquetOutputFile(Channels.newOutputStream(channel));
        final CompressionCodecName compressionCodecName = switch (this.codecName) {
            case LZO -> CompressionCodecName.LZO;
            case LZ4 -> CompressionCodecName.LZ4;
            case LZ4_RAW -> CompressionCodecName.LZ4_RAW;
            case ZSTD -> CompressionCodecName.ZSTD;
            case SNAPPY -> CompressionCodecName.SNAPPY;
            case GZIP -> CompressionCodecName.GZIP;
            case BROTLI -> CompressionCodecName.BROTLI;
            default -> CompressionCodecName.UNCOMPRESSED;
        };
        this.writer = AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
                .withSchema(schema)
                .withCompressionCodec(compressionCodecName)
                .withWriteMode(OVERWRITE)
                .enableValidation()
                .build();
    }

    @Override
    public void write(KV<String, UnionValue> element) throws IOException {
        final UnionValue input = element.getValue();
        final GenericRecord record = UnionValue.getAsRecord(schema, input);
        if(fitSchema && DataType.AVRO.equals(input.getType())) {
            final GenericRecord fitted = AvroSchemaUtil.toBuilder(schema, record).build();
            writer.write(fitted);
        } else {
            writer.write(record);
        }
    }

    @Override
    public void flush() throws IOException {
        this.writer.close();
    }

    private static class BeamParquetOutputFile implements OutputFile {

        private final OutputStream outputStream;

        BeamParquetOutputFile(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new BeamOutputStream(outputStream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new BeamOutputStream(outputStream);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static class BeamOutputStream extends PositionOutputStream {
        private long position = 0;
        private final OutputStream outputStream;

        private BeamOutputStream(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            position++;
            outputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

}
