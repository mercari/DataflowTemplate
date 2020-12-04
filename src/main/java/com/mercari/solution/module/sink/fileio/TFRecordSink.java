package com.mercari.solution.module.sink.fileio;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class TFRecordSink implements FileIO.Sink<Row> {

    private final Schema schema;
    private final RecordFormatter formatter;
    private transient WritableByteChannel channel;
    private transient TFRecordCodec codec;

    private TFRecordSink(final Schema schema, final RecordFormatter formatter) {
        this.schema = schema;
        this.formatter = formatter;
    }


    @Override
    public void open(WritableByteChannel channel) {
        this.channel = channel;
        this.codec = new TFRecordCodec();
    }

    @Override
    public void write(Row element) throws IOException {
        codec.write(channel, this.formatter.formatRecord(schema, element));
    }

    @Override
    public void flush() {
        // Nothing to do here.
    }

    static class TFRecordCodec {
        private static final int HEADER_LEN = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
        private static final int FOOTER_LEN = Integer.SIZE / Byte.SIZE;
        private static HashFunction crc32c = Hashing.crc32c();

        private ByteBuffer header = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.LITTLE_ENDIAN);
        private ByteBuffer footer = ByteBuffer.allocate(FOOTER_LEN).order(ByteOrder.LITTLE_ENDIAN);

        private int mask(int crc) {
            return ((crc >>> 15) | (crc << 17)) + 0xa282ead8;
        }

        private int hashLong(long x) {
            return mask(crc32c.hashLong(x).asInt());
        }

        private int hashBytes(byte[] x) {
            return mask(crc32c.hashBytes(x).asInt());
        }

        public int recordLength(byte[] data) {
            return HEADER_LEN + data.length + FOOTER_LEN;
        }

        public byte[] read(ReadableByteChannel inChannel) throws IOException {
            header.clear();
            int headerBytes = read(inChannel, header);
            if (headerBytes == 0) {
                return null;
            }

            header.rewind();
            long length64 = header.getLong();
            long lengthHash = hashLong(length64);
            int maskedCrc32OfLength = header.getInt();
            if (lengthHash != maskedCrc32OfLength) {
                throw new IOException(
                        String.format(
                                "Mismatch of length mask when reading a record. Expected %d but received %d.",
                                maskedCrc32OfLength, lengthHash));
            }
            int length = (int) length64;
            if (length != length64) {
                throw new IOException(String.format("length overflow %d", length64));
            }

            ByteBuffer data = ByteBuffer.allocate(length);
            readFully(inChannel, data);

            footer.clear();
            readFully(inChannel, footer);
            footer.rewind();

            int maskedCrc32OfData = footer.getInt();
            int dataHash = hashBytes(data.array());
            if (dataHash != maskedCrc32OfData) {
                throw new IOException(
                        String.format(
                                "Mismatch of data mask when reading a record. Expected %d but received %d.",
                                maskedCrc32OfData, dataHash));
            }
            return data.array();
        }

        public void write(WritableByteChannel outChannel, byte[] data) throws IOException {
            int maskedCrc32OfLength = hashLong(data.length);
            int maskedCrc32OfData = hashBytes(data);

            header.clear();
            header.putLong(data.length).putInt(maskedCrc32OfLength);
            header.rewind();
            writeFully(outChannel, header);

            writeFully(outChannel, ByteBuffer.wrap(data));

            footer.clear();
            footer.putInt(maskedCrc32OfData);
            footer.rewind();
            writeFully(outChannel, footer);
        }

        static void readFully(ReadableByteChannel in, ByteBuffer bb) throws IOException {
            int expected = bb.remaining();
            int actual = read(in, bb);
            if (expected != actual) {
                throw new IOException(String.format("expected %d, but got %d", expected, actual));
            }
        }

        private static int read(ReadableByteChannel in, ByteBuffer bb) throws IOException {
            int expected = bb.remaining();
            while (bb.hasRemaining() && in.read(bb) >= 0) {}
            return expected - bb.remaining();
        }

        static void writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    public static TFRecordSink of(final Schema schema, final RecordFormatter formatter) {
        return new TFRecordSink(schema, formatter);
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        byte[] formatRecord(Schema schema, ElementT element);
    }

}
