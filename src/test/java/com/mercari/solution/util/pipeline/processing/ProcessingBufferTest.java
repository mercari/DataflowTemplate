package com.mercari.solution.util.pipeline.processing;

import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.coders.Coder;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProcessingBufferTest {

    @Test
    public void testUpdateCount() {

        final List<Object> strings = new ArrayList<>();
        strings.add("a");
        strings.add("b");
        strings.add("c");
        strings.add("e");
        strings.add("f");
        strings.add("g");
        strings.add("h");
        strings.add("i");
        strings.add("j");
        strings.add("k");

        final long baseEpochMillis = Instant.parse("2023-01-01T00:00:00Z").getMillis();

        final ProcessingBuffer buffer = new ProcessingBuffer();
        for(long i = 0L; i<30L; i++) {
            long epochMillis = baseEpochMillis + (i * 1000 * 60 * 60 * 24);
            if(i<10L) {
                buffer.add("strings", strings.get((int)i), Instant.ofEpochMilli(epochMillis));
            }
            if(i<20L) {
                buffer.add("longs", i, Instant.ofEpochMilli(epochMillis));
            }
            buffer.add("doubles", (double)i, Instant.ofEpochMilli(epochMillis));
        }

        buffer.types.put("strings", ProcessingBuffer.FieldType.string.id);
        buffer.types.put("longs", ProcessingBuffer.FieldType.int64.id);
        buffer.types.put("doubles", ProcessingBuffer.FieldType.float64.id);

        buffer.sizes.put("strings", 8);
        buffer.sizes.put("longs", 10);
        buffer.sizes.put("doubles", 12);

        buffer.units.put("strings", Processor.SizeUnit.count.id);
        buffer.units.put("longs", Processor.SizeUnit.count.id);
        buffer.units.put("doubles", Processor.SizeUnit.count.id);

        Assert.assertEquals(10, buffer.values.get("strings").size());
        Assert.assertEquals(20, buffer.values.get("longs").size());
        Assert.assertEquals(30, buffer.values.get("doubles").size());

        buffer.update(Instant.now());

        Assert.assertEquals(8, buffer.values.get("strings").size());
        Assert.assertEquals(10, buffer.values.get("longs").size());
        Assert.assertEquals(12, buffer.values.get("doubles").size());

        Assert.assertEquals(8, buffer.timestamps.get("strings").size());
        Assert.assertEquals(10, buffer.timestamps.get("longs").size());
        Assert.assertEquals(12, buffer.timestamps.get("doubles").size());

        Assert.assertEquals("k", buffer.get("strings", 0));
        Assert.assertEquals("c", buffer.get("strings", 7));
        Assert.assertNull(buffer.get("strings", 8));
        Assert.assertEquals(19L, buffer.get("longs", 0));
        Assert.assertEquals(10L, buffer.get("longs", 9));
        Assert.assertNull(buffer.get("longs", 10));
        Assert.assertEquals(29D, buffer.get("doubles", 0));
        Assert.assertEquals(18D, buffer.get("doubles", 11));
        Assert.assertNull(buffer.get("doubles", 12));

        final long baseEpochMicros = baseEpochMillis * 1000L;
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (10 - 8), buffer.timestamps.get("strings").get(0).longValue());
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (20 - 10), buffer.timestamps.get("longs").get(0).longValue());
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (30 - 12), buffer.timestamps.get("doubles").get(0).longValue());
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (10 - 1), buffer.timestamps.get("strings").get(7).longValue());
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (20 - 1), buffer.timestamps.get("longs").get(9).longValue());
        Assert.assertEquals(baseEpochMicros + 1000_000L * 60 * 60 * 24 * (30 - 1), buffer.timestamps.get("doubles").get(11).longValue());
    }

    @Test
    public void testCoder() {

        final ProcessingBuffer buffer = new ProcessingBuffer();

        buffer.types.put("strings", ProcessingBuffer.FieldType.string.id);
        buffer.types.put("longs", ProcessingBuffer.FieldType.int64.id);
        buffer.types.put("doubles", ProcessingBuffer.FieldType.float64.id);

        final Long currentTimestampMillis = Instant.now().getMillis();

        buffer.add("strings", "a", Instant.ofEpochMilli(currentTimestampMillis));
        buffer.add("strings", "b", Instant.ofEpochMilli(currentTimestampMillis + 1L));
        buffer.add("strings", "c", Instant.ofEpochMilli(currentTimestampMillis + 2L));

        buffer.add("longs", 1L, Instant.ofEpochMilli(currentTimestampMillis + 3L));
        buffer.add("longs", null, Instant.ofEpochMilli(currentTimestampMillis + 4L));
        buffer.add("longs", 3L, Instant.ofEpochMilli(currentTimestampMillis + 5L));

        buffer.add("doubles", 1.1D, Instant.ofEpochMilli(currentTimestampMillis + 6L));
        buffer.add("doubles", -2.2D, Instant.ofEpochMilli(currentTimestampMillis + 7L));

        buffer.sizes.put("strings", 3);
        buffer.sizes.put("longs", 2);
        buffer.sizes.put("doubles", 1);

        buffer.units.put("strings", Processor.SizeUnit.count.id);
        buffer.units.put("longs", Processor.SizeUnit.count.id);
        buffer.units.put("doubles", Processor.SizeUnit.count.id);

        final long currentTimestampMicros = currentTimestampMillis * 1000;

        final Coder<ProcessingBuffer> coder = ProcessingBuffer.coder();

        try(final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            coder.encode(buffer, os);
            final byte[] encoded = os.toByteArray();
            final ProcessingBuffer decoded = coder.decode(new ByteArrayInputStream(encoded));
            Assert.assertEquals(Arrays.asList("c", "b", "a"), decoded.getValues("strings", 0, 3, Processor.SizeUnit.count));
            Assert.assertEquals(Arrays.asList(3L, null, 1L), decoded.getValues("longs", 0, 3, Processor.SizeUnit.count));
            Assert.assertEquals(Arrays.asList(-2.2D, 1.1D), decoded.getValues("doubles", 0, 2, Processor.SizeUnit.count));

            Assert.assertEquals(Arrays.asList(currentTimestampMicros, currentTimestampMicros + 1000L, currentTimestampMicros + 2000L), decoded.timestamps.get("strings"));
            Assert.assertEquals(Arrays.asList(currentTimestampMicros + 3000L, currentTimestampMicros + 4000L, currentTimestampMicros + 5000L), decoded.timestamps.get("longs"));
            Assert.assertEquals(Arrays.asList(currentTimestampMicros + 6000L, currentTimestampMicros + 7000L), decoded.timestamps.get("doubles"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
