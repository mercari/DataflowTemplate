package com.mercari.solution.util;

import freemarker.template.Template;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

import java.io.StringWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TemplateFileNaming implements FileIO.Write.FileNaming {

    private static final DateTimeUtil.DateTimeTemplateUtils datetimeUtils = new DateTimeUtil.DateTimeTemplateUtils();

    private final String template;
    private final String key;

    private TemplateFileNaming(final String template, final String key) {
        this.template = template;
        this.key = key;
    }

    @Override
    public String getFilename(final BoundedWindow window,
                              final PaneInfo pane,
                              final int numShards,
                              final int shardIndex,
                              final Compression compression) {

        return createFileName(template, key, window, pane, numShards, shardIndex);
    }

    private static String createFileName(final String templateString,
                                         final String key,
                                         final BoundedWindow window,
                                         final PaneInfo pane,
                                         final int numShards,
                                         final int shardIndex) {

        final Instant windowStart;
        final Instant windowEnd;
        if(window != GlobalWindow.INSTANCE) {
            if(!(window instanceof IntervalWindow)) {
                throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                        + window.getClass().getSimpleName());
            }
            final IntervalWindow iw = ((IntervalWindow)window);
            windowStart = Instant.ofEpochMilli(iw.start().getMillis());
            windowEnd   = Instant.ofEpochMilli(iw.end().getMillis());
        } else {
            windowStart = Instant.ofEpochMilli(0L);
            windowEnd   = Instant.ofEpochMilli(4102444800L);
        }

        // Pane Path
        final long paneIndex;
        if(!pane.isFirst() || !pane.isLast()) {
            paneIndex = pane.getIndex();
        } else {
            paneIndex = 0;
        }

        final Map<String, Object> map = new HashMap<>();
        map.put("_DateTimeUtil", datetimeUtils);

        map.put("__KEY__", key);
        map.put("__NUM_SHARDS__", numShards);
        map.put("__SHARD_INDEX__", shardIndex);
        map.put("__WINDOW_START__", windowStart);
        map.put("__WINDOW_END__", windowEnd);
        map.put("__PANE_INDEX__", paneIndex);

        final StringWriter writer = new StringWriter();
        try {
            final Template template = TemplateUtil.createSafeTemplate("templateFileNaming", templateString);
            template.process(map, writer);
            return writer.toString();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create filename: " + templateString);
        }
    }

    public static TemplateFileNaming of(final String key) {
        return new TemplateFileNaming(key, key);
    }

    public static TemplateFileNaming of(final String template, final String key) {
        return new TemplateFileNaming(template, key);
    }

}
