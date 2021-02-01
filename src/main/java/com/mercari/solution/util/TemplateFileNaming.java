package com.mercari.solution.util;

import freemarker.template.Template;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

import java.io.StringWriter;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TemplateFileNaming implements FileIO.Write.FileNaming {

    private final String key;

    private TemplateFileNaming(final String key) {
        this.key = key;
    }

    public String getFilename(final BoundedWindow window,
                              final PaneInfo pane,
                              final int numShards,
                              final int shardIndex,
                              final Compression compression) {

        return createFileName(key, window, pane, numShards, shardIndex);
    }

    public static Map<String, Object> createContextParams(final BoundedWindow window,
                                                          final PaneInfo pane,
                                                          final int numShards,
                                                          final int shardIndex) {

        final Map<String, Object> map = new HashMap<>();
        map.put("__NUM_SHARDS__", numShards);
        map.put("__SHARD_INDEX__", shardIndex);

        // Window Path
        if(window != GlobalWindow.INSTANCE) {
            if(!(window instanceof IntervalWindow)) {
                throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                        + window.getClass().getSimpleName());
            }
            final IntervalWindow iw = ((IntervalWindow)window);
            map.put("__WINDOW_START__", iw.start());
            map.put("__WINDOW_END__", iw.end());
        } else {
            map.put("__WINDOW_START__", Instant.ofEpochSecond(0L));
            map.put("__WINDOW_END__", Instant.ofEpochSecond(4102444800L));
        }

        // Pane Path
        if(!pane.isFirst() || !pane.isLast()) {
            map.put("__PANE_INDEX__", pane.getIndex());
        }

        return map;
    }

    public static Map<String, Object> createContextParams(final BoundedWindow window,
                                                          final PaneInfo pane) {

        final Map<String, Object> map = new HashMap<>();

        // Window Path
        if(window != GlobalWindow.INSTANCE) {
            if(!(window instanceof IntervalWindow)) {
                throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                        + window.getClass().getSimpleName());
            }
            final IntervalWindow iw = ((IntervalWindow)window);
            map.put("__WINDOW_START__", iw.start());
            map.put("__WINDOW_END__", iw.end());
        } else {
            map.put("__WINDOW_START__", Instant.ofEpochSecond(0L));
            map.put("__WINDOW_END__", Instant.ofEpochSecond(4102444800L));
        }

        // Pane Path
        if(!pane.isFirst() || !pane.isLast()) {
            map.put("__PANE_INDEX__", pane.getIndex());
        }

        return map;
    }

    public static String createFileName(final String templateString,
                                        final BoundedWindow window,
                                        final PaneInfo pane,
                                        final int numShards,
                                        final int shardIndex) {

        final Map<String, Object> map = new HashMap<>();
        map.put("__NUM_SHARDS__", numShards);
        map.put("__SHARD_INDEX__", shardIndex);
        final StringWriter writer = new StringWriter();

        StringBuilder templateText = new StringBuilder(templateString);

        // Window Path
        if(window != GlobalWindow.INSTANCE) {
            if(!(window instanceof IntervalWindow)) {
                throw new IllegalArgumentException("TemplateFileNaming only support IntervalWindow, but got: "
                        + window.getClass().getSimpleName());
            }
            final IntervalWindow iw = ((IntervalWindow)window);
            map.put("__WINDOW_START__", iw.start());
            map.put("__WINDOW_END__", iw.end());
            if(!templateString.contains("__WINDOW_START__") && !templateString.contains("__WINDOW_END__")) {
                templateText
                        .append("-")
                        .append(iw.start().toString())
                        .append("-")
                        .append(iw.end().toString());
            }
        } else {
            map.put("__WINDOW_START__", Instant.ofEpochSecond(0L));
            map.put("__WINDOW_END__", Instant.ofEpochSecond(4102444800L));
            if(templateString.contains("__WINDOW_START__") || templateString.contains("__WINDOW_END__")) {
                templateText = new StringBuilder(templateText.toString()
                        .replaceAll("__WINDOW_START__", "")
                        .replaceAll("__WINDOW_END__", ""));
            }
        }

        // Pane Path
        if(!pane.isFirst() || !pane.isLast()) {
            map.put("__PANE_INDEX__", pane.getIndex());
            if(!templateString.contains("__PANE_INDEX__")) {
                if (templateText.length() > 0) {
                    templateText.append("-");
                }
                templateText.append(pane.getIndex());
            }
        } else {
            if(templateString.contains("__PANE_INDEX__")) {
                templateText = new StringBuilder(templateText.toString()
                        .replaceAll("__PANE_INDEX__", ""));
            }
        }

        // NumShards Path Default
        if(numShards > 1 && !templateString.contains("__SHARD_INDEX__")) {
            final String numShardsStr = String.valueOf(numShards);
            final DecimalFormat df = new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
            templateText
                    .append(df.format(shardIndex))
                    .append("-of-")
                    .append(df.format(numShards));
        } else {
            if(templateString.contains("__SHARD_INDEX__")) {
                templateText = new StringBuilder(templateText.toString()
                        .replaceAll("__SHARD_INDEX__", ""));
            }
        }

        try {
            final Template template = TemplateUtil.createStrictTemplate("templateFileNaming", templateText.toString());
            template.process(map, writer);
            return writer.toString();
        } catch (Exception e) {
            throw new IllegalArgumentException("Template");
        }
    }

    public static TemplateFileNaming of(final String key) {
        return new TemplateFileNaming(key);
    }

}
