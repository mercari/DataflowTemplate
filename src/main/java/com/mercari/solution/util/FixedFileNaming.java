package com.mercari.solution.util;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FixedFileNaming implements FileIO.Write.FileNaming {

    private final String key;
    private final String suffix;
    private final DateTimeFormatter format;
    private final Boolean useOnlyEndDatetime;

    private FixedFileNaming(final String key, final String suffix) {
        this(key, suffix, "yyyyMMddHHmmss", "Etc/GMT", false);
    }

    private FixedFileNaming(final String key, final String suffix,
                            final String datetimeFormat, final String datetimeFormatZone, final Boolean useOnlyEndDatetime) {
        this.key = key;
        this.suffix = suffix;
        this.format = DateTimeFormat
                .forPattern(datetimeFormat)
                .withZone(DateTimeZone.forID(datetimeFormatZone));
        this.useOnlyEndDatetime = useOnlyEndDatetime;
    }

    public String getFilename(final BoundedWindow window,
                              final PaneInfo pane,
                              final int numShards,
                              final int shardIndex,
                              final Compression compression) {

        if(window != GlobalWindow.INSTANCE) {
            if(!(window instanceof IntervalWindow)) {
                throw new IllegalArgumentException("FixedNaming only support IntervalWindow, but got: "
                        + window.getClass().getSimpleName());
            }
            final IntervalWindow iw = ((IntervalWindow)window);
            if(this.useOnlyEndDatetime) {
                return String.format("%s-%s%s",
                        this.key,
                        iw.end().toString(this.format),
                        this.suffix);
            } else {
                return String.format("%s-%s-%s%s",
                        this.key,
                        iw.start().toString(this.format),
                        iw.end().toString(this.format),
                        this.suffix);
            }
        }

        return String.format("%s%s", this.key, this.suffix);
    }

    public static FixedFileNaming of(final String key, final String suffix) {
        return new FixedFileNaming(key, suffix);
    }

    public static FixedFileNaming of(final String key, final String suffix,
                                     final String datetimeFormat, final String datetimeFormatZone, final Boolean useOnlyEndDatetime) {
        return new FixedFileNaming(key, suffix, datetimeFormat, datetimeFormatZone, useOnlyEndDatetime);
    }

}
