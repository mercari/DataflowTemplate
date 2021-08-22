package com.mercari.solution.util;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OptionUtil {

    private static final Pattern PATTERN_OPTION_TIMESTAMP = Pattern.compile("<([Yy]{4})-?([M]{2})?-?([Dd]{2})?T?([Hh]{2})?:?([m]{2})?:?-?([Ss]{2})?>");
    private static final DateTimeFormatter formatterYYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");

    public static Set<String> toSet(final String param) {
        return Optional.ofNullable(param)
                .map(h -> new HashSet<>(Arrays.asList(h.split(","))))
                .orElse(new HashSet<>());
    }

    public static Set<String> toSet(final List<String> params) {
        return Optional.ofNullable(params)
                .map(HashSet::new)
                .orElse(new HashSet<>());
    }

    public static String ifnull(final String str, final String alt) {
        if(str == null) {
            return alt;
        }
        return str;
    }

    public static boolean isDirectRunner(final PipelineOptions options) {
        return PipelineOptions.DirectRunner.class.getSimpleName().equals(options.getRunner().getSimpleName());
    }

    public static boolean isStreaming(final PipelineOptions options) {
        return options.as(StreamingOptions.class).isStreaming();
    }

    public static String replaceParameter(final String text) {
        final Matcher matcher = PATTERN_OPTION_TIMESTAMP.matcher(text);
        while(matcher.find()) {
            //System.out.println(matcher.group());
        }
        Instant now = Instant.now();
        return text
                .replaceAll("<TODAY_[-+]([0-9]{4})>", formatterYYYYMMDD.print(now));
    }

}
