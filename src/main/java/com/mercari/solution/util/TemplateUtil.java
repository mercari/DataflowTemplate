package com.mercari.solution.util;

import freemarker.core.Environment;
import freemarker.template.*;
import org.apache.avro.Schema;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TemplateUtil {

    public static Template createSafeTemplate(final String name, final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_32);
        templateConfig.setNumberFormat("computer");
        templateConfig.setTemplateExceptionHandler(new ImputeSameVariablesTemplateExceptionHandler());
        try {
            return new Template(name, new StringReader(template), templateConfig);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Template createStrictTemplate(final String name, final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_32);
        templateConfig.setNumberFormat("computer");
        //templateConfig.setObjectWrapper(new CSVWrapper(Configuration.VERSION_2_3_30));
        try {
            return new Template(name, new StringReader(template), templateConfig);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String executeStrictTemplate(final String templateText, final Map<String, Object> data) {
        final Template template = createStrictTemplate("template", templateText);
        return executeStrictTemplate(template, data);
    }

    public static String executeStrictTemplate(final Template template, final Map<String, Object> data) {
        try(final StringWriter writer = new StringWriter()) {
            template.process(data, writer);
            return writer.toString();
        } catch (IOException | TemplateException e) {
            throw new RuntimeException("Failed to process template: " + template + ", for data: " + data + ", cause: " + e.getMessage(), e);
        }
    }

    public static boolean isTemplateText(final String text) {
        if(text == null) {
            return false;
        }
        return text.contains("${");
    }

    public static List<String> extractTemplateArgs(final String text, final Schema inputSchema) {
        final List<String> args = new ArrayList<>();
        if(!isTemplateText(text)) {
            return args;
        }
        for(final Schema.Field field : inputSchema.getFields()) {
            if(text.contains(field.name())) {
                args.add(field.name());
            }
        }
        return args;
    }

    public static List<String> extractTemplateArgs(final String text, final org.apache.beam.sdk.schemas.Schema inputSchema) {
        final List<String> args = new ArrayList<>();
        if(!isTemplateText(text)) {
            return args;
        }
        for(final org.apache.beam.sdk.schemas.Schema.Field field : inputSchema.getFields()) {
            if(text.contains(field.getName())) {
                args.add(field.getName());
            }
        }
        return args;
    }

    public static void setFunctions(final Map<String, Object> values) {
        setFunctions(values, "__");
    }

    public static void setFunctions(final Map<String, Object> values, final String prefix) {
        values.put(prefix + "StringUtils", new StringFunctions());
        values.put(prefix + "DateTimeUtils", new DateTimeFunctions());
    }

    public static class StringFunctions {
        public String format(String format, Object... args) {
            return String.format(format, args);
        }

    }

    public static class DateTimeFunctions {

        public String format(String pattern, Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return dateTime.format(DateTimeFormatter.ofPattern(pattern));
        }

        public String format(String pattern, Long epocMicros, String timezone) {
            if(epocMicros == null) {
                return "";
            }
            final Instant timestamp = Instant.ofEpochMilli(epocMicros / 1000L);
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return dateTime.format(DateTimeFormatter.ofPattern(pattern));
        }

        public String format(String pattern, Long epocMicros) {
            return format(pattern, epocMicros, "UTC");
        }

        public String formatTimestamp(Long epocMicros) {
            if(epocMicros == null) {
                return "";
            }
            final Instant timestamp = Instant.ofEpochMilli(epocMicros / 1000L);
            final LocalDateTime dateTime = getLocalDateTime(timestamp, "UTC");
            return dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
        }

        public String formatDateTime(Long epocMicros) {
            return formatDateTime(epocMicros, "UTC");
        }

        public String formatDateTime(Long epocMicros, String timezone) {
            if(epocMicros == null) {
                return "";
            }
            final Instant timestamp = Instant.ofEpochMilli(epocMicros / 1000L);
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
        }

        public String year(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getYear()).toString();
        }

        public String month(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getMonthValue()).toString();
        }

        public String month(Instant timestamp, String timezone, Integer padding) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return String.format("%0" + padding + "d", Integer.valueOf(dateTime.getMonthValue()));
        }

        public String day(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getDayOfMonth()).toString();
        }

        public String day(Instant timestamp, String timezone, Integer padding) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return String.format("%0" + padding + "d", Integer.valueOf(dateTime.getDayOfMonth()));
        }

        private static LocalDateTime getLocalDateTime(final Instant timestamp, final String timezone) {
            return LocalDateTime.ofInstant(timestamp, ZoneId.of(timezone));
        }

    }

    static class ImputeSameVariablesTemplateExceptionHandler implements TemplateExceptionHandler {

        @Override
        public void handleTemplateException(TemplateException te, Environment env, java.io.Writer out) {
            try {
                if(te.getBlamedExpressionString() == null) {
                    throw new IllegalArgumentException(te);
                }
                final List<String> lines = env.getCurrentTemplate().toString().lines().collect(Collectors.toList());
                final String line = lines.get(te.getLineNumber() - 1);
                final String prefix = line.substring(te.getColumnNumber()-2, te.getColumnNumber()-1);
                final String suffix = line.substring(te.getEndColumnNumber(), te.getEndColumnNumber()+1);
                if("{".equals(prefix) && "}".equals(suffix)) {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                } else if("{".equals(prefix) && line.contains("}")) {
                    final int start = te.getColumnNumber()-1;
                    final String target = line.substring(start, line.indexOf("}", start));
                    out.write("${" + target.replaceAll("\"", "'") + "}");
                } else {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class CSVWrapper extends DefaultObjectWrapper {

        CSVWrapper(Version incompatibleImprovements) {
            super(incompatibleImprovements);
        }

        @Override
        protected TemplateModel handleUnknownType(Object obj) throws TemplateModelException {
            if (obj instanceof CSVRecord) {
                return new CSVRecordModel((CSVRecord) obj);
            }
            return super.handleUnknownType(obj);
        }

        public class CSVRecordModel implements TemplateHashModel {
            private final CSVRecord csvRecord;

            public CSVRecordModel(CSVRecord csvRecord) {
                this.csvRecord = csvRecord;
            }

            @Override
            public TemplateModel get(String key) throws TemplateModelException {
                return wrap(csvRecord.get(key));
            }

            @Override
            public boolean isEmpty() {
                return csvRecord.size() == 0;
            }

        }

    }

}
