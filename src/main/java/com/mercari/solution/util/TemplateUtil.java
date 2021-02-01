package com.mercari.solution.util;

import freemarker.core.Environment;
import freemarker.template.*;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;

public class TemplateUtil {

    public static Template createSafeTemplate(final String name, final String template) throws IOException {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_30);
        templateConfig.setNumberFormat("computer");
        templateConfig.setTemplateExceptionHandler(new ImputeSameVariablesTemplateExceptionHandler());
        return new Template(name, new StringReader(template), templateConfig);
    }

    public static Template createStrictTemplate(final String name, final String template) throws IOException {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_30);
        templateConfig.setNumberFormat("computer");
        //templateConfig.setObjectWrapper(new CSVWrapper(Configuration.VERSION_2_3_30));
        return new Template(name, new StringReader(template), templateConfig);
    }

    static class ImputeSameVariablesTemplateExceptionHandler implements TemplateExceptionHandler {

        @Override
        public void handleTemplateException(TemplateException te, Environment env, java.io.Writer out) {
            try {
                if(te.getBlamedExpressionString() == null) {
                    throw new IllegalArgumentException(te);
                }
                out.write("${" + te.getBlamedExpressionString() + "}");
            } catch (IOException e) {}
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
