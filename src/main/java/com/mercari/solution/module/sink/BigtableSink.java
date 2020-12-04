package com.mercari.solution.module.sink;

import com.google.bigtable.v2.Mutation;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.mercari.solution.config.SinkConfig;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.RowToBigtableConverter;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class BigtableSink {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableSink.class);

    private class BigtableSinkParameters {

        private String projectId;
        private String instanceId;
        private String tableId;
        private List<String> rowKeyFields;
        private String columnFamily;
        private String separator;

        private List<String> fields;
        private Boolean exclude;


        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getTableId() {
            return tableId;
        }

        public void setTableId(String tableId) {
            this.tableId = tableId;
        }

        public List<String> getRowKeyFields() {
            return rowKeyFields;
        }

        public void setRowKeyFields(List<String> rowKeyFields) {
            this.rowKeyFields = rowKeyFields;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public Boolean getExclude() {
            return exclude;
        }

        public void setExclude(Boolean exclude) {
            this.exclude = exclude;
        }

    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config) {
        return write(collection, config, null);
    }

    public static FCollection<?> write(final FCollection<?> collection, final SinkConfig config, final List<FCollection<?>> waits) {
        final BigtableSinkParameters parameters = new Gson().fromJson(config.getParameters(), BigtableSinkParameters.class);
        final BigtableWrite write = new BigtableWrite(collection, parameters);
        final PCollection output = collection.getCollection().apply(config.getName(), write);
        try {
            config.outputAvroSchema(collection.getAvroSchema());
        } catch (Exception e) {
            LOG.error("Failed to output avro schema for " + config.getName() + " to path: " + config.getOutputAvroSchema(), e);
        }
        return FCollection.update(collection, output);
    }

    public static class BigtableWrite extends PTransform<PCollection<?>, PCollection<BigtableWriteResult>> {

        private static final Logger LOG = LoggerFactory.getLogger(BigtableWrite.class);

        private FCollection<?> collection;
        private final BigtableSinkParameters parameters;

        private BigtableWrite(final FCollection<?> collection, final BigtableSinkParameters parameters) {
            this.collection = collection;
            this.parameters = parameters;
        }

        public PCollection<BigtableWriteResult> expand(final PCollection<?> input) {
            validateParameters();
            setDefaultParameters();

            final Set<String> fields = OptionUtil.toSet(this.parameters.getFields());

            final Set<String> excludeFields;
            if(fields.size() > 0) {
                if(parameters.getExclude()) {
                    excludeFields = fields;
                } else {
                    excludeFields = collection.getSchema().getFields().stream()
                            .map(Schema.Field::getName)
                            .collect(Collectors.toSet());
                    excludeFields.removeAll(fields);
                }
            } else {
                excludeFields = new HashSet<>();
            }

            final PCollection<KV<ByteString, Iterable<Mutation>>> mutations = input
                    .apply("ToMutation", DataTypeTransform
                            .bigtableMutation(collection, parameters.getColumnFamily(), parameters.getRowKeyFields(), excludeFields));

            final PCollection<BigtableWriteResult> writeResults = mutations
                    .apply("WriteBigtable", BigtableIO.write()
                            .withProjectId(parameters.getProjectId())
                            .withInstanceId(parameters.getInstanceId())
                            .withTableId(parameters.getTableId())
                            .withoutValidation()
                            .withWriteResults());

            return writeResults;
        }

        private void validateParameters() {
            if(this.parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getProjectId() == null) {
                errorMessages.add("Parameter must contain projectId");
            }
            if(parameters.getInstanceId() == null) {
                errorMessages.add("Parameter must contain instanceId");
            }
            if(parameters.getTableId() == null) {
                errorMessages.add("Parameter must contain tableId");
            }
            if(parameters.getColumnFamily() == null) {
                errorMessages.add("Parameter must contain columnFamily");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters() {
            if(parameters.getSeparator() == null) {
                parameters.setSeparator("#");
            }
        }

        private String set(final String parent){
            if(parent != null && parent.length() > 0) {
                if(parent.length() > 64) {
                    return parent.substring(0, 64);
                } else {
                    return parent;
                }
            } else {
                return "r";
            }
        }

    }

    private static class RowToMutationDoFn extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

        private final String rowKeyFields;
        private transient List<String> keyFields;
        private final String columnFamily;
        private final String separator;

        public RowToMutationDoFn(final String rowKeyFields, final String columnFamily, final String separator) {
            this.rowKeyFields = rowKeyFields;
            this.columnFamily = columnFamily;
            this.separator = separator;
        }

        @Setup
        public void setup() {
            this.keyFields = Arrays.asList(rowKeyFields.split(","));
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final Row row = c.element();
            for(final String keyField : keyFields) {
                if(!row.getSchema().hasField(keyField) || row.getValue(keyField) == null) {
                    return;
                }
            }
            c.output(RowToBigtableConverter.convert(row, keyFields, columnFamily, separator));
        }

    }

}
