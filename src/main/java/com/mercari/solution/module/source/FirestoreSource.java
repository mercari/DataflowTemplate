package com.mercari.solution.module.source;

import com.google.firestore.v1.*;
import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.module.SourceModule;
import com.mercari.solution.util.OptionUtil;
import com.mercari.solution.util.converter.DocumentToRecordConverter;
import com.mercari.solution.util.converter.DocumentToRowConverter;
import com.mercari.solution.util.converter.RowToDocumentConverter;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.DocumentSchemaUtil;
import com.mercari.solution.util.schema.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FirestoreSource implements SourceModule {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSource.class);

    private static class FirestoreSourceParameters implements Serializable {

        private String projectId;
        private String databaseId;
        private String collection;
        private String filter;
        private List<String> fields;
        private String orderField;
        private StructuredQuery.Direction orderDirection;
        private String parent;
        private Boolean allDescendants;
        private Boolean parallel;

        private Integer pageSize;
        private Long partitionCount;


        public String getProjectId() {
            return projectId;
        }

        public String getDatabaseId() {
            return databaseId;
        }

        public String getCollection() {
            return collection;
        }

        public String getFilter() {
            return filter;
        }

        public List<String> getFields() {
            return fields;
        }

        public String getOrderField() {
            return orderField;
        }

        public StructuredQuery.Direction getOrderDirection() {
            return orderDirection;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public String getParent() {
            return parent;
        }

        public Boolean getAllDescendants() {
            return allDescendants;
        }

        public Boolean getParallel() {
            return parallel;
        }

        public Long getPartitionCount() {
            return partitionCount;
        }

        private OutputType outputType;

        public OutputType getOutputType() {
            return outputType;
        }


        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.collection == null) {
                errorMessages.add("firestore source module collection parameter must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaults(final PInput input) {
            if(this.projectId == null) {
                this.projectId = OptionUtil.getProject(input.getPipeline().getOptions());
            }
            if(this.databaseId == null) {
                this.databaseId = FirestoreUtil.DEFAULT_DATABASE_NAME;
            }
            if(!FirestoreUtil.DEFAULT_DATABASE_NAME.equals(this.databaseId)) {
                input.getPipeline().getOptions().as(FirestoreOptions.class)
                        .setFirestoreDb(this.databaseId);
            }
            if(this.parallel == null) {
                this.parallel = false;
            }
            if(this.allDescendants == null) {
                this.allDescendants = false;
            }
            if(this.parent == null) {
                this.parent = "";
            } else if(!this.parent.startsWith("/")) {
                this.parent = "/" + this.parent;
            }
            if(this.fields == null) {
                this.fields = new ArrayList<>();
            }
            if(this.orderDirection == null) {
                this.orderDirection = StructuredQuery.Direction.ASCENDING;
            }
            if(this.partitionCount == null) {
                final int hintMaxNumWorkers = OptionUtil.getMaxNumWorkers(input);
                this.partitionCount = hintMaxNumWorkers > 1 ? hintMaxNumWorkers - 1: 1L;
            }

            if(this.outputType == null) {
                if(OptionUtil.isStreaming(input)) {
                    this.outputType = OutputType.row;
                } else {
                    this.outputType = OutputType.avro;
                }
            }

        }

        public static FirestoreSourceParameters of(final PInput input, final SourceConfig config) {
            final FirestoreSourceParameters parameters = new Gson().fromJson(config.getParameters(), FirestoreSourceParameters.class);
            if(parameters == null) {
                throw new IllegalArgumentException("firestore source module parameters must not empty!");
            }
            parameters.validate();
            parameters.setDefaults(input);

            return parameters;
        }

    }

    private enum OutputType implements Serializable {
        row,
        avro,
        document
    }

    public String getName() { return "firestore"; }

    public Map<String, FCollection<?>> expand(PBegin begin, SourceConfig config, PCollection<Long> beats, List<FCollection<?>> waits) {

        if(OptionUtil.isStreaming(begin.getPipeline().getOptions())) {
            return Collections.emptyMap();
        } else {
            return Collections.singletonMap(config.getName(), batch(begin, config));
        }
    }

    public static FCollection batch(final PBegin begin, final SourceConfig config) {

        final FirestoreSourceParameters parameters = FirestoreSourceParameters.of(begin, config);
        final Schema outputSchema = SourceConfig.convertSchema(config.getSchema());
        switch (parameters.getOutputType()) {
            case row -> {
                final BatchSource<Schema, Schema, Row> source = new BatchSource<>(
                        outputSchema,
                        parameters,
                        outputSchema,
                        s -> s,
                        DocumentToRowConverter::convert);
                final PCollection<Row> rows = begin
                        .apply(config.getName(), source)
                        .setCoder(RowCoder.of(outputSchema));
                return FCollection.of(config.getName(), rows, DataType.ROW, outputSchema);
            }
            case avro -> {
                final org.apache.avro.Schema outputAvroSchema = SourceConfig.convertAvroSchema(config.getSchema());
                final BatchSource<String, org.apache.avro.Schema, GenericRecord> source = new BatchSource<>(
                        outputSchema,
                        parameters,
                        outputAvroSchema.toString(),
                        AvroSchemaUtil::convertSchema,
                        DocumentToRecordConverter::convert);
                final PCollection<GenericRecord> records = begin
                        .apply(config.getName(), source)
                        .setCoder(AvroCoder.of(outputAvroSchema));
                return FCollection.of(config.getName(), records, DataType.AVRO, outputAvroSchema);
            }
            case document -> {
                final Schema dummyOutputSchema = Schema.builder().addField("dummy", Schema.FieldType.STRING).build();
                final BatchSource<Schema, Schema, Document> source = new BatchSource<>(
                        outputSchema,
                        parameters,
                        dummyOutputSchema,
                        s -> s,
                        DocumentSchemaUtil::convert);
                final PCollection<Document> documents = begin
                        .apply(config.getName(), source)
                        .setCoder(SerializableCoder.of(Document.class));
                return FCollection.of(config.getName(), documents, DataType.DOCUMENT, dummyOutputSchema);
            }
            default ->
                    throw new IllegalArgumentException("firestore source module not support format: " + parameters.getOutputType());
        }
    }

    private static class BatchSource<SchemaInputT,SchemaRuntimeT,T> extends PTransform<PBegin, PCollection<T>> {

        private static final Pattern PATTERN_CONDITION = Pattern.compile("(.+?)(s*>=s*|s*<=s*|s*=s*|s*>s*|s*<s*)(.+)");

        private final Schema schema;
        private final FirestoreSourceParameters parameters;

        private final SchemaInputT inputSchema;
        private final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter;
        private final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter;

        BatchSource(final Schema schema,
                    final FirestoreSourceParameters parameters,
                    final SchemaInputT inputSchema,
                    final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter,
                    final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter) {

            this.schema = schema;
            this.parameters = parameters;
            this.inputSchema = inputSchema;
            this.schemaConverter = schemaConverter;
            this.converter = converter;
        }

        @Override
        public PCollection<T> expand(final PBegin begin) {
            final String parent = createParent();
            if(parameters.getFilter() == null) {
                ListDocumentsRequest.Builder builder = ListDocumentsRequest
                        .newBuilder()
                        .setParent(parent);
                if(parameters.getCollection() != null) {
                    builder = builder.setCollectionId(parameters.getCollection());
                }
                if(!parameters.getFields().isEmpty()) {
                    DocumentMask.Builder maskBuilder = DocumentMask.newBuilder();
                    for(final String field : parameters.getFields()) {
                        maskBuilder.addFieldPaths(field);
                    }
                    builder.setMask(maskBuilder.build());
                }

                final ListDocumentsRequest request = builder.build();
                return begin
                        .apply("CreateListDocumentRequest", Create
                                .of(request)
                                .withCoder(SerializableCoder.of(ListDocumentsRequest.class)))
                        .apply("ListDocument", FirestoreIO.v1().read().listDocuments().build())
                        .apply("Convert", ParDo.of(new ConvertListResponseDoFn(inputSchema, schemaConverter, converter)));
            } else {
                final StructuredQuery structuredQuery = createQuery(schema, parameters);
                final PCollection<RunQueryRequest> runQueryRequests;
                if(parameters.getParallel()) {
                    PartitionQueryRequest request = PartitionQueryRequest.newBuilder()
                            .setParent(parent)
                            .setStructuredQuery(structuredQuery)
                            .setPartitionCount(parameters.getPartitionCount())
                            .build();

                    runQueryRequests = begin
                            .apply("CreatePartitionQuery", Create
                                    .of(request)
                                    .withCoder(SerializableCoder.of(PartitionQueryRequest.class)))
                            .apply("SplitPartitionQuery", FirestoreIO.v1().read().partitionQuery().build());
                } else {
                    final RunQueryRequest runQueryRequest = RunQueryRequest.newBuilder()
                            .setParent(parent)
                            .setStructuredQuery(structuredQuery)
                            .build();

                    runQueryRequests = begin
                            .apply("CreateQuery", Create
                                    .of(runQueryRequest)
                                    .withCoder(SerializableCoder.of(RunQueryRequest.class)));
                }
                return runQueryRequests
                        .apply("RunQuery", FirestoreIO.v1().read().runQuery().build())
                        .apply("FilterEmpty", Filter.by((RunQueryResponse::hasDocument)))
                        .apply("Convert", ParDo.of(new ConvertQueryResponseDoFn(inputSchema, schemaConverter, converter)));

            }
        }

        private class ConvertListResponseDoFn extends DoFn<Document, T> {

            private final SchemaInputT inputSchema;
            private final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter;
            private final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter;


            private transient SchemaRuntimeT runtimeSchema;

            ConvertListResponseDoFn(final SchemaInputT inputSchema,
                                     final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter,
                                     final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.converter = converter;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Document document = c.element();
                final T output = converter.convert(runtimeSchema, document);
                c.output(output);
            }

        }

        private class ConvertQueryResponseDoFn extends DoFn<RunQueryResponse, T> {

            private final SchemaInputT inputSchema;
            private final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter;
            private final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter;


            private transient SchemaRuntimeT runtimeSchema;

            ConvertQueryResponseDoFn(final SchemaInputT inputSchema,
                                     final SchemaUtil.SchemaConverter<SchemaInputT,SchemaRuntimeT> schemaConverter,
                                     final SchemaUtil.DataConverter<SchemaRuntimeT, Document, T> converter) {

                this.inputSchema = inputSchema;
                this.schemaConverter = schemaConverter;
                this.converter = converter;
            }

            @Setup
            public void setup() {
                this.runtimeSchema = schemaConverter.convert(inputSchema);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final RunQueryResponse response = c.element();
                final Document document = response.getDocument();
                final T output = converter.convert(runtimeSchema, document);
                c.output(output);
            }

        }

        private String createParent() {
            final String databaseRootName = FirestoreUtil
                    .createDatabaseRootName(parameters.getProjectId(), parameters.getDatabaseId());
            return databaseRootName + "/documents" + parameters.getParent();
        }

        private StructuredQuery createQuery(
                final Schema schema,
                final FirestoreSourceParameters parameters) {

            StructuredQuery.CollectionSelector.Builder selectorBuilder = StructuredQuery.CollectionSelector
                    .newBuilder()
                    .setAllDescendants(parameters.getAllDescendants());
            if(parameters.getCollection() != null) {
                selectorBuilder = selectorBuilder.setCollectionId(parameters.getCollection());
            }

            final StructuredQuery.Builder builder = StructuredQuery.newBuilder()
                    .addFrom(selectorBuilder);

            if(!parameters.getFields().isEmpty()) {
                final List<StructuredQuery.FieldReference> refers = new ArrayList<>();
                for(String field : parameters.getFields()) {
                    refers.add(StructuredQuery.FieldReference.newBuilder()
                            .setFieldPath(field.trim())
                            .build());
                }
                builder.setSelect(StructuredQuery.Projection.newBuilder()
                        .addAllFields(refers)
                        .build());
            }

            if(parameters.getFilter() != null) {
                try(final Scanner scanner = new Scanner(parameters.getFilter())
                        .useDelimiter("s*ands*|s*ors*|s*ANDs*|s*ORs*")) {

                    final List<StructuredQuery.Filter> filters = new ArrayList<>();
                    while(scanner.hasNext()) {
                        final String fragment = scanner.next();
                        final Matcher matcher = PATTERN_CONDITION.matcher(fragment);
                        if(matcher.find() && matcher.groupCount() > 2) {
                            final String field = matcher.group(1).trim();
                            final String op = matcher.group(2).trim();
                            final String strValue = matcher.group(3)
                                    .trim()
                                    .replaceAll("\"","")
                                    .replaceAll("'","");

                            final Schema.FieldType fieldType = schema.getField(field).getType();
                            final StructuredQuery.FieldFilter.Operator operator = convertOp(op);
                            final Value value = RowToDocumentConverter.getValueFromString(fieldType, strValue);

                            final StructuredQuery.FieldFilter fieldFilter = StructuredQuery.FieldFilter.newBuilder()
                                    .setField(StructuredQuery.FieldReference.newBuilder()
                                            .setFieldPath(field)
                                            .build())
                                    .setOp(operator)
                                    .setValue(value)
                                    .build();
                            filters.add(StructuredQuery.Filter.newBuilder().setFieldFilter(fieldFilter).build());
                        } else {
                            throw new IllegalArgumentException("Failed to build query filter for: " + fragment);
                        }
                    }

                    if(filters.size() == 1) {
                        builder.setWhere(filters.get(0));
                    } else if(filters.size() > 1) {
                        builder.setWhere(StructuredQuery.Filter.newBuilder()
                                .setCompositeFilter(StructuredQuery.CompositeFilter.newBuilder()
                                        .setOp(StructuredQuery.CompositeFilter.Operator.AND)
                                        .addAllFilters(filters)
                                        .build())
                                .build());
                    }

                }
            }

            if(parameters.getOrderField() != null) {
                final StructuredQuery.Order order = StructuredQuery.Order.newBuilder()
                        .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(parameters.getOrderField()).build())
                        .setDirection(parameters.getOrderDirection())
                        .build();
                builder.addOrderBy(order);
            }

            return builder.build();
        }

        private static StructuredQuery.FieldFilter.Operator convertOp(final String op) {
            return switch (op.trim()) {
                case "=" -> StructuredQuery.FieldFilter.Operator.EQUAL;
                case ">" -> StructuredQuery.FieldFilter.Operator.GREATER_THAN;
                case "<" -> StructuredQuery.FieldFilter.Operator.LESS_THAN;
                case ">=" -> StructuredQuery.FieldFilter.Operator.GREATER_THAN_OR_EQUAL;
                case "<=" -> StructuredQuery.FieldFilter.Operator.LESS_THAN_OR_EQUAL;
                case "!=" -> StructuredQuery.FieldFilter.Operator.NOT_EQUAL;
                default -> throw new IllegalArgumentException("Not supported op type: " + op);
            };
        }

    }

}
