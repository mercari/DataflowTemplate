package com.mercari.solution.module.sink;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.module.*;
import com.mercari.solution.util.schema.converter.ToStatementConverter;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.cloud.google.SecretManagerUtil;
import com.mercari.solution.util.pipeline.Union;
import com.mercari.solution.util.domain.db.stmt.PreparedStatementTemplate;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Sink.Module(name="jdbc")
public class JdbcSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    private static class Parameters implements Serializable {

        private String table;
        private String url;
        private String driver;
        private String user;
        private String password;
        private String kmsKey;
        private Boolean createTable;
        private Boolean emptyTable;
        private List<String> keyFields;
        private Integer batchSize;
        private Integer bulkInsertSize;
        private String op;


        private void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(table == null) {
                errorMessages.add("Parameter must contain table");
            }
            if(url == null) {
                errorMessages.add("Parameter must contain connection url");
            }
            if(driver == null) {
                errorMessages.add("Parameter must contain driverClassName");
            }
            if(user == null) {
                errorMessages.add("Parameter must contain user");
            }
            if(password == null) {
                errorMessages.add("Parameter must contain password");
            }
            if(batchSize != null && batchSize < 1) {
                errorMessages.add("Parameter batchSize must be greater than or equal to 1");
            }
            if(bulkInsertSize != null && bulkInsertSize < 1) {
                errorMessages.add("Parameter bulkInsertSize must be greater than or equal to 1");
            }
            if((JdbcUtil.OP.INSERT_OR_UPDATE.name().equals(op)
                    || JdbcUtil.OP.INSERT_OR_DONOTHING.name().equals(op))
                    && (keyFields == null || keyFields.isEmpty())) {
                errorMessages.add("Parameter keyFields must not be empty for op: " + op);
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults() {
            if(createTable == null) {
                this.createTable = false;
            }
            if(emptyTable == null) {
                emptyTable = false;
            }
            if(op == null) {
                op = JdbcUtil.OP.INSERT.name();
            }
            if(batchSize == null) {
                batchSize = 1000;
            }
            if(bulkInsertSize == null) {
                bulkInsertSize = 1;
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
        }

        public void replaceParameters() {
            if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
                try(final SecretManagerServiceClient secretClient = SecretManagerUtil.createClient()) {
                    if(SecretManagerUtil.isSecretName(user)) {
                        user = SecretManagerUtil.getSecret(secretClient, user).toStringUtf8();
                    }
                    if(SecretManagerUtil.isSecretName(password)) {
                        password = SecretManagerUtil.getSecret(secretClient, password).toStringUtf8();
                    }
                }
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final JdbcUtil.DB db = getDB(parameters.driver);
        final List<List<String>> ddls;
        if (parameters.createTable) {
            ddls = new ArrayList<>();
            final String ddl = JdbcUtil.buildCreateTableSQL(
                    inputSchema.getAvroSchema(), parameters.table, db, parameters.keyFields);
            ddls.add(Arrays.asList(ddl));
        } else {
            ddls = new ArrayList<>();
        }
        if (parameters.emptyTable) {
            ddls.add(Arrays.asList("DELETE FROM " + parameters.table));
        }

        final PCollection<MElement> tableReady;
        if(ddls.isEmpty()) {
            tableReady = input;
        } else {
            final PCollection<String> wait = input.getPipeline()
                    .apply("SupplyDDL", Create.of(ddls).withCoder(ListCoder.of(StringUtf8Coder.of())))
                    .apply("PrepareTable", ParDo.of(new TablePrepareDoFn(
                            parameters.driver, parameters.url, parameters.user, parameters.password)));
            tableReady = input
                    .apply("WaitToTableCreation", Wait.on(wait))
                    .setCoder(input.getCoder());
        }

        final PCollection<MElement> results = tableReady
                .apply("WriteJdbc", ParDo.of(new WriteDoFn(
                        parameters.driver, parameters.url, parameters.user, parameters.password,
                        parameters.table, inputSchema.getAvroSchema(), JdbcUtil.OP.valueOf(parameters.op), db,
                        parameters.keyFields, parameters.batchSize, parameters.bulkInsertSize)));

        return MCollectionTuple
                .of(results, Schema.builder().withField("dummy", Schema.FieldType.STRING).build());
    }

    private JdbcUtil.DB getDB(final String driver) {
        if(driver.contains("mysql")) {
            return JdbcUtil.DB.MYSQL;
        } else if(driver.contains("postgresql")) {
            return JdbcUtil.DB.POSTGRESQL;
        } else {
            throw new IllegalStateException("Not supported JDBC driver: " + driver);
        }
    }

    private static class WriteDoFn extends DoFn<MElement, MElement> {

        private final String driver;
        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private final org.apache.avro.Schema schema;
        private final JdbcUtil.OP op;
        private final JdbcUtil.DB db;
        private final List<String> keyFields;
        private final int batchSize;
        private final int bulkInsertSize;
        private final int fieldSize;

        private transient JdbcUtil.CloseableDataSource dataSource;
        private transient PreparedStatementTemplate statementTemplate;
        private transient Connection connection = null;
        private transient PreparedStatement preparedStatement;

        private transient List<MElement> elementBuffer;
        private transient int batchBufferSize;

        public WriteDoFn(
                final String driver,
                final String url,
                final String user,
                final String password,
                final String table,
                final org.apache.avro.Schema schema,
                final JdbcUtil.OP op,
                final JdbcUtil.DB db,
                final List<String> keyFields,
                final int batchSize,
                final int bulkInsertSize) {

            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
            this.table = table;
            this.schema = schema;
            this.op = op;
            this.db = db;
            this.keyFields = keyFields;
            this.batchSize = batchSize;
            this.bulkInsertSize = bulkInsertSize;
            this.fieldSize = schema.getFields().size();
        }


        @Setup
        public void setup() {
            this.statementTemplate = createStatementTemplate(bulkInsertSize);
            this.dataSource = JdbcUtil.createDataSource(driver, url, user, password);
        }

        @Teardown
        public void teardown() throws Exception {
            cleanUpDataSource();
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws Exception {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(statementTemplate.getStatementString());
            }
            elementBuffer = new ArrayList<>(bulkInsertSize);
            batchBufferSize = 0;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            try {
                elementBuffer.add(c.element());
                if (elementBuffer.size() >= bulkInsertSize) {
                    addBufferedElementsToBatch(statementTemplate, preparedStatement, true);
                    elementBuffer.clear();
                }
            } catch (SQLException e) {
                preparedStatement.clearBatch();
                connection.rollback();
                elementBuffer.clear();
                batchBufferSize = 0;
                throw new RuntimeException(e);
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            try {
                boolean flushed = false;
                if (batchBufferSize > 0) {
                    preparedStatement.executeBatch();
                    batchBufferSize = 0;
                    flushed = true;
                }
                if (!elementBuffer.isEmpty()) {
                    executeRemainingElements();
                    flushed = true;
                }
                if (flushed) {
                    connection.commit();
                }
                cleanUpStatementAndConnection();
            } catch (SQLException e) {
                preparedStatement.clearBatch();
                connection.rollback();
                cleanUpStatementAndConnection();
                throw new RuntimeException(e);
            }
        }

        private void addBufferedElementsToBatch(
                final PreparedStatementTemplate template,
                final PreparedStatement statement,
                final boolean commitOnBatchSize) throws SQLException {

            statement.clearParameters();
            for (int i = 0; i < elementBuffer.size(); i++) {
                ToStatementConverter.convertElement(
                        elementBuffer.get(i),
                        template.createPlaceholderSetterProxy(statement, i * fieldSize));
            }
            statement.addBatch();
            batchBufferSize += 1;

            if (commitOnBatchSize && batchBufferSize >= batchSize) {
                statement.executeBatch();
                connection.commit();
                batchBufferSize = 0;
            }
        }

        private PreparedStatementTemplate createStatementTemplate(final int size) {
            return JdbcUtil.createStatement(table, schema, op, db, keyFields, size);
        }

        private void executeRemainingElements() throws SQLException {
            final PreparedStatementTemplate partialTemplate = createStatementTemplate(elementBuffer.size());
            try(final PreparedStatement partialStatement = connection.prepareStatement(partialTemplate.getStatementString())) {
                addBufferedElementsToBatch(partialTemplate, partialStatement, false);
                if(batchBufferSize > 0) {
                    partialStatement.executeBatch();
                    batchBufferSize = 0;
                }
            } finally {
                elementBuffer.clear();
            }
        }

        private void cleanUpStatementAndConnection() throws Exception {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } finally {
                    preparedStatement = null;
                }
            }

            if(connection != null) {
                try {
                    connection.close();
                } finally {
                    connection = null;
                }
            }
        }

        private void cleanUpDataSource() throws Exception {
            cleanUpStatementAndConnection();

            if(dataSource != null) {
                try {
                    dataSource.close();
                } catch (IOException e) {
                } finally {
                    dataSource = null;
                }
            }
        }
    }

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TablePrepareDoFn.class);

        private final String driver;
        private final String url;
        private final String user;
        private final String password;

        TablePrepareDoFn(final String driver, final String url, final String user, final String password) {
            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            final List<String> ddl = c.element();
            if(ddl == null) {
                return;
            }
            if(ddl.isEmpty()) {
                c.output("ok");
                return;
            }
            try(final JdbcUtil.CloseableDataSource dataSource = JdbcUtil.createDataSource(driver, url, user, password)) {
                try(final Connection connection = dataSource.getConnection()) {
                    for(final String sql : ddl) {
                        LOG.info("ExecuteDDL: " + sql);
                        connection.createStatement().executeUpdate(sql);
                        connection.commit();
                        LOG.info("ExecutedDDL: " + sql);
                    }
                }
            }
            c.output("ok");
        }
    }

}
