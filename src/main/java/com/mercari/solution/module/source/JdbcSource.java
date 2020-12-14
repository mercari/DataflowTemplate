package com.mercari.solution.module.source;

import com.google.gson.Gson;
import com.mercari.solution.config.SourceConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.converter.DataTypeTransform;
import com.mercari.solution.util.converter.ResultSetToRecordConverter;
import com.mercari.solution.util.gcp.JdbcUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSource {

    private class JdbcSourceParameters implements Serializable {

        private String query;
        private String url;
        private String driver;
        private String user;
        private String password;
        private String kmsKey;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getKmsKey() {
            return kmsKey;
        }

        public void setKmsKey(String kmsKey) {
            this.kmsKey = kmsKey;
        }
    }

    public static FCollection<GenericRecord> batch(final PBegin begin, final SourceConfig config) {
        final JdbcBatchSource source = new JdbcBatchSource(config);
        final PCollection<GenericRecord> output = begin.apply(config.getName(), source);
        return FCollection.of(config.getName(), output, DataType.AVRO, source.schema);
    }


    public static class JdbcBatchSource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private Schema schema;

        private final JdbcSourceParameters parameters;
        private final String timestampAttribute;
        private final String timestampDefault;

        public JdbcSourceParameters getParameters() {
            return parameters;
        }

        private JdbcBatchSource(final SourceConfig config) {
            this.parameters = new Gson().fromJson(config.getParameters(), JdbcSourceParameters.class);
            this.timestampAttribute = config.getTimestampAttribute();
            this.timestampDefault = config.getTimestampDefault();
        }

        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {

            validateParameters(parameters);
            setDefaultParameters(parameters);

            final String query;
            if(parameters.getQuery().startsWith("gs://")) {
                query = StorageUtil.readString(parameters.getQuery());
            } else {
                query = parameters.getQuery();
            }

            try {
                this.schema = JdbcUtil.createAvroSchema(
                        parameters.getDriver(), parameters.getUrl(),
                        parameters.getUser(), parameters.getPassword(), query);
                final PCollection<GenericRecord> records = begin.apply("QueryToJdbc", JdbcIO.<GenericRecord>read()
                        .withQuery(query)
                        .withRowMapper(ResultSetToRecordConverter::convert)
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create(parameters.getDriver(), parameters.getUrl())
                                .withUsername(parameters.getUser())
                                .withPassword(parameters.getPassword()))
                        .withOutputParallelization(true)
                        .withCoder(AvroCoder.of(this.schema)));

                if(timestampAttribute == null) {
                    return records;
                } else {
                    return records.apply("WithTimestamp", DataTypeTransform
                            .withTimestamp(DataType.AVRO, timestampAttribute, timestampDefault));
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }

        private void validateParameters(final JdbcSourceParameters parameters) {
            if(parameters == null) {
                throw new IllegalArgumentException("Spanner SourceConfig must not be empty!");
            }

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(parameters.getQuery() == null) {
                errorMessages.add("Parameter must contain query");
            }
            if(parameters.getUrl() == null) {
                errorMessages.add("Parameter must contain connection url");
            }
            if(parameters.getDriver() == null) {
                errorMessages.add("Parameter must contain driverClassName");
            }
            if(parameters.getUser() == null) {
                errorMessages.add("Parameter must contain user");
            }
            if(parameters.getPassword() == null) {
                errorMessages.add("Parameter must contain password");
            }

            if(errorMessages.size() > 0) {
                throw new IllegalArgumentException(errorMessages.stream().collect(Collectors.joining(", ")));
            }
        }

        private void setDefaultParameters(final JdbcSourceParameters parameters) {

        }


        /*
        private String decrypt(String key, String body) throws IOException {
            final Credentials credentials =
                    KeyManagementServiceSettings.defaultCredentialsProviderBuilder()
                            .build()
                            .getCredentials();
            final KeyManagementServiceSettings settings =
                    KeyManagementServiceSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();

            final byte[] decoded = Base64.getDecoder().decode(body);
            try(final KeyManagementServiceClient client = KeyManagementServiceClient.create(settings)) {
                final DecryptResponse res = client.decrypt(CryptoKeyName.parse(key), ByteString.copyFrom(decoded));
                return res.getPlaintext().toStringUtf8().trim();
            }
        }
        */

    }
}
