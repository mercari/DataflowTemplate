package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;

import java.io.IOException;

public class PubSubUtil {

    public static Pubsub pubsub() {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = new JacksonFactory();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Pubsub.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("PubSubClient")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
