package com.mercari.solution.util.gcp;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;

import java.io.IOException;

public class DataflowUtil {

    public static Dataflow dataflow() {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Dataflow.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("DataflowClient")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LaunchFlexTemplateResponse launchFlexTemplate(
            final String project,
            final String region,
            final LaunchFlexTemplateParameter parameter,
            final Boolean validateOnly) throws IOException  {

        return launchFlexTemplate(dataflow(), project, region, parameter, validateOnly);
    }

    public static LaunchFlexTemplateResponse launchFlexTemplate(
            final Dataflow dataflow,
            final String project,
            final String region,
            final LaunchFlexTemplateParameter parameter,
            final Boolean validateOnly) throws IOException {

        final LaunchFlexTemplateRequest request = new LaunchFlexTemplateRequest()
                .setLaunchParameter(parameter)
                .setValidateOnly(validateOnly);

        final LaunchFlexTemplateResponse response = dataflow
                .projects()
                .locations()
                .flexTemplates()
                .launch(project, region, request)
                .execute();
        return response;
    }

    public static Job cancel(
            final Dataflow dataflow,
            final String project,
            final String region,
            final String jobId) throws IOException {

        final Job job = dataflow
                .projects()
                .locations()
                .jobs()
                .update(project, region, jobId, new Job()
                        .setId(jobId)
                        .setRequestedState("JOB_STATE_CANCELLED"))
                .execute();
        return job;
    }

}
