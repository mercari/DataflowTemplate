package com.mercari.solution.util.gcp;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.regex.Pattern;

public class SecretManagerUtil {

    private static final Pattern PATTERN_SECRET_NAME = Pattern
            .compile("(projects/)[a-zA-Z]+?[a-zA-Z0-9\\-]*/secrets/[a-zA-Z0-9_\\-]+/versions/((latest)|([0-9]+))");

    public static SecretManagerServiceClient createClient() {
        try {
            return SecretManagerServiceClient.create(SecretManagerServiceSettings.newBuilder()
                    .setCredentialsProvider(SecretManagerServiceSettings.defaultCredentialsProviderBuilder().build())
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteString getSecret(final String project, final String secret, final String version) {
        try(final SecretManagerServiceClient client = SecretManagerServiceClient.create(SecretManagerServiceSettings.newBuilder()
                .setCredentialsProvider(SecretManagerServiceSettings.defaultCredentialsProviderBuilder().build())
                .build())) {
            return getSecret(client, project, secret, version);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteString getSecret(final String secretName) {
        try(final SecretManagerServiceClient client = SecretManagerServiceClient.create(SecretManagerServiceSettings.newBuilder()
                .setCredentialsProvider(SecretManagerServiceSettings.defaultCredentialsProviderBuilder().build())
                .build())) {
            return getSecret(client, secretName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteString getSecret(final SecretManagerServiceClient client, final String project, final String secret, final String version) {
        final SecretVersionName versionName = SecretVersionName.of(project, secret, version);
        final AccessSecretVersionResponse resp = client.accessSecretVersion(versionName);
        return resp.getPayload().getData();
    }

    public static ByteString getSecret(final SecretManagerServiceClient client, final String secretName) {
        final AccessSecretVersionResponse resp = client.accessSecretVersion(secretName);
        return resp.getPayload().getData();
    }

    public static boolean isSecretName(final String secretName) {
        if(secretName == null) {
            return false;
        }
        if(!secretName.startsWith("projects/")) {
            return false;
        }
        return PATTERN_SECRET_NAME.matcher(secretName).find();
    }

}