package com.mercari.solution.util.gcp;

import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtRequest;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.google.gson.JsonObject;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class IAMUtil {

    private static final String ENDPOINT_METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=";

    public static String signJwt(final String serviceAccount, final int expiration) {
        final long exp = DateTime.now().plusSeconds(expiration).getMillis() / 1000;
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("sub", serviceAccount);
        jsonObject.addProperty("exp", exp);
        try(final IamCredentialsClient client = IamCredentialsClient.create(IamCredentialsSettings
                .newBuilder().build())) {
            try {
                final SignJwtResponse res = client.signJwt(SignJwtRequest.newBuilder()
                        .setName(serviceAccount)
                        .setPayload(jsonObject.toString())
                        .build());
                return res.getSignedJwt();
            } catch (PermissionDeniedException e) {
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getIdToken(final HttpClient client, final String endpoint)
            throws IOException, URISyntaxException, InterruptedException {
        final String metaserver = ENDPOINT_METADATA + endpoint;
        final HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(metaserver))
                .header("Metadata-Flavor", "Google")
                .GET()
                .build();

        final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return res.body();
    }

    public static AccessToken getAccessToken() throws IOException {
        final GoogleCredentials credentials = GoogleCredentials
                .getApplicationDefault()
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
        return credentials.refreshAccessToken();
    }

    // TODO Pending as it was not available in the REST API. To be investigated at a later date.
    /*
    public static String signJwt(final String serviceAccount) throws IOException {
        final long exp = DateTime.now().plusSeconds(60).getMillis() / 1000;
        final JsonObject jsonObject = new JsonObject();
        if(serviceAccount.startsWith("projects")) {
            var strs = serviceAccount.split("/");
            jsonObject.addProperty("sub", strs[strs.length - 1]);
        } else {
            jsonObject.addProperty("sub", serviceAccount);
        }
        jsonObject.addProperty("exp", exp);

        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = new JacksonFactory();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            var iam = new IAMCredentials.Builder(transport, jsonFactory, initializer).build();
            var jwt = iam.projects().serviceAccounts()
                    .signJwt(serviceAccount, new SignJwtRequest().setPayload(jsonObject.toString()))
                    .execute();
            return jwt.getSignedJwt();
        } catch (Exception e) {
            throw e;
        }
    }
    */

}
