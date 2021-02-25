package com.mercari.solution.util.gcp;

import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtRequest;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.google.gson.JsonObject;
import org.joda.time.DateTime;

import java.io.IOException;

public class IAMUtil {

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
