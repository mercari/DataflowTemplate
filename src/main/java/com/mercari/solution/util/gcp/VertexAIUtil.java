package com.mercari.solution.util.gcp;

import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;


public class VertexAIUtil {

    private static final Logger LOG = LoggerFactory.getLogger(VertexAIUtil.class);

    private static final String URI_VERTEXAI = "https://%s-aiplatform.googleapis.com/v1/%s";


    public static String getEndpointID(final HttpClient client, final String token,
                                       final String project, final String region, final String endpointName) {

        final String endpoint = String.format("projects/%s/locations/%s/endpoints?filter=display_name=%s", project, region, endpointName);
        final String url = String.format(URI_VERTEXAI, region, endpoint);

        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .GET()
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (!responseJson.has("endpoints")) {
                throw new IllegalStateException("");
            }

            final String name = responseJson.getAsJsonObject("endpoints").getAsJsonPrimitive("name").getAsString();
            final String[] elements = name.split("/");
            if (elements.length < 6) {
                throw new IllegalStateException("");
            }
            return elements[5];
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to getEndpointID: " + url, e);
        }
    }

    public static String deploy(final HttpClient client, final String token,
                                final String endpoint, final String model,
                                final String machineType, final Integer minReplicaCount, final int maxReplicaCount) {

        final JsonObject machineSpec = new JsonObject();
        machineSpec.addProperty("machineType", machineType);

        final JsonObject dedicatedResources = new JsonObject();
        dedicatedResources.add("machineSpec", machineSpec);
        dedicatedResources.addProperty("minReplicaCount", minReplicaCount);
        dedicatedResources.addProperty("maxReplicaCount", maxReplicaCount);

        final JsonObject deployedModel = new JsonObject();
        deployedModel.addProperty("model", model);
        deployedModel.addProperty("displayName", model);
        deployedModel.add("dedicatedResources", dedicatedResources);

        final JsonObject trafficSplit = new JsonObject();
        trafficSplit.addProperty("0", 100);

        final JsonObject body = new JsonObject();
        body.add("deployedModel", deployedModel);
        body.add("trafficSplit", trafficSplit);

        final String region = getEndpointRegion(endpoint);
        final String url = String.format(URI_VERTEXAI, region, endpoint) + ":deployModel";
        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final String responseString = res.body();

            final JsonElement responseElement;
            try {
                responseElement = new Gson().fromJson(responseString, JsonElement.class);
            } catch (JsonSyntaxException e) {
                LOG.error("Failed to parse deploy response: " + responseString);
                throw new IllegalStateException("Failed to parse deploy response: " + responseString + ", url: " + url, e);
            }

            if(!responseElement.isJsonObject()) {
                throw new IllegalStateException("Vertex AI endpoints deploy model request failed: " + responseElement);
            }
            final JsonObject responseJson = responseElement.getAsJsonObject();
            if (!responseJson.has("name")) {
                throw new IllegalStateException("Vertex AI endpoints deploy model request failed: " + responseJson);
            }
            return responseJson.getAsJsonPrimitive("name").getAsString();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to deploy: " + url, e);
        }
    }

    public static String undeploy(final HttpClient client, final String token,
                                  final String endpoint,
                                  final String deployedModelId) {

        final JsonObject body = new JsonObject();
        body.addProperty("deployedModelId", deployedModelId);

        final String region = getEndpointRegion(endpoint);
        final String url = String.format(URI_VERTEXAI, region, endpoint) + ":undeployModel";
        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .method("POST", HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (!responseJson.has("name")) {
                throw new IllegalStateException("Illegal undeploy response: " + responseJson);
            }
            return responseJson.getAsJsonPrimitive("name").getAsString();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to undeploy: " + url, e);
        }
    }

    public static List<String> getDeployedModelIds(final HttpClient client, final String token, final String endpoint) {


        final String region = getEndpointRegion(endpoint);
        final String url = String.format(URI_VERTEXAI, region, endpoint);

        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .GET()
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (!responseJson.has("deployedModels")) {
                throw new IllegalStateException("Endpoints get request failed: " + responseJson);
            }

            final List<String> deployedModelIds = new ArrayList<>();
            final JsonArray models = responseJson.getAsJsonArray("deployedModels");
            for(final JsonElement model : models) {
                if(!model.isJsonObject() || !model.getAsJsonObject().has("id")) {
                    continue;
                }
                deployedModelIds.add(model.getAsJsonObject().getAsJsonPrimitive("id").getAsString());
            }
            return deployedModelIds;
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to getDeployModelIds: " + url, e);
        }
    }

    public static boolean isOperationDone(final HttpClient client, final String token,
                                          final String region, final String operation) {

        final String url = String.format(URI_VERTEXAI, region, operation);

        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .GET()
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final String responseString = res.body();

            final JsonElement responseElement;
            try {
                responseElement = new Gson().fromJson(responseString, JsonElement.class);
            } catch (JsonSyntaxException e) {
                LOG.error("Failed to get Vertex AI operation response: " + responseString + ", url: " + url);
                throw new IllegalStateException("Failed to get Vertex AI operation response: " + responseString + ", url: " + url, e);
            }
            if(!responseElement.isJsonObject()) {
                throw new IllegalStateException("Failed to parse Vertex AI get operation: " + responseElement);
            }

            final JsonObject responseJson = responseElement.getAsJsonObject();
            if (!responseJson.has("name")) {
                throw new IllegalStateException("Failed to get operation: " + responseJson + ", for request: " + url);
            }
            if (responseJson.has("error")) {
                throw new IllegalStateException("Failed operation: " + responseJson + ", for request: " + url);
            }
            if (!responseJson.has("done")) {
                return false;
            }
            return responseJson.getAsJsonPrimitive("done").getAsBoolean();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to isOperationDone: " + url, e);
        }
    }

    public static JsonObject predict(final HttpClient client, final String token,
                                     final String endpoint,
                                     final JsonArray instances) {

        final JsonObject body = new JsonObject();
        body.add("instances", instances);

        final String region = getEndpointRegion(endpoint);
        final String url = String.format(URI_VERTEXAI, region, endpoint) + ":predict";
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .method("POST", HttpRequest.BodyPublishers.ofString(body.toString()));

            final HttpRequest req = builder.header("Authorization", "Bearer " + token).build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (!responseJson.has("predictions")) {
                if (!responseJson.has("error")) {
                    throw new RuntimeException("Illegal AutoML response: " + responseJson + ", no error and predictions. request body: " + body);
                }
                final JsonObject errorJson = responseJson.getAsJsonObject("error");
                if (errorJson.has("code")) {
                    int code = errorJson.getAsJsonPrimitive("code").getAsInt();
                    if (code >= 400 && code < 500) {
                        throw new IllegalStateException("Illegal response: " + responseJson + ", request body: " + body);
                    }
                }
                throw new RuntimeException("Illegal response: " + responseJson + ", request body: " + body);
            }

            final JsonArray predictions = responseJson.getAsJsonArray("predictions");
            if (instances.size() != predictions.size()) {
                throw new RuntimeException("AutoML request size: " + instances.size() + " does not match to response size: " + predictions.size());
            }
            return responseJson;
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to predict: " + url, e);
        }
    }

    public static String getEndpointRegion(final String endpoint) {
        //projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT_ID
        if(endpoint == null) {
            return null;
        }
        final String[] elements = endpoint.split("/");
        if(elements.length < 5) {
            return null;
        }
        return elements[3];
    }

}
