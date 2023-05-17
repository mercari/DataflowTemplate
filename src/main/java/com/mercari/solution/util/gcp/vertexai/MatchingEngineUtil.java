package com.mercari.solution.util.gcp.vertexai;

//import com.google.cloud.aiplatform.v1beta1.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class MatchingEngineUtil {

    public static final int PRIVATE_ENDPOINT_GRPC_PORT = 10000;

    private static final Logger LOG = LoggerFactory.getLogger(MatchingEngineUtil.class);

    private static final String DOMAIN_SERVICE_ENDPOINT = "%s-aiplatform.googleapis.com";

    private static final String NAME_INDEXES = "projects/%s/locations/%s/indexes/%s";
    private static final String NAME_INDEX_ENDPOINTS = "projects/%s/locations/%s/indexEndpoints/%s";


    public static Integer upsertDatapoints(
            final HttpClient client,
            final String token,
            final String project,
            final String region,
            final String indexId,
            final Iterable<DataPoint> dataPoints) {

        int count = 0;
        final JsonArray dataPointsJsonArray = new JsonArray();
        for(final DataPoint dataPoint : dataPoints) {
            final JsonObject dataPointJsonObject = new JsonObject();

            dataPointJsonObject.addProperty("datapoint_id", dataPoint.id);

            final JsonArray featureVector = new JsonArray();
            for(final Float value : dataPoint.embedding) {
                featureVector.add(value);
            }
            dataPointJsonObject.add("feature_vector", featureVector);

            if(dataPoint.restrictions != null && dataPoint.restrictions.size() > 0) {
                final JsonArray restrictsJsonArray = new JsonArray();
                for(Restriction restriction : dataPoint.restrictions) {
                    final JsonObject restrictJson = new JsonObject();
                    restrictJson.addProperty("namespace", restriction.namespace);

                    final JsonArray allowListJsonArray = new JsonArray();
                    restriction.allowList.forEach(allowListJsonArray::add);
                    restrictJson.add("allowList", allowListJsonArray);

                    final JsonArray denyListJsonArray = new JsonArray();
                    restriction.denyList.forEach(denyListJsonArray::add);
                    restrictJson.add("denyList", denyListJsonArray);

                    restrictsJsonArray.add(restrictJson);
                }

                dataPointJsonObject.add("restricts", restrictsJsonArray);
            }

            if(dataPoint.crowdingTag != null) {
                final JsonObject crowdingTag = new JsonObject();
                crowdingTag.addProperty("crowdingAttribute", dataPoint.crowdingTag);
                dataPointJsonObject.add("crowdingTag", crowdingTag);
            }

            dataPointsJsonArray.add(dataPointJsonObject);

            count += 1;
        }

        final JsonObject body = new JsonObject();
        body.add("datapoints", dataPointsJsonArray);

        final String serviceEndpointDomain = String.format(DOMAIN_SERVICE_ENDPOINT, region);
        final String indexName = String.format(NAME_INDEXES, project, region, indexId);
        final String url = String.format("https://%s/v1/%s:%s", serviceEndpointDomain, indexName, "upsertDatapoints");
        final JsonObject responseJson = sendPostRequest(client, url, token, body);
        return count;
    }

    public static Integer removeDatapoints(
            final HttpClient client,
            final String token,
            final String project,
            final String region,
            final String indexId,
            final List<String> dataPointIds) {

        final JsonObject body = new JsonObject();
        final JsonArray dataPointIdsArray = new JsonArray();
        dataPointIds.forEach(dataPointIdsArray::add);
        body.add("datapointIds", dataPointIdsArray);

        final String serviceEndpointDomain = String.format(DOMAIN_SERVICE_ENDPOINT, region);
        final String indexName = String.format(NAME_INDEXES, project, region, indexId);
        final String url = String.format("https://%s/v1/%s:%s", serviceEndpointDomain, indexName, "removeDatapoints");
        final JsonObject responseJson = sendPostRequest(client, url, token, body);
        return dataPointIds.size();
    }

    public static JsonObject search(
            final HttpClient client,
            final String token,
            final String publicIndexEndpointDomainName,
            final String project,
            final String region,
            final String indexEndpointId,
            final String deployedIndexId,
            final JsonArray queries) {

        final JsonObject body = new JsonObject();
        body.addProperty("deployed_index_id", deployedIndexId);

        body.add("queries", queries);
        body.addProperty("return_full_datapoint", true);

        final String indexEndpointName = String.format(NAME_INDEX_ENDPOINTS, project, region, indexEndpointId);
        final String url = String.format("https://%s/v1beta1/%s:%s", publicIndexEndpointDomainName, indexEndpointName, "findNeighbors");
        final JsonObject responseJson = sendPostRequest(client, url, token, body);
        return responseJson;
    }

    /*
    public static FindNeighborsResponse search(
            final MatchServiceClient client,
            final String project,
            final String region,
            final String indexEndpointId,
            final String deployedIndexId,
            final JsonArray queries) {

        final List<FindNeighborsRequest.Query> requestQueries = new ArrayList<>();
        for(final JsonElement query : queries) {
            if(!query.isJsonObject()) {
                continue;
            }
            final JsonObject datapointObject = query.getAsJsonObject().getAsJsonObject("datapoint");
            final String datapointId = datapointObject.get("datapoint_id").getAsString();
            final JsonArray featureVector = datapointObject.getAsJsonArray("feature_vector");
            final List<Float> floats = new ArrayList<>();
            for(final JsonElement feature : featureVector) {
                floats.add(feature.getAsFloat());
            }
            final IndexDatapoint datapoint = IndexDatapoint.newBuilder()
                    .setDatapointId(datapointId)
                    .addAllFeatureVector(floats)
                    .build();
            final FindNeighborsRequest.Query requestQuery = FindNeighborsRequest.Query.newBuilder()
                    .setApproximateNeighborCount(10)
                    .setDatapoint(datapoint)
                    .build();
            requestQueries.add(requestQuery);
        }

        final FindNeighborsRequest req = FindNeighborsRequest.newBuilder()
                .addAllQueries(requestQueries)
                .setDeployedIndexId(deployedIndexId)
                .setIndexEndpoint(IndexEndpointName.of(project, region, indexEndpointId).toString())
                .build();
        final FindNeighborsResponse res = client.findNeighbors(req);
        return res;
    }
     */

    public static JsonObject getIndexEndpoint(
            final HttpClient client,
            final String token,
            final String project, final String region, final String indexEndpointId) {

        final String serviceEndpointDomain = String.format(DOMAIN_SERVICE_ENDPOINT, region);
        final String indexEndpointsName = String.format(NAME_INDEX_ENDPOINTS, project, region, indexEndpointId);
        final String url = String.format("https://%s/v1/%s", serviceEndpointDomain, indexEndpointsName);
        final JsonObject response = sendGetRequest(client, url, token);
        return response;
    }

    public static String getPublicIndexEndpointDomainName(
            final HttpClient client,
            final String token,
            final String project, final String region, final String indexEndpointId) {

        final JsonObject response = getIndexEndpoint(client, token, project, region, indexEndpointId);
        System.out.println(response);
        if(response.has("publicEndpointDomainName")) {
            return response.getAsJsonPrimitive("publicEndpointDomainName").getAsString();
        } else {
            return null;
        }
    }

    public static String getPrivateEndpointGrpcAddress(
            final HttpClient client,
            final String token,
            final String project,
            final String region,
            final String indexEndpointId,
            final String deployedIndexId) {

        final JsonObject response = getIndexEndpoint(client, token, project, region, indexEndpointId);
        System.out.println(response);
        if(!response.has("deployedIndexes") || !response.get("deployedIndexes").isJsonArray()) {
            return null;
        }
        final JsonArray deployedIndexes = response.getAsJsonArray("deployedIndexes");
        for(final JsonElement deployedIndex : deployedIndexes) {
            final JsonObject object = deployedIndex.getAsJsonObject();
            if(!deployedIndexId.equals(object.get("id").getAsString())) {
                continue;
            }
            final JsonObject privateEndpoints = object.getAsJsonObject("privateEndpoints");
            return privateEndpoints.get("matchGrpcAddress").getAsString();
        }
        return null;
    }

    private static JsonObject sendGetRequest(final HttpClient client, final String url, final String token) {
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json");

            final HttpRequest req = builder.header("Authorization", "Bearer " + token).build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to send request to url: " + url + ", code: " + res.statusCode() + ", body: " + res.body());
            }
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (responseJson.has("error")) {
                throw new RuntimeException("Illegal Matching Engine index upsert response error: " + responseJson);
            }
            return responseJson;
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to predict: " + url, e);
        }
    }

    private static JsonObject sendPostRequest(final HttpClient client, final String url, final String token, final JsonObject body) {
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .method("POST", HttpRequest.BodyPublishers.ofString(body.toString()));

            final HttpRequest req = builder.header("Authorization", "Bearer " + token).build();
            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if(res.statusCode() >= 400) {
                throw new RuntimeException("Failed to send request to url: " + url + ", code: " + res.statusCode() + ", body: " + res.body() + ", request: " + body);
            }
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
            if (responseJson.has("error")) {
                throw new RuntimeException("Matching Engine request error: " + responseJson + ", request body: " + body);
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

    public static class DataPoint implements Serializable {

        private String id;
        private List<Float> embedding;
        private List<Restriction> restrictions;
        private String crowdingTag;

        public String getId() {
            return id;
        }

        public List<Float> getEmbedding() {
            return embedding;
        }

        public List<Restriction> getRestrictions() {
            return restrictions;
        }

        public String getCrowdingTag() {
            return crowdingTag;
        }

        public static DataPoint of(final String id) {
            final DataPoint dataPoint = new DataPoint();
            dataPoint.id = id;
            return dataPoint;
        }

        public static DataPoint of(
                final String id,
                final List<Float> embedding,
                final String crowdingTag,
                final Map<String, List<String>> restrictAllowValues,
                final Map<String, List<String>> restrictDenyValues) {

            final DataPoint dataPoint = new DataPoint();
            dataPoint.id = id;
            dataPoint.embedding = embedding;
            dataPoint.crowdingTag = crowdingTag;

            dataPoint.restrictions = new ArrayList<>();
            final Set<String> namespaces = new HashSet<>();
            namespaces.addAll(restrictAllowValues.keySet());
            namespaces.addAll(restrictDenyValues.keySet());
            for(final String namespace : namespaces) {
                final List<String> allowList = restrictAllowValues.get(namespace);
                final List<String> denyList = restrictDenyValues.get(namespace);
                final Restriction restriction = Restriction.of(namespace, allowList, denyList);
                dataPoint.restrictions.add(restriction);
            }

            return dataPoint;
        }

    }

    public static class Restriction implements Serializable {

        private String namespace;
        private List<String> allowList;
        private List<String> denyList;

        public String getNamespace() {
            return namespace;
        }

        public List<String> getAllowList() {
            return allowList;
        }

        public List<String> getDenyList() {
            return denyList;
        }

        public static Restriction of(String namespace, List<String> allowList, List<String> denyList) {
            final Restriction restriction = new Restriction();
            restriction.namespace = namespace;
            restriction.allowList = Optional.ofNullable(allowList).orElseGet(ArrayList::new);
            restriction.denyList = Optional.ofNullable(denyList).orElseGet(ArrayList::new);
            return restriction;
        }

    }
}
