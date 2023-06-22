package com.mercari.solution.util.domain.search;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.union.UnionValue;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class Neo4jUtil implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jUtil.class);

    public static final String DEFAULT_DATABASE_NAME = "neo4j";

    public static class NodeConfig implements Serializable {

        private String input;
        private List<String> labels;
        private List<String> keyFields;
        private List<String> propertyFields;

        public String getInput() {
            return input;
        }

        public List<String> getLabels() {
            return labels;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }


        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("localNeo4j.nodes[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(labels == null) {
                labels = new ArrayList<>();
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
        }
    }

    public static class RelationshipConfig implements Serializable {

        private String input;
        private String type;
        private List<String> keyFields;
        private List<String> propertyFields;
        private RelationshipNodeConfig source;
        private RelationshipNodeConfig target;

        public String getInput() {
            return input;
        }

        public String getType() {
            return type;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }

        public RelationshipNodeConfig getSource() {
            return source;
        }

        public RelationshipNodeConfig getTarget() {
            return target;
        }

        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("neo4jIndex.relationships[" + index + "].input parameter must not be null.");
            }
            if(this.type == null) {
                errorMessages.add("neo4jIndex.relationships[" + index + "].type parameter must not be null.");
            }
            if(this.source == null) {
                errorMessages.add("neo4jIndex.relationships[" + index + "].source parameter must not be null.");
            } else {
                errorMessages.addAll(this.source.validate("source", index));
            }
            if(this.target == null) {
                errorMessages.add("neo4jIndex.relationships[" + index + "].target parameter must not be null.");
            } else {
                errorMessages.addAll(this.target.validate("target", index));
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
            source.setDefaults();
            target.setDefaults();
        }
    }

    public static class RelationshipNodeConfig implements Serializable {

        private String label;
        private List<String> keyFields;
        private List<String> propertyFields;

        public String getLabel() {
            return label;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getLabels() {
            return Arrays.asList(label);
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }

        public List<String> validate(final String type, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.label == null) {
                errorMessages.add("neo4jIndex.relationships[" + index + "]." + type + ".label parameter must not be null.");
            }
            if(this.keyFields == null || keyFields.size() == 0) {
                errorMessages.add("neo4jIndex.relationships[" + index + "]." + type + ".keyFields parameter must not be null and must not be size zero.");
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
        }

    }

    public static void query(final HttpClient client, final String username, final String password, final String endpoint, final String database, final String cypher) {
        final String url = String.format("%s/db/%s/tx/commit", endpoint, database);

        final JsonArray statements = new JsonArray();
        final JsonObject statement = new JsonObject();
        statement.addProperty("statement", cypher);
        statements.add(statement);

        final JsonObject body = new JsonObject();
        body.add("statements", statements);

        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)))
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to getEndpointID: " + url, e);
        }
    }

    public static void index(final GraphDatabaseService graphDB,
                             final List<UnionValue> buffer,
                             final List<NodeConfig> nodes,
                             final List<RelationshipConfig> relationships,
                             final List<String> inputNames) {

        if(buffer == null || buffer.size() == 0) {
            LOG.info("no update index");
            return;
        }

        int countNode = 0;
        try(final Transaction tx = graphDB.beginTx()) {
            for(final UnionValue unionValue : buffer) {
                final int index = unionValue.getIndex();
                if(inputNames.size() <= index) {
                    throw new IllegalStateException("UnionValue index: " + index + " is over inputs size: " + inputNames.size());
                }
                final String sourceInputName = inputNames.get(index);

                for(final Neo4jUtil.NodeConfig nodeConfig : nodes) {
                    if(!nodeConfig.getInput().equals(sourceInputName)) {
                        continue;
                    }

                    final Node node = getNode(tx, nodeConfig.getLabels(), nodeConfig.getKeyFields(), unionValue);
                    final Map<String, Object> properties = unionValue.getMap(nodeConfig.getPropertyFields());
                    for(final Map.Entry<String, Object> property : properties.entrySet()) {
                        node.setProperty(property.getKey(), property.getValue());
                    }
                    countNode += 1;
                }
            }
            tx.commit();
        }

        // update relationships
        int countRelationship = 0;
        try(final Transaction tx = graphDB.beginTx()) {
            for(final UnionValue unionValue : buffer) {
                final int index = unionValue.getIndex();
                if(inputNames.size() <= index) {
                    throw new IllegalStateException("UnionValue index: " + index + " is over inputs size: " + inputNames.size());
                }
                final String sourceInputName = inputNames.get(index);

                for(final Neo4jUtil.RelationshipConfig relationshipConfig : relationships) {
                    if(!relationshipConfig.getInput().equals(sourceInputName)) {
                        continue;
                    }

                    final RelationshipNodeConfig sourceConfig = relationshipConfig.getSource();
                    final Node source = getNode(tx, sourceConfig.getLabels(), sourceConfig.getKeyFields(), unionValue);
                    if(sourceConfig.getPropertyFields().size() > 0) {
                        Map<String, Object> properties = unionValue.getMap(sourceConfig.getPropertyFields());
                        for(final Map.Entry<String, Object> property : properties.entrySet()) {
                            source.setProperty(property.getKey(), property.getValue());
                        }
                    }

                    final RelationshipNodeConfig targetConfig = relationshipConfig.getTarget();
                    final Node target = getNode(tx, targetConfig.getLabels(), targetConfig.getKeyFields(), unionValue);
                    if(targetConfig.getPropertyFields().size() > 0) {
                        Map<String, Object> properties = unionValue.getMap(targetConfig.getPropertyFields());
                        for(final Map.Entry<String, Object> property : properties.entrySet()) {
                            target.setProperty(property.getKey(), property.getValue());
                        }
                    }

                    final Relationship relationship = getRelationship(tx, source, target,
                            relationshipConfig.getType(), relationshipConfig.getKeyFields(), unionValue);
                    final Map<String, Object> properties = unionValue.getMap(relationshipConfig.getPropertyFields());
                    for(final Map.Entry<String, Object> property : properties.entrySet()) {
                        relationship.setProperty(property.getKey(), property.getValue());
                    }
                    countRelationship += 1;
                }
            }
            tx.commit();
        }
        LOG.info("update node: " + countNode + ", relationship: " + countRelationship);
    }

    public static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

    private static Node getNode(final Transaction tx, final List<String> labelNames, final List<String> keyFields, final UnionValue unionValue) {
        final Label[] labels = labelNames
                .stream()
                .map(Label::label)
                .toArray(Label[]::new);
        final Map<String, Object> keyProperties = unionValue.getMap(keyFields);
        try(final ResourceIterator<Node> nodes = tx.findNodes(labels[0], keyProperties)) {
            if(nodes.hasNext()) {
                return nodes.next();
            } else {
                final Node node = tx.createNode(labels);
                for(final Map.Entry<String, Object> property : keyProperties.entrySet()) {
                    node.setProperty(property.getKey(), property.getValue());
                }
                return node;
            }
        }
    }

    private static Relationship getRelationship(final Transaction tx, final Node source, final Node target, final String type, final List<String> keyFields, final UnionValue unionValue) {
        final RelationshipType relationshipType = RelationshipType.withName(type);
        final Map<String, Object> keyProperties = unionValue.getMap(keyFields);
        if(keyFields != null && !keyFields.isEmpty()) {
            try(final ResourceIterator<Relationship> relationships = tx.findRelationships(relationshipType, keyProperties)) {
                if(relationships.hasNext()) {
                    return relationships.next();
                }
            }
        }
        final Relationship relationship = source.createRelationshipTo(target, relationshipType);
        for(final Map.Entry<String, Object> property : keyProperties.entrySet()) {
            relationship.setProperty(property.getKey(), property.getValue());
        }
        return relationship;
    }

}
