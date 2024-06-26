package com.mercari.solution.util.gcp;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.*;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

public class PubSubUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubUtil.class);

    private static final Pattern PATTERN_SUBSCRIPTION = Pattern.compile("^projects\\/[a-zA-Z0-9_-]+\\/subscriptions\\/[a-zA-Z0-9_-]+$");

    public static Pubsub pubsub() {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
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

    public static Schema getSchemaFromTopic(final String topic) {
        return getSchemaFromTopic(pubsub(), topic);
    }

    public static Schema getSchemaFromTopic(final Pubsub pubsub, final String topicResource) {
        try {
            final Topic topic = pubsub.projects().topics().get(topicResource).execute();
            final String schemaResource = topic.getSchemaSettings().getSchema();
            final com.google.api.services.pubsub.model.Schema topicSchema = pubsub.projects().schemas().get(schemaResource).execute();
            if(!"AVRO".equals(topicSchema.getType())) {
                throw new IllegalArgumentException();
            }
            return AvroSchemaUtil.convertSchema(topicSchema.getDefinition());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get schema for topic: " + topicResource, e);
        }
    }

    public static Schema getSchemaFromSubscription(final String subscriptionResource) {
        final Pubsub pubsub = pubsub();
        return getSchemaFromSubscription(pubsub, subscriptionResource);
    }

    public static Schema getSchemaFromSubscription(final Pubsub pubsub, final String subscriptionResource) {
        try {
            final Subscription subscription = pubsub.projects().subscriptions().get(subscriptionResource).execute();
            final String topicResource = subscription.getTopic();
            return getSchemaFromTopic(pubsub, topicResource);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get schema for subscription: " + subscriptionResource, e);
        }
    }

    public static String getDeadLetterTopic(String subscriptionResource) {
        return getDeadLetterTopic(pubsub(), subscriptionResource);
    }

    public static String getDeadLetterTopic(final Pubsub pubsub, String subscriptionResource) {
        try {
            final Subscription subscription = pubsub
                    .projects()
                    .subscriptions()
                    .get(subscriptionResource)
                    .execute();
            if (subscription.getDeadLetterPolicy() == null) {
                return null;
            }
            return subscription.getDeadLetterPolicy().getDeadLetterTopic();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                return null;
            } else if(e.getStatusCode() == 403) {
                LOG.warn("dataflow worker does not have dead-letter topic access permission for subscription: " + subscriptionResource);
                return null;
            }
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isSubscriptionResource(final String name) {
        return PATTERN_SUBSCRIPTION.matcher(name).find();
    }

    public static List<String> publish(
            final String topic,
            final List<PubsubMessage> messages) throws IOException {

        return publish(pubsub(), topic, messages);
    }

    public static List<String> publish(
            final Pubsub pubsub,
            final String topic,
            final List<PubsubMessage> messages) throws IOException {

        final PublishRequest request = new PublishRequest().setMessages(messages);
        final PublishResponse response =pubsub.projects().topics().publish(topic, request).execute();
        return response.getMessageIds();
    }

    public static List<ReceivedMessage> pull(
            final Pubsub pubsub,
            final String subscription,
            final int maxMessages,
            final boolean ack) throws IOException {

        final PullResponse response = pubsub
                .projects()
                .subscriptions()
                .pull(subscription, new PullRequest().setMaxMessages(maxMessages))
                .execute();

        if(ack && !response.isEmpty()) {
            final List<String> ackIds = response.getReceivedMessages()
                    .stream()
                    .map(ReceivedMessage::getAckId)
                    .toList();
            final Empty empty = pubsub
                    .projects()
                    .subscriptions()
                    .acknowledge(subscription, new AcknowledgeRequest().setAckIds(ackIds))
                    .execute();
        }

        return response.getReceivedMessages();
    }

    public static String getTextMessage(final String subscription) throws IOException {
        return getTextMessage(pubsub(), subscription);
    }

    public static String getTextMessage(
            final Pubsub pubsub,
            final String subscription) throws IOException {

        final PullResponse response = pubsub
                .projects()
                .subscriptions()
                .pull(subscription, new PullRequest()
                        .setMaxMessages(1))
                .execute();
        if(response.isEmpty() && (response.getReceivedMessages() == null || response.getReceivedMessages().isEmpty())) {
            LOG.info("response is empty: " + response.getReceivedMessages());
            return null;
        } else if(response.getReceivedMessages().size() > 1) {
            LOG.info("response is over zero: " + response.getReceivedMessages().size());
        }

        final ReceivedMessage receivedMessage = response.getReceivedMessages().get(0);
        final Empty empty = pubsub.projects().subscriptions()
                .acknowledge(subscription, new AcknowledgeRequest()
                        .setAckIds(List.of(receivedMessage.getAckId())))
                .execute();
        final String data = receivedMessage.getMessage().getData();
        return convertReceivedMessageData(data);
    }

    public static String convertPubsubMessageData(final String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    public static String convertReceivedMessageData(final String data) {
        return new String(Base64.getDecoder().decode(data), StandardCharsets.UTF_8);
    }

}
