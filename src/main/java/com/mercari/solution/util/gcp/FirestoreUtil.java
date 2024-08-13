package com.mercari.solution.util.gcp;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreFactory;
import com.google.cloud.firestore.FirestoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FirestoreUtil {

    public static final String DEFAULT_DATABASE_NAME = "(default)";
    public static final String NAME_FIELD = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreUtil.class);

    public static Firestore getFirestore(final String projectId, final String databaseId) {
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final FirestoreFactory factory = new FirestoreOptions.DefaultFirestoreFactory();
            final FirestoreOptions options = FirestoreOptions.newBuilder()
                    .setProjectId(projectId)
                    .setDatabaseId(databaseId)
                    .setCredentials(credential)
                    .build();
            return factory.create(options);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String createName(final String project, final String database, final String collection, final String id) {
        final String path = String.format("%s/%s", collection, id);
        return createName(project, database, path);
    }

    public static String createName(final String project, final String database, final String path) {
        return String.format("projects/%s/databases/%s/documents/%s", project, database, path);
    }

    public static String createDatabaseRootName(final String project) {
        return createDatabaseRootName(project, DEFAULT_DATABASE_NAME);
    }

    public static String createDatabaseRootName(final String project, final String database) {
        return String.format("projects/%s/databases/%s", project, database);
    }

}
