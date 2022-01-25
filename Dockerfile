FROM openjdk:11
COPY target/solution-bundled-0.6.0.jar solution-bundled.jar
ENV GOOGLE_APPLICATION_CREDENTIALS /mnt/gcloud/application_default_credentials.json
ENTRYPOINT ["java", "-cp", "solution-bundled.jar", "-Xmx4096m", "com.mercari.solution.FlexPipeline", "--runner=DirectRunner"]