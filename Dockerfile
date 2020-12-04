FROM openjdk:8
COPY target/solution-bundled-0.1.jar .
ENV GOOGLE_APPLICATION_CREDENTIALS /mnt/gcloud/application_default_credentials.json
ENTRYPOINT ["java", "-cp", "solution-bundled-0.1.jar", "-Xmx4096m", "com.mercari.solution.FlexPipeline", "--runner=DirectRunner"]