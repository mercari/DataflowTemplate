# Deploy Template

Mercari Dataflow Template is used as FlexTemplate.
Therefore, the Mercari Dataflow Template should be deployed according to the FlexTemplate creation steps.

## Requirements

* Java 17
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

## Push Template Container Image to Cloud Container Registry.

The first step is to build the source code and register it as a container image in the [Cloud Artifact Registry](https://cloud.google.com/artifact-registry).

To upload container images to the Artifact registry via Docker commands, you will first need to execute the following commands, depending on the repository region.

```sh
gcloud auth configure-docker us-central1-docker.pkg.dev, asia-northeast1-docker.pkg.dev
```

The following command will generate a container for FlexTemplate from the source code and upload it to Container Registry.

```sh
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/cloud
```

## Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/cloud" \
  --sdk-language "JAVA"
```
