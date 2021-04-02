# Deploy Template

Mercari Dataflow Template is used as FlexTemplate.
Therefore, the Mercari Dataflow Template should be deployed according to the FlexTemplate creation steps.

## Requirements

* Java 11
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

## Push Template Container Image to Cloud Container Registry.

The first step is to build the source code and register it as a container image in the [Cloud Container Registry](https://cloud.google.com/container-registry).

The following command will generate a container for FlexTemplate from the source code and upload it to Container Registry.

```sh
mvn clean package -DskipTests -Dimage=gcr.io/{deploy_project}/{template_repo_name}
```

## Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "gcr.io/{deploy_project}/{template_repo_name}" \
  --sdk-language "JAVA"
```
