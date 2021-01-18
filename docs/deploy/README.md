# Deploy Template

## Requirements

* Java 11
* Maven 3

## Push Template Container Image to Cloud Container Registry.

```sh
mvn clean package -DskipTests -Dimage=gcr.io/{deploy_project}/{template_repo_name}
```

## Upload template file.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "gcr.io/{deploy_project}/{template_repo_name}" \
  --sdk-language "JAVA"
```

You can write legacy format template file manually and upload gcs for REST API.

```json
{
  "docker_template_spec": {
    "docker_image": "gcr.io/{deploy_project}/{template_repo_name}"
  }
}
```