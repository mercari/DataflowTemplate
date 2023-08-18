# Dataflow Settings

Setting module for dataflow config.

Dataflow config parameters can be specified as [pipeline options](https://cloud.google.com/dataflow/docs/reference/pipeline-options).
If you wish to define these parameters in the config file, use this option.

## Dataflow setting module parameters

| parameter                    | type                | description                                                                                                                                                                                                                                                                                                                |
|------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| jobName                      | String              | The name of the Dataflow job being executed as it appears in the Dataflow jobs list and job details. Also used when updating an existing pipeline. If not set, Dataflow generates a unique name automatically.                                                                                                             |
| tempLocation                 | String              | Cloud Storage path for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://BUCKET-NAME/`                                                                                                                                                                                                              |
| stagingLocation              | String              | Cloud Storage path for staging local files. Must be a valid Cloud Storage URL, beginning with `gs://BUCKET-NAME/`. If not set, defaults to what you specified for `tempLocation`                                                                                                                                           |
| labels                       | Map<String,String\> | User-defined [labels](https://cloud.google.com/resource-manager/docs/creating-managing-labels), also known as additional-user-labels. User-specified labels are available in billing exports, which you can use for cost attribution.                                                                                      |
| autoscalingAlgorithm         | Enum                | The autoscaling mode for Dataflow job. Possible values are `THROUGHPUT_BASED` to enable autoscaling, or `NONE` to disable.                                                                                                                                                                                                 |
| flexRSGoal                   | Enum                | Specify Flexible Resource Scheduling (FlexRS) for autoscaled batch jobs. `COST_OPTIMIZED`, where the cost-optimized goal means that the Dataflow service chooses any available discounted resources. `SPEED_OPTIMIZED`, which is the same as omitting this option.                                                         |
| numWorkers                   | Integer             | The initial number of Google Compute Engine instances for the job.                                                                                                                                                                                                                                                         |
| maxNumWorkers                | Integer             | The maximum number of Google Compute Engine instances to be made available to your pipeline during execution, from 1 to 1000.                                                                                                                                                                                              |
| numberOfWorkerHarnessThreads | Integer             | The number of threads per each worker harness process                                                                                                                                                                                                                                                                      |
| workerMachineType            | Enum                | The [machine type](https://cloud.google.com/compute/docs/machine-types) to use for the job. Defaults to the value from the template if not specified.                                                                                                                                                                      |
| workerDiskType               | Enum                | The type of Persistent Disk to use, specified by a full URL of the disk type resource. For example, use `compute.googleapis.com/projects/PROJECT/zones/ZONE/diskTypes/pd-ssd` to specify an SSD Persistent Disk. When using Streaming Engine, do not specify a Persistent Disk. Supported values: `pd-ssd`, `pd-standard`. |
| diskSizeGb                   | Integer             | Worker disk size, in gigabytes.                                                                                                                                                                                                                                                                                            |
| subnetwork                   | String              | The Compute Engine subnetwork for launching Compute Engine instances to run your pipeline. Expected format is `regions/REGION/subnetworks/SUBNETWORK` or the fully qualified subnetwork name, beginning with https://..., e.g. `https://www.googleapis.com/compute/alpha/projects/PROJECT/`                                |
| usePublicIps                 | Boolean             | Specify whether Dataflow workers use external IP addresses. If the value is set to false, Dataflow workers use internal IP addresses for all communication. The default is true                                                                                                                                            |
| enableStreamingEngine        | Boolean             | Specify whether Dataflow Streaming Engine is enabled or disabled. The default is true.                                                                                                                                                                                                                                     |
| dataflowServiceOptions       | Array<String\>      | Specify additional job [modes and configurations](https://cloud.google.com/dataflow/docs/reference/service-options)                                                                                                                                                                                                        |
| experiments                  | Array<String\>      | Enables experimental or pre-GA Dataflow features                                                                                                                                                                                                                                                                           |