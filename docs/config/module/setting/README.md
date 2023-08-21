# Define pipeline settings

settings is a module that defines common settings for all modules.
The following items can be defined as settings.

| parameter | type                            | description                                                       |
|-----------|---------------------------------|-------------------------------------------------------------------|
| streaming | Boolean                         | Specify whether the dataflow job starts in streaming mode or not. |
| dataflow  | [DataflowSettings](dataflow.md) | Specify Cloud Dataflow specific parameters.                       |
| beamsql   | [BeamSQLSettings](beamsql.md)   | Specify Beam SQL common settings.                                 |


#### Example

```JSON:settings
{
  "settings": {
    "streaming": false,
    "dataflow": {
      "autoscalingAlgorithm": "NONE",
      "workerMachineType": "n2-custom-2-131072-ext",
      "numWorkers": 1,
      "diskSizeGb": 256,
      "workerDiskType": "compute.googleapis.com/projects//zones//diskTypes/pd-ssd"
    },
    "beamsql": {
      "plannerName": "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner"
    }
  },
  "sources": [],
  "transforms": [],
  "sinks": []   
}
```
