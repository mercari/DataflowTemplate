# JavaScript Transform Module

JavaScript transform module processes input records with JavaScript functions.
In the function, it is possible to refer to the values of past records and processed values as states.

## Transform module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `javascript` |
| inputs | required | Array<String\> | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters. |

## JavaScript transform module parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| script | required | String | Specify the JS text or the GCS path where the JS file was uploaded. |
| mappings | required | Array<Mapping\> | Specify the mapping between the JS function, the data type of the result, and the field name that holds the result. |
| stateUpdateFunction | optional | String | If you use state, specify the name of the function that updates the state. |
| groupFields | optional | Array<String\> | When using states, and you want to use states separately for each group, specify the name of the field used to divide the records into groups. |
| failFast | optional | Boolean | When JS processing fails, specify whether to fail the job or output the failed records to another destination and continue processing. Default is `true`.|

* State is available as a Map<String,Double> type.
* If `stateUpdateFunction` is specified, parallelism will drop for each value of the field specified in groupFields, so be careful to use it only if you need the state.

## Mapping parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| function | required | String | Specify the function name to be used for processing in JS defined in `script`. |
| outputField | required | String | Specify the field name where you want the result of JS processing to be retained.|
| outputType | required | String | Specify the data type of the field where you want to retain the result of processing by JS. |


## Supported outputType

| type | description |
| --- | --- |
| bool | `true` or `false` |
| string | text |
| bytes | byte array |
| int32 | int 32bit |
| int64 | int 64bit |
| float32 | float 32bit |
| float64 | float 64bit |
| date | date |
| timestamp | timestamp |

## Signatures of JavaScript functions

The JavaScript code specified by `script` defines functions with the following signature.

```js
// If stateUpdateFunction is not set,
// only the input data will be passed as a Map type argument.

function funcWithoutState(input) {
   return input.doubleValue * 10;
}


// If stateUpdateFunction is set, 
// the first argument of the function will be the input data of type Map,
// the second argument will be the state variable of type Map<String,Double>.

function funcWithState(input, states) {
   var prev = states.getOrDefault("prevFloatValue", 0);
   return prev + input.floatValue;
}


// In the function specified by stateUpdateFunction,
// the first argument is the input data with the processing result in the Map type.
// the second argument is the previous state passed in the Map<String,Double> type.

function stateUpdateFunc(input, states) {
    // In the stateUpdateFunction,
    // add the value you want to pass in the next process to the Map
    // and return it. (The value of the Map must be numeric type)
    
    states.put("prevFloatValue", input.floatValue);
    states.put("prevDoubleValue", input.doubleValue);
    return states;
}
```