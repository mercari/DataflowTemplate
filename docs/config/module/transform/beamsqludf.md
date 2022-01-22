# BeamSQL module build-in UDFs

`beamsql` module provides a variety of original built-in UDFs. The names of these built-in UDFs all start with `MDT_`.

## Mathematics functions

| function | return type | description |
| --- | --- | --- |
| MDT_MAX_INT64(value1 INT64, value2 INT64) | INT64 | Output the larger of the values of the first and second arguments. If one of them is NULL, output the other one. If both are NULL, output NULL. |
| MDT_MAX_FLOAT64(value1 FLOAT64, value2 FLOAT64) | FLOAT64 | The arguments and output types are FLOAT64, the function is the same as `MDT_MAX_INT64` |
| MDT_MIN_INT64(value1 INT64, value2 INT64) | INT64 | Output the smaller of the values of the first and second arguments. If one of them is NULL, output the other one. If both are NULL, output NULL. |
| MDT_MIN_FLOAT64(value1 FLOAT64, value2 FLOAT64) | FLOAT64 | The arguments and output types are FLOAT64, the function is the same as `MDT_MAX_INT64` |

## Array functions

| function | return type | description |
| --- | --- | --- |
| MDT_CONTAINS_ALL_INT64(value1 ARRAY<INT64\>, value2 ARRAY<INT64\>) | BOOLEAN | Returns true if the value set of the first argument contains all the value sets of the second argument. |
| MDT_CONTAINS_ALL_STRING(value1 ARRAY<STRING\>, value2 ARRAY<STRING\>) | BOOLEAN | The arguments type are STRING, the function is the same as `MDT_CONTAINS_ALL_INT64`. |

## Aggregation functions

| function | return type | description |
| --- | --- | --- |
| MDT_ARRAY_AGG_INT64(value INT64) | ARRAY<INT64\> | Gathers the values of the argument fields into an array. |
| MDT_ARRAY_AGG_STRING(value STRING) | ARRAY<INT64\> | The arguments type are STRING, the function is the same as `MDT_ARRAY_AGG_INT64` |

