# WebSocket Source Module (Experimental)

WebSocket source module for receiving realtime messages from specified endpoint.

## Source module common parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| name | required | String | Step name. specified to be unique in config file. |
| module | required | String | Specified `websocket` |
| schema | required | [Schema](SCHEMA.md) | Schema of the data to be read. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters |

## WebSocket source module parameters

| parameter                | optional | type    | description                                                                                                                                            |
|--------------------------| --- |---------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| endpoint                 | required | String  | Specify the WebSocket endpoint, which must start with `wss://` or `ws://`                                                                              |
| requests                 | optional | Json    | Specify the request Json to be sent when the WebSocket connection is opened. To send multiple requests, specify multiple Json with the JsonArray type. |
| intervalSeconds          | optional | Integer | Specify the interval in seconds at which data received by WebSocket will be sent. The default is 1.                                                    |
| heartbeatRequests        | optional | Json    | Specify the request Json to be sent for heartbeat.                                                                                                     |
| heartbeatIntervalSeconds | optional | Integer | Specify the interval in seconds for heartbeat request. The default is 60.                                                                              |
| requestIntervalSeconds   | optional | Integer | Specify the interval in seconds for requests. The default is 0.                                                                                        |
| eventtimeField           | optional | String  | Specify the field name when you want to get the eventtime. If specified, the field specified here will be added automatically.                         |
| receivedTimestampField   | optional | String  | Specify the field name when you want to get the received timestamp. If specified, the field specified here will be added automatically.                |
| format                   | optional | Enum    | Specify the format of the data to be received via WebSocket. Currently support only `json`. (The default is `json`)                                    |
| ignoreError              | optional | Boolean | Specify whether to ignore errors and continue processing. The default is `false`.                                                                        |
