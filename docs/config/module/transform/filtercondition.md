# Filter condition

The filter condition of a record specifies three things: the field to be filtered, the comparison operator, and the value to be compared.

For example, in the following description, the record with a field1 value of 0 will be selected.

```JSON
{
  "filters": [
    { "key": "field1", "op": "=", "value": 0 }
  ]
}
```

If you specify multiple filter conditions, each filter condition will be combined with AND condition.

For example, the following filter condition is the same as the description.

`(field1 IS NOT NULL AND field2 >= 10 AND field3 IN ["a", "b", "c"])`

```JSON
{
  "filters": [
    { "key": "field1", "op": "!=", "value": null },
    { "key": "field2", "op": ">=", "value": 10 },
    { "key": "field3", "op": "in", "value": ["a", "b", "c"] }
  ]
}
```

If you want to combine filter conditions with OR, or define multiple filter conditions nested together, you can do so as follows.

For example, the following filter condition is the same as the description.

`(field1 = 0 OR field2 < 10 OR (field3 = "a" AND field4 NOT IN [0, 5])`

```JSON
{
  "filters": {
    "or": [
      { "key": "field1", "op": "=", "value": 0 },
      { "key": "field2", "op": "<", "value": 10 },
      {
        "and": [
          { "key": "field3", "op": "=", "value": "a" },
          { "key": "field4", "op": "not in", "value": [0, 5] },
        ]
      }
    ]
  }
}
```

In this example, as a filter condition, instead of an array, we specify an object with the name `and` or `or` property whose value is an array of filter conditions.
It can be nested in filter conditions.

Currently, the following comparison operators are supported

`=`,`!=`,`>`,`>=`,`<`,`<=`,`in`,`not in`

The fields that can be used for comparison must be of type string or numeric or date or timestamp.
