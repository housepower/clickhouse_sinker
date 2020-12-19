# task config

```
{
  "name": "daily_request",

  // kafka cluster
  "kafka": "kfk1",
  "topic": "topic",

  // kafka consume from earliest or latest
  "earliest": true,
  // kafka consumer group
  "consumerGroup": "group",

  // message parser
  "parser": "json",

  // clickhouse cluster
  "clickhouse": "ch1",

  // table name
  "tableName": "daily",

  // columns of the table
  "dims": [
    {
      "name": "day",
      "type": "Date",
      "sourceName": "day"
    },
    ...
  ],

  // if it's specified, the schema will be auto mapped from clickhouse,
  "autoSchema" : true,
  // "this columns will be excluded by insert SQL "
  "excludeColumns": []
}

```