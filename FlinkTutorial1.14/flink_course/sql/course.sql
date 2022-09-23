create table t_kafka
(
    id     int,
    name   string,
    age    int,
    gender string
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'hadoop102:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
      )
