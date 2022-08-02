## Flink Cdc Producer

### 配置

```json5
{
  "default-flink-kafka-sink": {
    "bootstrap-servers": "",
    "delivery-guarantee": "",
    "transactional-id-prefix": "",
    "kafka-producer": [{"": ""}, {"": ""}, {"": ""}] // kafka producer config option
  },
  "branches": [
    {
      "branch-id": "",
      "flink-cdc-source": {
        "instance": "",
        "hostname": "",
        "username": "",
        "password": "",
        "port": 3306,
        "database-name": "", 
        "table-name": "",
        "server-id": "",
        "scan-startup-mode": {
          "mode": "",
          "file": "",
          "offset": "",
          "timestamp": ""
        }, //  
        "server-time-zone": "",
        "connect-timeout": "",
        "connect-max-retries": "",
        "connection-pool-size": "",
        "heartbeat-interval": "",
        "jdbc": [{"": ""}, {"": ""}, {"": ""}],
        "debezium": [{"": ""}, {"": ""}, {"": ""}]
      }
    },
    {
      "branch-id": "",
      "flink-cdc-source": {}
    },
    {
      "branch-id": "",
      "flink-cdc-source": {},
      "flink-kafka-sink": {
      	// udf kafka sink config ...
      }
    }
    // more branches...
  ]
}
```

##### todo

1. 日志