# StarRocks Connector for Apache Kafka
starrocks-connector-for-kafka is a plugin of Apache Kafka Connect - Used to Ingest data from Kafka Topic to StarRocks Table.

## Documentation
For the user manual of the released version of the Kafka connector, please visit the StarRocks official documentation.


* [Load data using Kafka connector](https://docs.starrocks.io/docs/loading/Kafka-connector-starrocks/)

## How to build
Executing the `mvn package` command will generate the JAR file for the connector along with the JAR files that the connector depends on. The path is located at `target/starrocks-connector-for-kafka-1.0-SNAPSHOT-package/share/java`.