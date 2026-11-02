# StarRocks Connector for Apache Kafka
starrocks-connector-for-kafka is a plugin of Apache Kafka Connect - Used to Ingest data from Kafka Topic to StarRocks Table.

## Documentation
For the user manual of the released version of the Kafka connector, please visit the StarRocks official documentation.


* [Load data using Kafka connector](https://docs.starrocks.io/docs/loading/Kafka-connector-starrocks/)
* [Load data with merge commit](doc/MergeCommit-HowTo.md)

## How to build
The connector depends on `starrocks-stream-load-sdk` 1.1-SNAPSHOT, which supports merge commit and is not on Maven Central yet. Install it from [starrocks-connector-for-apache-flink](https://github.com/StarRocks/starrocks-connector-for-apache-flink) first, with JDK 8:

```bash
git clone https://github.com/StarRocks/starrocks-connector-for-apache-flink.git
cd starrocks-connector-for-apache-flink/starrocks-stream-load-sdk
mvn install -DskipTests
```

Executing the `mvn package` command will generate the JAR file for the connector along with the JAR files that the connector depends on. The path is located at `target/starrocks-connector-for-kafka-1.0-SNAPSHOT-package/share/java`.

## Integration tests
The `*IT` tests drive the sink task against a real StarRocks cluster, with and without merge commit, which needs StarRocks 3.4 or later. They are skipped unless `STARROCKS_IT_FE` is set:

```bash
STARROCKS_IT_FE=<fe host> mvn test -Dtest='*IT'
```

`STARROCKS_IT_HTTP_PORT`, `STARROCKS_IT_QUERY_PORT`, `STARROCKS_IT_USER` and `STARROCKS_IT_PASSWORD` override the defaults (8030, 9030, `root` and no password). Each run creates a database and drops it at the end, unless `STARROCKS_IT_KEEP_DATABASE=true`.

## LICENSE
The connector is under the [Apache License 2.0](LICENSE.txt).