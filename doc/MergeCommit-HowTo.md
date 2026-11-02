# Load Data with Merge Commit

## Background

Without merge commit, every flush of every task is a separate Stream Load transaction. A table therefore gets `tasks × flushes per second` transactions, and each one creates a data version. Lowering `bufferflush.intervalms` for fresher data, or raising `tasks.max` for throughput, multiplies the versions. This drives up compaction, and eventually makes StarRocks throttle the loads.

[Merge commit](https://docs.starrocks.io/docs/loading/StreamLoad/#merge-stream-load-requests), available in StarRocks 3.4 and later, solves this on the server side. StarRocks merges the Stream Load requests that arrive for a table within a time window (`merge_commit_interval_ms`) into one transaction, provided their Stream Load properties are identical. All tasks of a connector send identical properties, so a table gets at most one transaction per window, regardless of the number of tasks and the flush interval.

## Requirements

- StarRocks 3.4 or later.
- The connector built with `starrocks-stream-load-sdk` 1.1-SNAPSHOT. See [How to build](../README.md#how-to-build). Sending the requests of a table one at a time (`sink.merge-commit.max-concurrent-requests`) needs the SDK at starrocks-connector-for-apache-flink `3ad9f84` or later.

## Quick start

```properties
name=starrocks-sink
connector.class=com.starrocks.connector.kafka.StarRocksSinkConnector
topics=orders
tasks.max=8
starrocks.http.url=fe1:8030,fe2:8030
starrocks.database.name=demo
starrocks.username=loader
starrocks.password=******
sink.properties.format=json
sink.properties.enable_merge_commit=true
sink.properties.merge_commit_interval_ms=1000
bufferflush.intervalms=200
```

## Options

### Stream Load properties

| Option | Default | Description |
|---|---|---|
| `sink.properties.enable_merge_commit` | `false` | Load with merge commit. |
| `sink.properties.merge_commit_interval_ms` | (required) | The merge window in milliseconds. A larger window merges more requests into a transaction, at the cost of latency. |
| `sink.properties.merge_commit_parallel` | `3` | The parallelism of the load plan of each merged transaction, capped by the number of BE or CN nodes. |
| `sink.properties.merge_commit_async` | `true` | `true`: StarRocks returns a request once it receives the data. `false`: it returns once the merged transaction is visible. The connector waits until the data is visible before it commits the Kafka offsets in both cases. |

With merge commit and JSON, the connector also sets `strip_outer_array=true` and `ignore_json_size=true` unless you set them. A merge commit request carries its length, so without `ignore_json_size` StarRocks rejects JSON bodies larger than `streaming_load_max_batch_size_mb` (100 MB by default).

### Connector options

| Option | Default | Description |
|---|---|---|
| `bufferflush.intervalms` | `1000` | The flush interval. With merge commit, it can go down to `100`; without merge commit, the minimum stays `1000`. Keep it at most `merge_commit_interval_ms`, so that every task sends data in every window. |
| `sink.merge-commit.chunk.size` | 20971520, or 524288000 when requests are sent one at a time | The maximum bytes of a table sent in one request. |
| `sink.merge-commit.max-concurrent-requests` | `2147483647` | The maximum number of inflight requests of a table in a task. `0` or `1` sends one request at a time, which keeps the order of the records, see [Ordering for primary key tables](#ordering-for-primary-key-tables). A negative value means no limit. A limit needs the SDK at starrocks-connector-for-apache-flink `3ad9f84` or later, and the connector rejects it with an older SDK. |
| `sink.merge-commit.max-retries` | `10` | How many times a failed request is sent again before the flush fails, see [Semantics](#semantics). `0` fails the flush at once. |
| `sink.merge-commit.retry-interval-ms` | `1000` | How long to wait before sending a failed request again. The SDK adds up to 2 seconds at random, so the default retries span 10 to 30 seconds. |
| `sink.merge-commit.backend-direct-connection` | `false` | Send requests to the BE or CN nodes that StarRocks assigns to the table, instead of an FE node, which redirects every request through the leader FE. The nodes must be reachable from Kafka Connect. |
| `sink.merge-commit.http.thread.num` | `3` | Threads that check the state of the merged transactions. |
| `sink.merge-commit.http.max-connections` | `20` | Maximum HTTP connections to a StarRocks node. |
| `sink.merge-commit.http.total-max-connections` | `1000` | Maximum HTTP connections to StarRocks. |
| `sink.merge-commit.http.idle-connection-timeout-ms` | `60000` | How long an idle HTTP connection is kept. |
| `sink.merge-commit.node-meta.update-interval-ms` | `5000` | How often to refresh the nodes assigned to a table, with `backend-direct-connection`. |
| `sink.merge-commit.check-label-state.init-delay-ms` | `500` | How long after the merge window closes to first check a merged transaction. |
| `sink.merge-commit.check-label-state.interval-ms` | `500` | How often to check a merged transaction until it is visible. |
| `sink.merge-commit.check-label-state.timeout-ms` | the Stream Load `timeout` | How long to wait for a merged transaction to become visible. |
| `sink.publish-timeout.ms` | `10000` with merge commit, `-1` otherwise | How long a committed transaction may stay unpublished before the load counts as successful. `-1` uses the StarRocks default. |
| `sink.io.thread-count` | `2` | Threads that send requests to StarRocks in a task. With `merge_commit_async=false`, each request holds a thread for the whole merge window. |

## Semantics

- **At-least-once.** When Kafka Connect commits offsets, the connector flushes and waits until the merged transactions of all data sent so far are visible. If a load fails, the commit fails and Kafka Connect replays the records from the last committed offsets.
- **Failed requests are sent again.** The SDK sends each request to an FE it picks at random from `starrocks.http.url`, and does not skip an FE that is down. A failed request is sent again, to another FE picked at random, up to `sink.merge-commit.max-retries` times, which rides out an FE failover and the seconds StarRocks takes to notice a CN that died. Only when the retries run out does the flush fail and the tasks replay.
- **No exactly-once.** StarRocks generates the labels of merged transactions and ignores the labels of the requests, so replayed records are loaded again, and so is a request that is sent again after StarRocks loaded it but the response was lost. This is the same as without merge commit.
- **A bad row fails the whole window.** If a request fails a merged transaction, for example with a row that does not fit the table while `max_filter_ratio` is 0, every request in that transaction fails, including those of other tasks. The task that owns the bad row replays it and fails the next window again, so the loads of all tasks keep failing, and loading the same records again, until the bad record is skipped. `max_filter_ratio` applies to the merged transaction as a whole, so set `sink.properties.max_filter_ratio`, for example to `0.001`, to drop the bad rows instead, and `sink.properties.log_rejected_record_num` to keep them.

## Ordering for primary key tables

Without a limit on `sink.merge-commit.max-concurrent-requests`, a task sends the data of a table every `bufferflush.intervalms`, without waiting for the requests it sent before. The updates of a key can then apply out of order, without any error or failure:

- Each request goes to one of the BE or CN nodes that StarRocks assigns to the table. An FE picks one of them at random for every request, and with `sink.merge-commit.backend-direct-connection` the SDK takes them in turn. Consecutive requests of a task often land in the same merged transaction on different nodes, and the nodes write the data of that transaction to a tablet in no particular order. In a primary key table the row that arrives last wins, so an older upsert can overwrite a newer one, a delete can remove a key that a newer upsert wrote, and an older upsert can bring back a key that a newer delete removed.
- A request that fails and is sent again can land in a later merged transaction than the request after it.

A key keeps the wrong version until its next update. A later update hides an earlier reordering, so a test that only checks the final state of a table can pass in one run and fail in the next. Tests that updated 80,000 keys every 200 ms found keys at an older version and, with deletes, missing keys and deleted keys that came back. Loads without merge commit kept every key at its last version.

To keep a primary key table consistent:

- **Upserts only.** Set `sink.properties.merge_condition` to a version column, so that an older update does not overwrite a newer one. In the same tests, every key ended at its last version, also when a CN, the leader FE or a Kafka Connect worker was killed during the load, and through a rolling restart of the CNs. StarRocks does not apply a delete (`__op` = 1) with `merge_condition`: a load that has both fails with `delete with condition column`, and the tasks keep failing on it.
- **Deletes.** Set `sink.merge-commit.max-concurrent-requests` to `1`. A task then sends the next request of a table only after the merged transaction of the previous one is visible, so two requests of a task never share a transaction, and a request that is sent again lands before the next one. This keeps the updates of a key in order as long as they all go to the same Kafka partition, which they do when the producer uses the primary key as the record key. With deletes, every key ended at its last version in the same tests, also through the same failures. It needs the SDK at starrocks-connector-for-apache-flink `3ad9f84` or later, which fixes [a data loss](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/528) when a table reaches its limit, and costs latency and throughput:
  - A request waits until the previous one is visible, about one window plus the publish time. With a 1 second window, a record was visible after 2.0 seconds at p50 and 4.0 seconds at p99, against 0.5 to 0.9 and 2.0 seconds without a limit.
  - The merged transactions of a table run one after another, and take longer as they grow. In a throughput test on a duplicate key table with 3 CNs of 16 cores each, a table took at most about 1.6 million rows per second, with 48 tasks and with 96, against 2.8 million without a limit.
- Otherwise, load the table without merge commit, or mark deleted rows with a column instead of deleting them, so that `merge_condition` orders them like any other update.

## Latency

The time until a record is visible is roughly:

```
consume + bufferflush.intervalms + up to merge_commit_interval_ms + commit and publish
```

With `sink.merge-commit.max-concurrent-requests` set to `1`, add the wait for the previous request of the table to be visible, up to one more window plus commit and publish.

When Kafka Connect commits offsets (every `offset.flush.interval.ms`), the task waits for its inflight loads and does not consume in the meantime.

## Limitations

- All merge commit tasks in a Kafka Connect worker must use the same `starrocks.http.url`, `starrocks.username` and `starrocks.password`. The SDK shares its connections to StarRocks across them and keeps the settings of the first task, so the connector fails a task whose settings differ. Run such connectors in different workers, and restart the worker after you change these settings.
- For the same reason, the HTTP settings `sink.merge-commit.http.*` and `sink.merge-commit.node-meta.update-interval-ms` come from the first merge commit task in a worker.
- Merge commit helps when several tasks or frequent flushes write to a table. A single task with a long flush interval gains nothing and waits up to one more window.
- The SDK logs every request at INFO (`Load request trace`). Set the log level of `com.starrocks.data.load.stream.mergecommit` to `WARN` if that is too verbose.
