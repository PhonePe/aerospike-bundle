# API Reference

## AerospikeBundle

The main entry point. Extend this abstract class in your Dropwizard application.

| Method | Description |
|--------|-------------|
| `configuration(T config)` | **Abstract.** Return your `AerospikeBundleConfig` from the app config. |
| `getAerospikeClient()` | Returns the configured `IAerospikeClient` instance. |
| `registerInterceptor(AerospikeInterceptor)` | Add a custom interceptor to the chain. |
| `getXdrReplicationLag()` | Returns XDR replication lag details per datacenter. |
| `writePolicy(T config)` | Override to provide custom `WritePolicy`. |
| `readPolicy(T config)` | Override to provide custom `Policy` for reads. |
| `scanPolicy(T config)` | Override to provide custom `ScanPolicy`. |
| `queryPolicy(T config)` | Override to provide custom `QueryPolicy`. |
| `batchPolicy(T config)` | Override to provide custom `BatchPolicy`. |
| `threadPool(T config)` | Override to provide custom `ExecutorService`. |
| `refreshDualModeASReadWriteConfig(T config)` | Override for dynamic dual-mode routing. |

## AerospikeConfiguration

Configuration POJO for a single Aerospike cluster connection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | `String` | required | Unique identifier for this cluster |
| `hosts` | `List<AerospikeHost>` | required | List of seed hosts |
| `user` | `String` | `null` | Username for authentication |
| `password` | `String` | `null` | Password for authentication |
| `tlsProtocols` | `Set<String>` | `null` | TLS protocol versions |
| `retries` | `int` | `5` | Max retries for operations |
| `sleepBetweenRetries` | `int` | `100` | Sleep ms between retries |
| `maxConnectionsPerNode` | `int` | `5` | Max connections per node |
| `socketTimeout` | `int` | `0` | Socket timeout ms |
| `totalTimeout` | `int` | `0` | Total timeout ms |
| `threadPoolSize` | `int` | `0` | Thread pool size (0 = auto) |
| `healthcheckEnabled` | `boolean` | `false` | Register health check |
| `metricsEnabled` | `boolean` | `false` | Enable metrics |
| `sizeMetricsEnabled` | `boolean` | `false` | Emit size metrics for bins |
| `readModeSC` | `ReadModeSC` | `SESSION` | Strong consistency read mode |
| `replica` | `Replica` | `MASTER_PROLES` | Replica policy |
| `namespaces` | `List<Namespace>` | `[]` | Namespace configurations |
| `rackId` | `int` | `0` | Rack ID for rack-aware reads |
| `rackAware` | `boolean` | `false` | Enable rack awareness |
| `maxSocketIdle` | `Integer` | `null` | Max socket idle seconds |

## AerospikeInterceptor

Interface for operation interceptors.

| Method | Description |
|--------|-------------|
| `preProcess(AerospikeInterceptorContext)` | Called before each operation |
| `postProcess(AerospikeInterceptorContext)` | Called after each operation |
| `setNext(AerospikeInterceptor)` | Chain to the next interceptor |

## AerospikeClientWithMetrics

A wrapper around `IAerospikeClient` that invokes the interceptor chain and collects metrics for all key-level operations.

## DualModeAerospikeClient

Routes read/write operations to different clusters based on `DualModeASReadWriteConfig`.
