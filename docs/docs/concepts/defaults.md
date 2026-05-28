# Defaults & Configuration

## Default Policy Values

When no custom policy is provided, the bundle uses these defaults:

### Write Policy

| Setting | Default |
|---------|---------|
| `maxRetries` | Value from `AerospikeConfiguration.retries` (default: 5) |
| `sleepBetweenRetries` | Value from config (default: 100ms) |
| `commitLevel` | `COMMIT_ALL` |
| `socketTimeout` | Value from config |
| `totalTimeout` | Value from config |
| `sendKey` | `true` |
| `expiration` | `0` (never expires) |
| `replica` | Value from config (default: `MASTER_PROLES`) |

### Read Policy

| Setting | Default |
|---------|---------|
| `maxRetries` | Value from config |
| `sleepBetweenRetries` | Value from config |
| `socketTimeout` | Value from config |
| `totalTimeout` | Value from config |
| `sendKey` | `true` |
| `readModeSC` | Value from config (default: `SESSION`) |
| `replica` | Value from config |

### Scan Policy

| Setting | Default |
|---------|---------|
| `concurrentNodes` | `true` |
| `maxConcurrentNodes` | Value from config (default: 0) |
| `includeBinData` | `true` |

### Query Policy

| Setting | Default |
|---------|---------|
| `shortQuery` | Value from config (default: `true`) |
| `maxConcurrentNodes` | Value from config (default: 0) |
| `recordQueueSize` | Value from config (default: 5000) |
| `includeBinData` | `true` |

### Batch Policy

| Setting | Default |
|---------|---------|
| `maxConcurrentThreads` | Value from config (default: 1) |
| `allowInline` | `true` |
| `readModeSC` | Value from config |

### Client Policy

| Setting | Default |
|---------|---------|
| `maxConnsPerNode` | Value from config (default: 5) |
| `failIfNotConnected` | `true` |
| `threadPool` | Fixed pool of `threadPoolSize` or `availableProcessors * 4` |
| `rackId` | Value from config (default: 0) |
| `rackAware` | Value from config (default: false) |

## Configuration Validation

- `id` is required and must be unique across clusters in dual mode
- `hosts` list max size: 256
- `retries` >= 0
- `sleepBetweenRetries`: 10-10000
- `maxConnectionsPerNode` >= 1
- `threadPoolSize`: 0-1000
- Namespace tags must be unique across all namespaces
