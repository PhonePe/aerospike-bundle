# Error Codes

## ResponseCode

The bundle defines the following error response codes in `com.phonepe.aerospike.exception.ResponseCode`:

| Code | Description |
|------|-------------|
| Aerospike client errors | Propagated directly from the Aerospike Java client SDK |

## AerospikeBundleException

Custom exception thrown by the bundle for configuration and runtime errors.

| Scenario | Message |
|----------|---------|
| Duplicate cluster ID in dual mode | `"Duplicate cluster ID found: <id>"` |
| Invalid read/write config | Validation error from `AerospikeConfigValidationUtil` |
| Connection failure | `failIfNotConnected = true` causes startup failure |

## Common Aerospike Client Errors

| Result Code | Meaning |
|-------------|---------|
| `KEY_NOT_FOUND_ERROR` | Record does not exist |
| `GENERATION_ERROR` | Record generation mismatch (CAS failure) |
| `TIMEOUT` | Operation timed out |
| `SERVER_NOT_AVAILABLE` | Node is not responding |
| `DEVICE_OVERLOAD` | Storage device is overloaded |

Refer to the [Aerospike error codes documentation](https://aerospike.com/docs/server/reference/info#error-codes) for the full list.
