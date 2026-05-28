# Namespaces

Aerospike Bundle provides a tag-based namespace resolution system that lets you define all namespaces in your configuration and look them up by logical tags at runtime.

## Why Tags?

In a typical service, you might use multiple Aerospike namespaces for different purposes (user data, session cache, analytics). Rather than hardcoding namespace names throughout your code, you assign **tags** to namespaces and resolve them by tag. This decouples your application logic from the physical namespace layout.

## Configuration

```yaml
aerospike:
  modeOfOperationType: DEFAULT_MODE
  aerospikeConfiguration:
    id: "primary"
    hosts:
      - host: "localhost"
        port: 3000
    namespaces:
      - name: user_data
        consistency: SC
        replicationFactor: 2
        storage: FILE
        tags:
          - users
          - profiles
      - name: cache_ns
        consistency: AP
        replicationFactor: 2
        storage: MEMORY
        tags:
          - session_cache
```

## Resolving a Namespace

```java
AerospikeConfiguration config = ...;

// Resolve by tag
Optional<Namespace> ns = config.getNamespaceFor("users");
ns.ifPresent(n -> {
    String namespaceName = n.getName();       // "user_data"
    String consistency = n.getConsistency();  // "SC"
});
```

## Validation Rules

- **Tags must be unique across all namespaces.** If two namespaces share the same tag, configuration validation will fail at startup.
- This is enforced by the `@ValidationMethod` on `AerospikeConfiguration`.

## Namespace Properties

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | The Aerospike namespace name |
| `consistency` | `String` | Consistency model (SC, AP) |
| `replicationFactor` | `int` | Number of replicas |
| `storage` | `String` | Storage engine (FILE, MEMORY) |
| `tags` | `List<String>` | Logical tags for lookup |

## See Also

- [Usage Guide](../usage.md#namespace-tag-resolution) -- quick example
- [Defaults & Configuration](defaults.md) -- all configuration fields
- [API Reference](api-reference.md) -- `getNamespaceFor()` method
