# Usage

This guide walks you through configuring and using Aerospike Bundle in your Dropwizard application. For deeper explanations of each concept, follow the links to the dedicated pages.

## Prerequisites

- **Java 17+**
- **Apache Maven 3.8+**
- **Dropwizard 2.0.x** application
- A running **Aerospike** cluster

## Add Dependency

=== "Maven"

    ```xml
    <dependency>
        <groupId>com.phonepe</groupId>
        <artifactId>aerospike-bundle</artifactId>
        <version>1.0.0</version>
    </dependency>
    ```

=== "Gradle"

    ```groovy
    implementation 'com.phonepe:aerospike-bundle:1.0.0'
    ```

---

## Step 1: Define Your Configuration Class

```java
@Data
public class MyAppConfiguration extends Configuration {
    @Valid
    @NotNull
    private AerospikeBundleConfig aerospike;
}
```

---

## Step 2: Register the Bundle

```java
public class MyApplication extends Application<MyAppConfiguration> {

    private final AerospikeBundle<MyAppConfiguration> aerospikeBundle =
        new AerospikeBundle<MyAppConfiguration>() {
            @Override
            protected AerospikeBundleConfig configuration(MyAppConfiguration config) {
                return config.getAerospike();
            }
        };

    @Override
    public void initialize(Bootstrap<MyAppConfiguration> bootstrap) {
        bootstrap.addBundle(aerospikeBundle);
    }

    @Override
    public void run(MyAppConfiguration config, Environment environment) {
        IAerospikeClient client = aerospikeBundle.getAerospikeClient();
        // Pass client to your resources/services
    }
}
```

---

## Step 3: Configure Your YAML

=== "Default Mode (single cluster)"

    ```yaml
    aerospike:
      modeOfOperationType: DEFAULT_MODE
      aerospikeConfiguration:
        id: "primary"
        hosts:
          - host: "localhost"
            port: 3000
        retries: 5
        sleepBetweenRetries: 100
        maxConnectionsPerNode: 10
        socketTimeout: 1000
        totalTimeout: 3000
        threadPoolSize: 16
        healthcheckEnabled: true
        metricsEnabled: true
        readModeSC: SESSION
        replica: MASTER_PROLES
    ```

    See [Default Mode](modes/default-mode.md) for internals.

=== "Dual Mode (multi-cluster)"

    ```yaml
    aerospike:
      modeOfOperationType: DUAL_MODE
      aerospikeConfiguration:
        - id: "cluster-a"
          hosts:
            - host: "cluster-a.example.com"
              port: 3000
          healthcheckEnabled: true
        - id: "cluster-b"
          hosts:
            - host: "cluster-b.example.com"
              port: 3000
          healthcheckEnabled: true
      asReadWriteConfig:
        readClusterId: "cluster-a"
        writeClusterId: "cluster-a"
    ```

    See [Dual Mode](modes/dual-mode.md) for internals and dynamic routing.

Learn more about when to use each mode: [Modes of Operation](concepts/modes.md).

---

## Interceptors

Add custom logic before/after every Aerospike operation:

```java
aerospikeBundle.registerInterceptor(new MyLoggingInterceptor());
```

See [Interceptors](concepts/interceptors.md) for the full guide on writing and registering interceptors.

---

## Custom Policies

Override the default read/write/scan/query/batch policies by implementing the corresponding protected methods:

```java
new AerospikeBundle<MyAppConfiguration>() {
    @Override
    protected AerospikeBundleConfig configuration(MyAppConfiguration config) {
        return config.getAerospike();
    }

    @Override
    protected WritePolicy writePolicy(MyAppConfiguration config) {
        WritePolicy wp = new WritePolicy();
        wp.maxRetries = 3;
        wp.commitLevel = CommitLevel.COMMIT_ALL;
        return wp;
    }

    @Override
    protected Policy readPolicy(MyAppConfiguration config) {
        Policy rp = new Policy();
        rp.maxRetries = 2;
        return rp;
    }
};
```

See [Defaults & Configuration](concepts/defaults.md) for all default values.

---

## TLS Configuration

```yaml
aerospike:
  modeOfOperationType: DEFAULT_MODE
  aerospikeConfiguration:
    id: "secure-cluster"
    hosts:
      - host: "secure.example.com"
        port: 4333
        tlsName: "secure.example.com"
    user: "admin"
    password: "secret"
    tlsProtocols:
      - "TLSv1.2"
      - "TLSv1.3"
```

TLS is enabled automatically when `tlsName` is provided on hosts and credentials are configured.

---

## Namespace Tag Resolution

Define namespaces with tags and resolve them at runtime:

```java
Optional<Namespace> ns = config.getNamespaceFor("users");
```

See [Namespaces](concepts/namespaces.md) for the full guide.

---

## XDR Replication Lag

Monitor cross-datacenter replication lag:

```java
Map<String, List<XdrLagDetails>> lag = aerospikeBundle.getXdrReplicationLag();
```

---

## Next Steps

- [Interceptors](concepts/interceptors.md) -- understand the interceptor chain
- [Modes of Operation](concepts/modes.md) -- choose between default and dual mode
- [Namespaces](concepts/namespaces.md) -- tag-based namespace resolution
- [API Reference](concepts/api-reference.md) -- full method signatures
- [Error Codes](concepts/error-codes.md) -- troubleshooting
