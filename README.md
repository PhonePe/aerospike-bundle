# Aerospike Bundle

> A Dropwizard bundle for seamless Aerospike integration with metrics, interceptors, and dual-mode operations.

[![Build](https://github.com/PhonePe/aerospike-bundle/actions/workflows/maven.yml/badge.svg)](https://github.com/PhonePe/aerospike-bundle/actions/workflows/maven.yml)
[![SonarCloud](https://github.com/PhonePe/aerospike-bundle/actions/workflows/sonarcloud-checks.yml/badge.svg)](https://github.com/PhonePe/aerospike-bundle/actions/workflows/sonarcloud-checks.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=coverage)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=bugs)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=PhonePe_aerospike-bundle&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=PhonePe_aerospike-bundle)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/com.phonepe/aerospike-bundle)](https://central.sonatype.com/artifact/com.phonepe/aerospike-bundle)
[![Java](https://img.shields.io/badge/java-17%2B-blue.svg)](https://openjdk.org/projects/jdk/17/)

## Overview

Aerospike Bundle is a Java library that provides a production-ready [Dropwizard](https://www.dropwizard.io/) bundle for connecting to [Aerospike](https://aerospike.com/) clusters. It handles client lifecycle, policy configuration, health checks, metrics, pluggable interceptors, and multi-cluster routing out of the box.

## Features

- **Dropwizard Integration** -- lifecycle-managed Aerospike client
- **Configurable Policies** -- read, write, scan, query, batch
- **Health Checks** -- automatic cluster health monitoring
- **Metrics & Interceptors** -- built-in metric collection with a pluggable interceptor chain
- **Dual-Mode Operations** -- route reads/writes to different clusters
- **TLS Support** -- configurable TLS policies and protocols
- **Namespace Management** -- tag-based namespace resolution
- **XDR Lag Monitoring** -- cross-datacenter replication lag visibility

## Quick Start

### 1. Add Dependency

**Maven**

```xml
<dependency>
    <groupId>com.phonepe</groupId>
    <artifactId>aerospike-bundle</artifactId>
    <version>${aerospike-bundle.version}</version>
</dependency>
```

> **Note:** Find the latest version on [Maven Central](https://search.maven.org/artifact/com.phonepe/aerospike-bundle).

### 2. Configure

```yaml
aerospike:
  modeOfOperationType: DEFAULT_MODE
  aerospikeConfiguration:
    id: "primary"
    hosts:
      - host: "localhost"
        port: 3000
    retries: 5
    maxConnectionsPerNode: 10
    healthcheckEnabled: true
```

### 3. Register the Bundle

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
        // Use the client
    }
}
```

## API Overview

| Class | Method | Description |
|-------|--------|-------------|
| `AerospikeBundle<T>` | `getAerospikeClient()` | Get the configured client |
| | `registerInterceptor(...)` | Add custom interceptor |
| | `getXdrReplicationLag()` | Get XDR lag details |
| `AerospikeConfiguration` | `getNamespaceFor(tag)` | Resolve namespace by tag |

## Documentation

Full documentation is available at the [project docs site](https://phonepe.github.io/aerospike-bundle/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of Conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

Distributed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).
