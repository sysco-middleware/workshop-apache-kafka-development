# [WIP] Workshop: Apache Kafka and Stream Processing

## Goals

* Been able to create and implement applications to integrate with 
Apache Kafka using APIs: Producer, Consumer, and Streams.
* Understand the benefits of binary formats and schema evoluting using 
Schema Registry and Apache Avro.
* Understand integration strategies with Apache Kafka by using Kafka Connectors
* Explore data and create data pipelines using KSQL  

## Lessons

* Get started with Kafka Clients: Producer, Consumer and Admin APIs.
* Understand how to use binary formats, like Apache Avro.
* Implement Stream Processing applications with Kafka Streams.
* Use Kafka Connect to integrate with data sources.
* Explore SQL Streaming with KSQL.

## Introduction

### Kafka Cluster

A Kafka cluster is composed by a set of `brokers`. This brokers are scaled 
horizontally to support more load/data. Internally Kafka uses Zookeeper to store
internal metadata to manage the cluster.

### Kafka Topics

Apache Kafka organizes messages (called `Record` at the API level) and are grouped 
in logic representations called `Topics`. 

Topics are partitioned, then we have Topic Partitions. Each Topic Partition is located
in a Broker, there could be many Topic Partitions per Broker.

Topic Partitions define how "parallel" is a Topic: Clients could send/poll Records in
parallel, by partition. 

More info: https://www.confluent.io/blog/how-to-choose-the-number-of-topicspartitions-in-a-kafka-cluster/

To achieve Data consistency and availability, Topic Partitions are replicated, 
defined by a `replication-factor`. This means that every Topic Partition will have 
a copy in other broker(s).