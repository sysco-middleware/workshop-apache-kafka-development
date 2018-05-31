#!/usr/bin/env bash
KAFKA_HOME=/data/opt/apache/kafka/kafka_2.11-0.11.0.0

${KAFKA_HOME}/bin/connect-standalone.sh connect-standalone.properties file-sink.properties