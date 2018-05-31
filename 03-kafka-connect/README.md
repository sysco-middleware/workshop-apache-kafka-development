# Kafka Connectors

Kafka Connect API defines a way to implement Source or Sink 
connectors. This connectors are aimed to move data from a Data Source
to Kafka (Source) and to move data from Kafka to Data Source (Sink).

Once Connectors are developed, it can be reused via configuration. 

## Demo: Twitter Source

Copy `jar` file in the plugin path: 

```bash
cp twitter-source-connector/kafka-connect-twitter-0.1-jar-with-dependencies.jar /usr/share/java/kafka-connect-twitter
```

And load connector:

```bash
confluent load twitter-source -d twitter-source-connector/twitter-source-config-file.properties
```

Check connector: 

```
$ curl http://localhost:8083/connectors | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100    18  100    18    0     0   2250      0 --:--:-- --:--:-- --:--:--  2571
[
  "twitter-source"
]
```

```
$ curl http://localhost:8083/connectors/twitter-source | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   524  100   524    0     0  65500      0 --:--:-- --:--:-- --:--:-- 65500
{
  "name": "twitter-source",
  "config": {
    "connector.class": "com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector",
    "twitter.token": "135295304-BtQsTuzMelReQTIOAwHePx9z1dO1NrfhTKnXv0LV",
    "topics": "tweets",
    "twitter.secret": "e4FKesWZdbvJmNkKWgLn4UVUrpr1KJANOQrTlxdUCnn9h",
    "track.terms": "bet,norsk-tipping",
    "name": "twitter-source",
    "twitter.consumersecret": "n1d8UbayHn0yKxocZDo7IxbxJsPr41pnpk8lvrpuFHXnAgTpBB",
    "twitter.consumerkey": "UNlTu7ZodAnfpv4yeJJmQFkzT"
  },
  "tasks": [
    {
      "connector": "twitter-source",
      "task": 0
    }
  ],
  "type": "unknown"
}
```

And test consuming records:

```bash
$ kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic tweets
```

## Demo: File Sink

Load File Sink Connector:

```bash
$ confluent load file-sink -d file-sink-connector/file-sink.properties 
```

Validate Connector with API.

And tail file to check that records are stores:

```bash
 tail -f /tmp/twitter-file-sink.txt 
```