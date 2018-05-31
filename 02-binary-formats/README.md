# Binary Formats

Binary Formats are the most efficient way to send data over the wire. There are different libraries
that provide a way to define Schemas and generate binary data. e.g. Protocol Buffers, Avro, Thrift.

One of the main benefits is the schema evolution support (update schema without breaking clients):

More details: https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html

## Serializers and Deserializers

Remember that Kafka Producer and Consumer have to define Serializers and Deserializers. At the end
the Kafka Cluster stores binary data. 

## Avro

Avro helps defining a Schema that will be kept on the Producer and Consumer side. Both Schemas has to 
be compatible to be able to consume its data. 

Here is an example of a schema: 

```
{
  "namespace": "no.sysco.middleware.workshop.kafka.avro",
  "name": "Record1",
  "doc": "Some Record",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "record_type",
      "type": "string",
      "default": "RECORD_TYPE_1"
    },
    {
      "name": "optional_element",
      "type": ["string", "null"]
    }
  ]
}
```

## Schema Registry

By Technical and historical reasons (e.g. Hadoop use cases) tooling for Avro has been developed around
Kafka. Mainly the Schema Registry.

Most important Schema Registry API:

Subjects: 

```
$ curl http://localhost:8081/subjects | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100    74  100    74    0     0   9250      0 --:--:-- --:--:-- --:--:--  9250
[
  "avro-topic-value"
]

```

Subject versions:

```
$ curl http://localhost:8081/subjects/avro-topic-value/versions | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100     3  100     3    0     0    428      0 --:--:-- --:--:-- --:--:--   428
[
  1
]
```

Latest Schema:

```
$ curl http://localhost:8081/subjects/avro-topic-value/versions/latest | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   460  100   460    0     0   224k      0 --:--:-- --:--:-- --:--:--  224k
{
  "subject": "avro-topic-value",
  "version": 1,
  "id": 2,
  "schema": "{\"type\":\"record\",\"name\":\"Record1\",\"namespace\":\"no.sysco.middleware.workshop.kafka.avro\",\"doc\":\"Some Record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"record_type\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"RECORD_TYPE_1\"},{\"name\":\"optional_element\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]}]}"
}
```

Specific versions:

```
$ curl http://localhost:8081/subjects/avro-topic-value/versions/1 | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   460  100   460    0     0  57500      0 --:--:-- --:--:-- --:--:-- 57500
{
  "subject": "avro-topic-value",
  "version": 1,
  "id": 2,
  "schema": "{\"type\":\"record\",\"name\":\"Record1\",\"namespace\":\"no.sysco.middleware.workshop.kafka.avro\",\"doc\":\"Some Record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"record_type\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"RECORD_TYPE_1\"},{\"name\":\"optional_element\",\"type\":[{\"type\":\"string\",\"avro.java.string\":\"String\"},\"null\"]}]}"
}
```

Only schema:

```
$ curl http://localhost:8081/subjects/avro-topic-value/versions/1/schema | jq .
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   339  100   339    0     0  37666      0 --:--:-- --:--:-- --:--:-- 37666
{
  "type": "record",
  "name": "Record1",
  "namespace": "no.sysco.middleware.workshop.kafka.avro",
  "doc": "Some Record",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "record_type",
      "type": {
        "type": "string",
        "avro.java.string": "String"
      },
      "default": "RECORD_TYPE_1"
    },
    {
      "name": "optional_element",
      "type": [
        {
          "type": "string",
          "avro.java.string": "String"
        },
        "null"
      ]
    }
  ]
}
```

### Compatibility

Docs of configuration of compatibility: https://docs.confluent.io/current/schema-registry/docs/api.html#config

