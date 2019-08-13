
# spring-kafka-flume

AvroFlumeEvent 를 통해 이벤트의 발생시각을 기준으로 Flume 은 이벤트 데이터를 일자별 파티션 디렉토리 이하에 저장합니다. 
Flume Hdfs sink 는 `useLocalTimestamp` 를 제공하고 있지만, 이것은 이벤트의 수집시각이기에 원하는 결과를 얻을 수 없습니다.

1. 2019-08-01 23:59:59, 이벤트가 발생, API 로 전송
1. 2019-08-02 00:00:00, API 서버는 Kafka 로 Produce
1. 2019-08-02 00:00:01, Flume 은 Kafka source, Hdfs sink 를 통해 Hdfs 디렉토리에 저장

Hdfs Sink 가 데이터를 일자별로 파티션 하여 적재 한다면, `useLocalTimestamp` 는 2019-08-02 디렉토리에 데이터를 적재합니다. 
AvroFlumeEvent 클래스를 통해 헤더에 Timestamp 값을 저장하고 Flume 에서 이 헤더의 Timestamp 값을 기준으로 저장한다면 데이터는 2019-08-01 디렉토리에 저장되게 됩니다.


### Maven

```xml
<dependency>
    <groupId>org.apache.flume</groupId>
    <artifactId>flume-ng-sdk</artifactId>
    <version>1.8.0</version>
</dependency>
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.8.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-avro</artifactId>
    <version>2.8.5</version>
</dependency>
```

### Getting Started

Avro 처리는 Jackson 을 사용 했습니다. EmbeddedKafka 테스트 코드를(`EmbeddedKafkaTest.java`) 통해 Kafka 에 헤더값이 저장되는 것을 확인 할 수 있습니다. 
[CloudeKafka](https://www.cloudkarafka.com/) 는 Kafka 클러스터를 이용해 볼 수 있는 사이트 입니다. 
CloudKafka 를 사용하여 Api 를 테스트 해볼 수 있는 테스트 코드가(`RestApiWithCloudKafkaTest.java`) 포함되어 있습니다. 


### Flume Configuration
```properties
tier1.sinks.sink1.type = hdfs
tier1.sinks.sink1.hdfs.path = /etl/flume/activity-event/ymd=%Y-%m-%d

```

### Reference Documentation

[How to Extract Event Time in Apache Flume](http://shzhangji.com/blog/2017/08/05/how-to-extract-event-time-in-apache-flume/)


