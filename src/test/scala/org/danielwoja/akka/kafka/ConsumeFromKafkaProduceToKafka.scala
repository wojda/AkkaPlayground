package org.danielwoja.akka.kafka

import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.danielwoja.testing.BaseSpec

class ConsumeFromKafkaProduceToKafka extends BaseSpec with EmbeddedKafka {

  implicit val serializer: StringSerializer = new StringSerializer
  implicit val deserializer: StringDeserializer = new StringDeserializer
  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 12345)
  val consumerSettings: ConsumerSettings[String, String] = ConsumerSettings(system, deserializer, deserializer)
    .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
    .withProperty("auto.offset.reset", "earliest")
    .withGroupId("stream")
  val producerSettings: ProducerSettings[String, String] = ProducerSettings(system, serializer, serializer)
    .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")


  "Akka Reactive Kafka lib" should "allow to consume and prouduce to Kafka" in {
    withRunningKafka {
      //Given
      val source = Consumer.committableSource[String, String](consumerSettings, Subscriptions.topics("test1"))
      val consumerMessageToProducerMessage = (msg: ConsumerMessage.CommittableMessage[String, String]) => ProducerMessage.Message(new ProducerRecord[String, String]("test2", msg.record.value), msg.committableOffset)
      val sink = Producer.commitableSink(producerSettings)

      //When
      source
        .map(consumerMessageToProducerMessage)
        .runWith(sink)

      publishToKafka("test1", "pale_ale")

      //Then
      consumeFirstStringMessageFrom("test2") shouldBe "pale_ale"
    }
  }

}
