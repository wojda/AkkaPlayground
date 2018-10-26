package org.danielwoja.akka.kafka

import java.util.Properties

import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.danielwoja.testing.BaseSpec
import com.ovoenergy.kafka.serialization.avro4s._
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication

class ConsumeFromKafkaProduceToKafka extends BaseSpec with EmbeddedKafka with EmbeddedSchemaRegistry {

  implicit val serializer: StringSerializer = new StringSerializer
  implicit val deserializer: StringDeserializer = new StringDeserializer

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9095)


  implicit val schemaRegistryConfig = SchemaRegistryConfig(8088, s"localhost:${kafkaConfig.zooKeeperPort}")

  val schemaRegistryEndpoint = "http://localhost:8088"
  "Akka Reactive Kafka lib" should "allow to consume and prouduce to Kafka" in {
    withRunningKafka {
      withEmbeddedSchemaRegistry { () =>

        implicit val userSerializer: Serializer[User] = avroBinarySchemaIdSerializer(schemaRegistryEndpoint , isKey = false, includesFormatByte = true)
        implicit val userDeserializer: Deserializer[User] = avroBinarySchemaIdDeserializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
        implicit val firstNameSerializer: Serializer[FirstName] = avroBinarySchemaIdSerializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
        implicit val firstNameDeserializer: Deserializer[FirstName] = avroBinarySchemaIdDeserializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)


        val consumerSettings: ConsumerSettings[String, User] = ConsumerSettings(system, deserializer, userDeserializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
          .withProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
          .withGroupId("stream")
        val producerSettings: ProducerSettings[String, FirstName] = ProducerSettings(system, serializer, firstNameSerializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

        //When
        Consumer.committableSource[String, User](consumerSettings, Subscriptions.topics("test1"))
          .map(msg => ProducerMessage.Message(new ProducerRecord[String, FirstName]("test2", FirstName(msg.record.value.firstName)), msg.committableOffset))
          .runWith(Producer.commitableSink(producerSettings))

        publishToKafka("test1", User("John", "Wick", 1978))

        //Then
        val user = consumeFirstMessageFrom[FirstName]("test2")
        user.value shouldBe "John"
      }
    }
  }

  case class User(firstName: String, lastname: String, yearOfBirth: Int)
  case class FirstName(value: String)

}

case class SchemaRegistryConfig(registryPort: Int, zookeeperUrl: String)
trait EmbeddedSchemaRegistry {


  def withEmbeddedSchemaRegistry[T](f: () => T)(implicit config: SchemaRegistryConfig): T = {
    val properties = new Properties {
      put("listeners", s"http://0.0.0.0:${config.registryPort}")
      put("kafkastore.connection.url", config.zookeeperUrl)
      put("kafkastore.topic", "_schemas")
    }
    val server = new SchemaRegistryRestApplication(properties).createServer()
    server.start()
    try {
      f()
    } finally {
      server.stop()
    }

  }
}
