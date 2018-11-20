package org.danielwoja.akka.kafka

import java.util
import java.util.Properties

import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.serialization.SerializationExtension
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.danielwoja.testing.BaseSpec
import com.ovoenergy.kafka.serialization.avro4s._
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication
import org.apache.kafka.common.errors.SerializationException

import scala.collection._
import scala.util.{Failure, Success, Try}

class ConsumeFromKafkaProduceToKafka extends BaseSpec with EmbeddedKafka with EmbeddedSchemaRegistry {

  implicit val serializer: StringSerializer = new StringSerializer
  implicit val deserializer: StringDeserializer = new StringDeserializer

  implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9095)


  implicit val schemaRegistryConfig = SchemaRegistryConfig(8088, s"localhost:${kafkaConfig.zooKeeperPort}")
  val schemaRegistryEndpoint = s"http://localhost:${schemaRegistryConfig.registryPort}"

  "Akka Reactive Kafka lib" should "allow to consume and produce Avro messages to Kafka" in {
    withRunningKafka {
      withEmbeddedSchemaRegistry { () =>

        implicit val userSerializer: Serializer[User] = avroBinarySchemaIdSerializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
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
          .map(msg => (msg.committableOffset, msg.record.value()))
          .map { case (offset, user) => (offset, FirstName(user.firstName)) }
          .map { case (offset, firstName) => firstName.asProducerMessage(offset) }
          .runWith(Producer.commitableSink(producerSettings))

        publishToKafka("test1", User("John", "Wick", 1978))

        //Then
        val consumedFirstName = consumeFirstMessageFrom[FirstName]("test2")
        consumedFirstName.value shouldBe "John"
      }
    }
  }

  it should "skip non-avro messages" in {
    withRunningKafka {
      withEmbeddedSchemaRegistry { () =>
        def tryDeserializer[T](d: Deserializer[T]): Deserializer[Try[T]] = new Deserializer[Try[T]] {
          override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = d.configure(configs, isKey)
          override def deserialize(topic: String, data: Array[Byte]): Try[T] = Try(d.deserialize(topic, data))
          override def close(): Unit = d.close()
        }

        implicit val userSerializer: Serializer[User] = uncloseable {
          avroBinarySchemaIdSerializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
        }
        implicit val userDeserializer: Deserializer[User] = avroBinarySchemaIdDeserializer(schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
        val consumerSettings: ConsumerSettings[String, Try[User]] = ConsumerSettings(system, deserializer, tryDeserializer(userDeserializer))
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")
          .withProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
          .withGroupId("stream")
        val producerSettings: ProducerSettings[String, User] = ProducerSettings(system, serializer, userSerializer)
          .withBootstrapServers(s"localhost:${kafkaConfig.kafkaPort}")

        val decider: Supervision.Decider = {
          case e: SerializationException => Supervision.Resume
          case _                      => Supervision.Stop
        }
        val mat = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

        def exceptionHandler[A, B]: ((A, Try[B])) => immutable.Iterable[(A, B)] = {
          case (a, Success(b)) =>
            List((a, b))
          case (_, Failure(e: SerializationException)) =>
            println("shit happens...")
            List()
          case (_, Failure(e)) =>
            throw e
        }

        Consumer.committableSource[String, Try[User]](consumerSettings, Subscriptions.topics("test1"))
          .map(msg => (msg.committableOffset, msg.record.value()))
          .mapConcat(exceptionHandler[ConsumerMessage.CommittableOffset, User])

          .map { case (offset, user) => (offset, superImportantBusinessLogic(user)) }
          .map { case (offset, user) => ProducerMessage.Message(new ProducerRecord[String, User]("test2", user), offset) }
          .runWith(Producer.commitableSink(producerSettings))(mat)

        //When
        publishStringMessageToKafka("test1", "that's not an avro message")
        publishToKafka("test1", User("John", "Rambo", 1986))(kafkaConfig, userSerializer)

        //Then
        val consumedUser = consumeFirstMessageFrom[User]("test2")
        consumedUser.lastname shouldBe "RAMBO"
      }
    }
  }

  def superImportantBusinessLogic(u: User): User = u.copy(lastname = u.lastname.toUpperCase)

  implicit class FirstNameOps(value: FirstName) {
    def asProducerMessage[P](passThrough: P): ProducerMessage.Message[String, FirstName, P] = {
      ProducerMessage.Message(new ProducerRecord[String, FirstName]("test2", value), passThrough)
    }
  }

  case class User(firstName: String, lastname: String, yearOfBirth: Int)

  case class FirstName(value: String)

  def uncloseable[T](ser: Serializer[T]): Serializer[T] = new Serializer[T] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ser.configure(configs, isKey)

    override def serialize(topic: String, data: T): Array[Byte] = ser.serialize(topic, data)

    override def close(): Unit = ()
  }

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
