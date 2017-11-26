package org.danielwoja.akka.source

import akka.actor.ActorSystem
import akka.stream.Attributes.InputBuffer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, DelayOverflowStrategy, OverflowStrategy}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps


class QueueSource extends FlatSpec with Matchers with ScalaFutures {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val config: PatienceConfig = PatienceConfig(timeout = 5 second)
  implicit val askTimeout: Timeout = 1 second

  "QueueSource" should "allow sending messages from outside of a stream" in {
    //Given
    val queueSource = Source.queue[Int](1, OverflowStrategy.backpressure)
    val stream = queueSource.toMat(Sink.seq[Int])(Keep.both)

    //When
    val (sourceQueue, sinkMessages) = stream.run()
    sourceQueue.offer(1)
    sourceQueue.offer(2)
    sourceQueue.offer(3)
    sourceQueue.complete()

    //Then
    sinkMessages.futureValue shouldBe Seq(1, 2, 3)
  }

  it should "fail the offer when strategy is backpressure and previous offer did not finish" in {
    //Given
    val queueSource = Source.queue[Int](1, OverflowStrategy.backpressure)
    val slowStream = queueSource
      .delay(200 millis, DelayOverflowStrategy.backpressure).addAttributes(Attributes(InputBuffer(1, 1)))
      .toMat(Sink.seq[Int])(Keep.both)

    //When
    val (sourceQueue, sinkMessages) = slowStream.run()
    sourceQueue.offer(1).onComplete(e => println(e))
    sourceQueue.offer(2).onComplete(e => println(e))
    sourceQueue.offer(3).onComplete(e => println(e))
    sourceQueue.offer(4).onComplete(e => println(e))
    sourceQueue.complete()

    sinkMessages.futureValue should not be Seq(1, 2, 3, 4)
  }

  it should "not fail a new offer is send after previous offer had finished" in {
    //Given
    val queueSource = Source.queue[Int](1, OverflowStrategy.backpressure)
    val slowStream = queueSource
      .delay(200 millis, DelayOverflowStrategy.backpressure).addAttributes(Attributes(InputBuffer(1, 1)))
      .toMat(Sink.seq[Int])(Keep.both)

    //When
    val (sourceQueue, sinkMessages) = slowStream.run()
    sourceQueue.offer(1)
      .flatMap(_ => sourceQueue.offer(2))
      .flatMap(_ => sourceQueue.offer(3))
      .flatMap(_ => sourceQueue.offer(4))
      .map(_ => sourceQueue.complete())

    sinkMessages.futureValue shouldBe Seq(1, 2, 3, 4)
  }

}
