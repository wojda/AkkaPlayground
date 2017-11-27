package org.danielwoja.akka.stream.source

import akka.actor.{ActorRef, Status}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.danielwoja.testing.BaseSpec

class ActorSource extends BaseSpec {

  /**
    * Source.actorRef does not support backpressure overflow strategy
    */
  "Actor Source" should "allow sending sending messages to a Stream by sending a message to an actor" in {
    //Given
    val source = Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.fail)

    //When
    val (sourceActor, sinkMessages) = source.log("", println(_)).toMat(Sink.seq)(Keep.both).run()
    sourceActor ! 1
    sourceActor ! Status.Success(0) //The stream can be completed successfully by sending a [[akka.actor.Status.Success]], content will be ignored

    //Then
    sinkMessages.futureValue shouldBe Seq(1)
  }

}
