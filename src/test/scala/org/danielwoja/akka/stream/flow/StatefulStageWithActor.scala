package org.danielwoja.akka.stream.flow

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.danielwoja.testing.{BaseSpec, CountActor}
import akka.pattern.ask

import scala.collection.immutable

class StatefulStageWithActor extends BaseSpec {

  "Stateful Stage" should "be implemented by using an actor" in {
    //Given
    val countActor = system.actorOf(CountActor.props)
    val statefulFlow = Flow[Int].mapAsync(1)(n => countActor.ask(n))

    //When
    val sinkMessages = Source(immutable.Seq(1,1,1,1,2)).via(statefulFlow).toMat(Sink.seq)(Keep.right).run()

    //Then
    sinkMessages.futureValue shouldBe Seq(1,2,3,4,1)
  }
}
