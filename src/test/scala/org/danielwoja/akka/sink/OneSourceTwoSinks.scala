package org.danielwoja.akka.sink

import akka.actor.ActorSystem
import akka.pattern._
import akka.stream.scaladsl.{Broadcast, GraphDSL, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.Timeout
import org.danielwoja.testing.StoreActor
import org.danielwoja.testing.StoreActor.GetStored
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


class OneSourceTwoSinks extends FlatSpec with ScalaFutures with Matchers with Eventually {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val config: PatienceConfig = PatienceConfig(timeout = 5 second)
  implicit val askTimeout: Timeout = 1 second
  import GraphDSL.Implicits._

  "Partitioned Sink" should "accepts messages in the same order as was send" in {
    //Given
    val storeNumbersActor = system.actorOf(StoreActor.props)

    val numbersSource = Source(immutable.Seq(1, -1, 2, -2, 3, -3, 4))
    val partitionLogic = Partition[Int](2, n => if (n < 0) 0 else 1)
    val positiveSink = Sink.foreach[Int](n => { println(s"Positive number: $n"); storeNumbersActor ! n })
    val negativeSink = Sink.foreach[Int](n => { println(s"Negative number: $n"); Thread.sleep(200); storeNumbersActor ! n })

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      val partition = b.add(partitionLogic)

      numbersSource  ~> partition.in
                        partition.out(0) ~> negativeSink
                        partition.out(1) ~> positiveSink

      ClosedShape
    })

    //When
    graph.run()

    //Then
    eventually {
      storeNumbersActor.ask(GetStored).futureValue shouldBe Seq(1, -1, 2, -2, 3, -3, 4)
    }
  }

  "Broadcasting Sink" should "send the same message to both sinks" in {
    //Given
    val numbersSource = Source(immutable.Seq(1,2,3))
    val dbSink: Sink[Int, Future[immutable.Seq[Int]]] = Sink.seq[Int]
    val emailSink: Sink[Int, Future[immutable.Seq[Int]]] = Sink.seq[Int]

    val graph = RunnableGraph.fromGraph(GraphDSL.create(dbSink, emailSink)((_, _)) {
      implicit b => (dbSink, emailSink) =>
      val broadcast = b.add(Broadcast[Int](2, eagerCancel = true))

        numbersSource ~>  broadcast.in
                          broadcast.out(0) ~> dbSink
                          broadcast.out(1) ~> emailSink

      ClosedShape
    })

    //When
    val (dbSinkMessages, emailSinkMessages) = graph.run()

    //Then
    dbSinkMessages.futureValue shouldBe Seq(1,2,3)
    emailSinkMessages.futureValue shouldBe Seq(1,2,3)
  }

  /**
    * Simplified Api does not provide materialized values, that is why Sink.seq cannot be used.
    */
  it should "send the same message to both sinks when graph is built with simplified API" in {
    val dbSinkMessages = mutable.ListBuffer[Int]()
    val emailSinkMessages = mutable.ListBuffer[Int]()

    val dbSink = Sink.foreach[Int](dbSinkMessages += _)
    val emailSink = Sink.foreach[Int](emailSinkMessages += _)
    val broadcastingSink = Sink.combine(dbSink, emailSink)(Broadcast[Int](_, eagerCancel = true))

    val stream = Source(immutable.Seq(1, 2, 3)).to(broadcastingSink)

    //when
    stream.run()

    //Then
    eventually {
      dbSinkMessages shouldBe Seq(1,2,3)
      emailSinkMessages shouldBe Seq(1,2,3)
    }
  }
}

