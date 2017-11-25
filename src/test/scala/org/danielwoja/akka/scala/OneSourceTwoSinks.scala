package org.danielwoja.akka.scala

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern._
import akka.stream.scaladsl.{GraphDSL, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.Timeout
import org.danielwoja.akka.scala.StoreActor.GetStored
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection._
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
    val storeNumbersActor = system.actorOf(Props[StoreActor]())

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
}

object StoreActor {
  case object GetStored
}

class StoreActor extends Actor {
  override def receive = store(Seq())

  val store: Seq[Int] => Receive = (v: Seq[Int]) => {
    case number: Int => context.become(store(v :+ number))
    case GetStored => sender() ! v
  }
}
