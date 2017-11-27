package org.danielwoja.akka.stream.flow

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import org.danielwoja.testing.{BaseSpec, CountActor}

import scala.collection.immutable

class CustomStatefulStage extends BaseSpec {

  "Stateful Stage" should "be implemented with custom graph stage" in {
    //Given
    val statefulFlow = new ElementOccurrence[Int]

    //When
    val sinkMessages = Source(immutable.Seq(1,1,1,1,2)).via(statefulFlow).toMat(Sink.seq)(Keep.right).run()

    //Then
    sinkMessages.futureValue shouldBe Seq(1,2,3,4,1)
  }

  class ElementOccurrence[T] extends GraphStage[FlowShape[T, Int]] {
    val in = Inlet[T]("ElementOccurrence.in")
    val out = Outlet[Int]("ElementOccurrence.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(attr: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var countedElements = Map.empty[T, Int]

        def logic(element: T): Int = {
          val occurrence = countedElements.getOrElse(element, 0) + 1
          countedElements = countedElements.updated(element, occurrence)
          occurrence
        }

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            push(out, logic(grab(in)))
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }
}


