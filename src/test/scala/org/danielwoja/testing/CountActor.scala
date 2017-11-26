package org.danielwoja.testing

import akka.actor.{Actor, Props}

object CountActor {
  val props = Props[CountActor]
}

class CountActor extends Actor {
  override def receive = behaviour(Map.empty)

  def behaviour(state: Map[Int, Int]): Receive = {
    case n: Int =>
      val counter = state.getOrElse(n, 0) + 1
      sender() ! counter
      context.become(behaviour( state.updated(n, counter) ))
  }

}
