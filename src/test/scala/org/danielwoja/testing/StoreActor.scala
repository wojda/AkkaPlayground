package org.danielwoja.testing

import akka.actor.{Actor, Props}
import org.danielwoja.testing.StoreActor.GetStored


object StoreActor {
  val props: Props = Props[StoreActor]
  case object GetStored
}

class StoreActor extends Actor {
  override def receive = store(Seq())

  val store: Seq[Int] => Receive = (v: Seq[Int]) => {
    case number: Int => context.become(store(v :+ number))
    case GetStored => sender() ! v
  }
}