package org.danielwoja.testing

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps


trait BaseSpec extends FlatSpec with Matchers with ScalaFutures with Eventually {

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  implicit val config: PatienceConfig = PatienceConfig(timeout = 5 second)
  implicit val askTimeout: Timeout = 1 second

}
