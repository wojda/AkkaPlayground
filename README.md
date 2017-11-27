# AkkaPlayground

## [Building Blocks](https://doc.akka.io/docs/akka/current/stream/stages-overview.html?language=scala)
* [Source](https://doc.akka.io/api/akka/current/akka/stream/scaladsl/Source$.html)
* Flow
* [Sink](https://doc.akka.io/api/akka/current/akka/stream/scaladsl/Sink$.html)
* Runnable Graph
  * Fan-out
    * Broadcast[T]
    * Balance[T]
    * UnzipWith[In,A,B,...]
    * UnZip[A,B]
  * Fan-in
    * Merge[In]
    * MergePreferred[In]
    * MergePrioritized[In]
    * ZipWith[A,B,...,Out]
    * Zip[A,B]
    * Concat[A]
## Integration
* [Kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html)

## Materials
* [Akka Stream Docs](https://doc.akka.io/docs/akka/current/stream/index.html?language=scala)

## Extensions
* [Paypal Squbs](https://github.com/paypal/squbs)
