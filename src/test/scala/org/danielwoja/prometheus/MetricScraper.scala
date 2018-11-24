package org.danielwoja.prometheus

import org.danielwoja.prometheus.PrometheusMetric.{Metric, MetricClient}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Success, Failure, Try}

class MetricScraper extends FlatSpec with Matchers with MetricMatchers {
  val body =
    """
      |# HELP kafka_server_replicafetchermanager_minfetchrate Attribute exposed for management (kafka.server<type=ReplicaFetcherManager, name=MinFetchRate, clientId=Replica><>Value)
      |# TYPE kafka_server_replicafetchermanager_minfetchrate untyped
      |kafka_server_replicafetchermanager_minfetchrate 0.01
      |# HELP kafka_network_requestmetrics_totaltimems Attribute exposed for management (kafka.network<type=RequestMetrics, name=TotalTimeMs, request=OffsetFetch><>Count)
      |# TYPE kafka_network_requestmetrics_totaltimems untyped
      |kafka_network_requestmetrics_totaltimems{request="OffsetFetch",} 0.0
      |kafka_network_requestmetrics_totaltimems{request="JoinGroup",} 0.0
      |kafka_network_requestmetrics_totaltimems{request="DescribeGroups",} 0.0
      |kafka_network_requestmetrics_totaltimems{request="LeaveGroup",} 0.0
      |kafka_network_requestmetrics_totaltimems{request="GroupCoordinator",} 0.0
    """.stripMargin

  implicit val metricClient: PrometheusMetric.MetricClient = () => Success(body)
  val metricName = "kafka_server_replicafetchermanager_minfetchrate"

  "MetricScraper" should "just work" in {
    val metric = PrometheusMetric.init(metricName)

    metric should increaseBy (0)
  }
}

trait MetricMatchers {

  def increaseBy(diff: Int)(implicit mc: MetricClient): Matcher[Metric] =
    (metric: Metric) => PrometheusMetric.fetchMetric(metric.name)(mc) match {
      case Success(currentValue) =>
      MatchResult(
        currentValue - metric.initialValue == diff,
        s"${metric.name} was not increased by $diff. Initial value was: ${metric.initialValue}, current value is $currentValue",
        metric + " was odd"
      )
      case Failure(exception) => throw exception
    }
}

object PrometheusMetric {
  case class Metric (name: String, initialValue: Double)
  type MetricClient = () => Try[String]

  def init(metricName: String)(implicit mc: MetricClient): Metric = fetchMetric(metricName) match {
    case Success(metricValue) => Metric(metricName, metricValue)
    case Failure(e) => throw e
  }

  def fetchMetric(metricName: String)(implicit metricClient: MetricClient): Try[Double] = {
    val prometheusMetricPattern = s"(?s).*$metricName (\\d+.\\d+).*".r

    metricClient().map {
      case prometheusMetricPattern(metricValue) => metricValue.toDouble
      case _ => throw new RuntimeException("no match")
    }
  }


}

