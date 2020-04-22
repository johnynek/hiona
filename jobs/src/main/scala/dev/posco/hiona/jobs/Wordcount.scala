package dev.posco.hiona.jobs

import dev.posco.hiona._

import cats.implicits._

object Wordcount {
  case class Data(word: String, ts: Timestamp)

  val ev: Event.Source[Data] = Event.source("data", Validator.pure[Data](_.ts))

  val countFeature: Feature[String, Long] = ev.map(d => (d.word, 1L)).sum

  val totalCount: Event[(Timestamp, String, Long)] =
    ev.map(d => (d.word, ()))
      .postLookup(countFeature)
      .valueWithTime
      .map { case (w, ((_, cnt), ts)) => (ts, w, cnt) }
}

class AwsWordcount extends aws.LambdaApp(Args.event(Wordcount.totalCount))
