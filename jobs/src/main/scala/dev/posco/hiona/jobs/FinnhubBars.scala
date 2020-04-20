package dev.posco.hiona.jobs

import dev.posco.hiona._
import cats.Monoid

import cats.implicits._

object FinnhubBars {
  // symbol,timestamp,open,high,low,close,volume,resolution,adjusted
  case class Bar(
      symbol: String,
      timestampSeconds: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Long,
      resolution: Int,
      adjusted: Int
  ) {

    def change: Double = close / open
    def priceRange: Double = high / low
  }

  val v: Validator[Bar] =
    Validator.pure[Bar](b => Timestamp(b.timestampSeconds * 1000L))

  val finnhubSrc: Event.Source[Bar] =
    Event.source("finnhub_bar", v)

  val latestBar: Feature[String, Option[Bar]] =
    finnhubSrc
      .map(bar => (bar.symbol, bar))
      .latest(Duration.Infinite)

  case class Target(close: Double, volume: Long)

  val label = Label(latestBar.map {
    case None    => Target(Double.NaN, 0L)
    case Some(b) => Target(b.close, b.volume)
  })

  case class Values[A](close: A, change: A, range: A, volume: A) {

    def map[B](fn: A => B): Values[B] =
      Values(
        close = fn(this.close),
        change = fn(this.change),
        range = fn(this.range),
        volume = fn(this.volume)
      )

    def zip[B](that: Values[B]): Values[(A, B)] =
      Values(
        close = (this.close, that.close),
        change = (this.change, that.change),
        range = (this.range, that.range),
        volume = (this.volume, that.volume)
      )
  }

  object Values {
    implicit def monoid[A: Monoid]: Monoid[Values[A]] = {
      import ShapelessMonoid._

      genericMonoid
    }

    implicit def doubleModule[A: DoubleModule]: DoubleModule[Values[A]] =
      DoubleModule.genericModule

    def fromBar(b: Bar): Values[Double] =
      Values(b.close, b.change, b.priceRange, b.volume.toDouble)

    def moments(vd: Values[Double]): Values[Moments2] = {
      import Moments2.value

      Values(
        close = value(vd.close),
        change = value(vd.change),
        range = value(vd.range),
        volume = value(vd.volume)
      )
    }
  }

  case class Decays[A](
      mins10: A,
      hour: A,
      hours2: A,
      day: A,
      days3: A,
      days14: A
  ) {
    def map[B](fn: A => B): Decays[B] =
      Decays(
        mins10 = fn(mins10),
        hour = fn(hour),
        hours2 = fn(hours2),
        day = fn(day),
        days3 = fn(days3),
        days14 = fn(days14)
      )
  }

  object Decays {
    val mins10: Duration = Duration.minute * 10
    val hours2: Duration = Duration.hour * 2
    val days3: Duration = Duration.day * 3
    val days14: Duration = Duration.day * 14

    def feature[K, V: DoubleModule](
        ev: Event[(K, V)]
    ): Feature[K, Decays[V]] = {
      val tupled = ev.valueWithTime
        .mapValues {
          case (v, ts) =>
            (
              Decay.fromTimestamped[mins10.type, V](ts, v),
              Decay.fromTimestamped[Duration.hour.type, V](ts, v),
              Decay.fromTimestamped[hours2.type, V](ts, v),
              Decay.fromTimestamped[Duration.day.type, V](ts, v),
              Decay.fromTimestamped[days3.type, V](ts, v),
              Decay.fromTimestamped[days14.type, V](ts, v)
            )
        }

      val feat = tupled.sum

      feat.mapWithKeyTime {
        case (_, (d1, d2, d3, d4, d5, d6), ts) =>
          Decays(
            d1.atTimestamp(ts),
            d2.atTimestamp(ts),
            d3.atTimestamp(ts),
            d4.atTimestamp(ts),
            d5.atTimestamp(ts),
            d6.atTimestamp(ts)
          )
      }
    }
  }

  case class ZeroHistoryFeatures(
      ts: Timestamp,
      symbol: String,
      minuteOfDay: Int,
      dayOfWeek: Int,
      log: Values[Double],
      lin: Values[Double]
  )

  val zeroHistory: Event[ZeroHistoryFeatures] =
    finnhubSrc.withTime
      .map {
        case (b, ts) =>
          val v = Values.fromBar(b)
          ZeroHistoryFeatures(
            ts,
            b.symbol,
            ts.unixMinuteOfDay,
            ts.unixDayOfWeek,
            log = v.map(d => math.log(d + 1e-6)),
            lin = v
          )
      }

  val decayedFeatures
      : Feature[String, Decays[(Values[Moments2], Values[Moments2])]] =
    Decays.feature(
      zeroHistory
        .map { zh =>
          (zh.symbol, (Values.moments(zh.log), Values.moments(zh.lin)))
        }
    )

  case class Result(
      zeroHistory: ZeroHistoryFeatures,
      logMoments: Decays[Values[Moments2]],
      linMoments: Decays[Values[Moments2]],
      logZ: Decays[Values[Double]],
      linZ: Decays[Values[Double]],
      target15: Target,
      target30: Target
  )

  val labeled: LabeledEvent[Result] = {
    val ev =
      zeroHistory
        .map(zh => (zh.symbol, zh))
        .postLookup(decayedFeatures)

    val lab1530 =
      label
        .lookForward(Duration.minutes(15))
        .zip(label.lookForward(Duration.minutes(30)))

    LabeledEvent(ev, lab1530)
      .map {
        case (_, ((zh, decayPair), (t15, t30))) =>
          val logM = decayPair.map(_._1)
          val linM = decayPair.map(_._2)
          val logZ = logM.map { v =>
            v.zip(zh.log).map { case (m, d) => m.zscore(d) }
          }
          val linZ = linM.map { v =>
            v.zip(zh.lin).map { case (m, d) => m.zscore(d) }
          }
          Result(zh, logM, linM, logZ, linZ, t15, t30)
      }
  }
}

object FinnhubBarsApp extends LabeledApp(FinnhubBars.labeled)
