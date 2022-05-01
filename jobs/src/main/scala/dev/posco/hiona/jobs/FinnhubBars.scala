/*
 * Copyright 2022 devposco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.posco.hiona.jobs

import cats.Monoid
import dev.posco.hiona._
import dev.posco.hiona.jobs.Featurize._

/** Consider FinnhubDBCandle instead */
object FinnhubBars {
  case class Bar(
      symbol: Symbol,
      timestampSeconds: Long,
      open: Double,
      high: Double,
      low: Double,
      close: Double,
      volume: Double,
      resolution: Int,
      adjusted: Int
  ) {

    def change: Double = close / open
    def priceRange: Double = high / low

    def toValues: Values[Double] = Values(close, change, priceRange, volume)
  }

  val v: Validator[Bar] =
    Validator.pure[Bar](b => Timestamp(b.timestampSeconds * 1000L))

  val src: Event.Source[Bar] =
    Event.source("finnhub_bar", v)

  val latestBar: Feature[Symbol, Option[Bar]] =
    src.latestBy(_.symbol)

  // region Values

  /**
    * Our featurization consists of "zero history" features, some labels (as in
    * labels for supervised learning), and "data cubed" features. The values
    * below are what is subjected to the "data cubing" -- for each of these,
    * various transforms will be computed
    *
    * TODO: rename Values -> ValuesToFeaturize? FeaturizationInputs?
    */
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

    def moments(vd: Values[Double]): Values[Moments2] =
      vd.map(Moments2.value)
  }

  // endregion Values

  case class Target(close: Double, volume: Double)

  val label = Label(latestBar.map {
    case None    => Target(Double.NaN, 0.0)
    case Some(b) => Target(b.close, b.volume)
  })

  case class ZeroHistoryFeatures(
      ts: Timestamp,
      symbol: Symbol,
      minuteOfDay: Int,
      dayOfWeek: Int,
      log: Values[Double],
      lin: Values[Double]
  )

  val zeroHistory: Event[ZeroHistoryFeatures] =
    src.withTime
      .map {
        case (b, ts) =>
          val v = b.toValues
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
      : Feature[Symbol, Decays[(Values[Moments2], Values[Moments2])]] =
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

    val lab15 = label.lookForward(Duration.minutes(15))
    val lab30 = label.lookForward(Duration.minutes(30))

    val lab15_30: Label[Symbol, (Target, Target)] = lab15.zip(lab30)

    LabeledEvent(ev, lab15_30)
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

object FinnhubBarsApp extends App0(Output.labeledEvent(FinnhubBars.labeled))

// This is the AWS-lambda version of the above
class AwsFinnhubBars
    extends aws.LambdaApp0(Output.labeledEvent(FinnhubBars.labeled))
