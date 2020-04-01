package dev.posco.hiona

import cats.Monoid
import cats.implicits._

object Example {
  // rows like
  // symbol,whatToShow,rth_only,bar_start_utc,open,high,low,close,volume
  // 1:HK,TRADES,False,2015-07-02 01:30:00 UTC,113.2,113.9,112.9,113.0,1039500
  case class StockData(
    symbol: String,
    whatToShow: Row.Dummy,
    rthOnly: Boolean,
    barStartUtc: String,
    open: BigDecimal,
    high: BigDecimal,
    low: BigDecimal,
    close: BigDecimal,
    volume: Long)

  object StockData {
    implicit val rowStockData: Row[StockData] =
      Row.genericRow
  }

  val dateValidator: Validator[StockData] =
    Validator.parseAndShiftUtc("yyyy-MM-dd hh:mm:ss 'UTC'", Duration.min(30))(_.barStartUtc)

  val hkStockData: Event[StockData] = Event.source("hk-stocks", dateValidator)


  // represents the current decayed value over four different decay half lifes
  case class D4(hour: Float, day: Float, week: Float, quarter: Float)
  case class DecayedFeature(high: D4, logHigh: D4, vol: D4, logVol: D4)

  val decayFeatures: Feature[String, DecayedFeature] = {
    def make4[N: Numeric](fn: StockData => N) = { (input: (StockData, Timestamp)) =>
      val (sd, ts) = input
      val v = fn(sd)
      (Decay.build[Decay.Hour.type, N](ts, v),
        Decay.build[Decay.Day.type, N](ts, v),
        Decay.build[Decay.Week.type, N](ts, v),
        Decay.build[Decay.Quarter.type, N](ts, v))
    }

    val makeHigh = make4(_.high)
    val makeLogHigh = make4 { sd => Targets.Log1(sd.high.toFloat) }
    val makeVol = make4(_.volume)
    val makeLogVol = make4 { sd => Targets.Log1(sd.volume.toFloat) }


    val inputs = hkStockData
      .withTime
      .map { case pair@(sd, ts) =>
        val value = (makeHigh(pair), makeLogHigh(pair), makeVol(pair), makeLogVol(pair))
        (sd.symbol, value)
      }
      .sum

    inputs.map { case (dh, dlh, dv, dlv) =>

      import Decay._

      def toD4(tuple: (Decay[Hour.type], Decay[Day.type], Decay[Week.type], Decay[Quarter.type])) =
        D4(tuple._1.value.toFloat, tuple._2.value.toFloat, tuple._3.value.toFloat, tuple._4.value.toFloat)

      DecayedFeature(toD4(dh), toD4(dlh), toD4(dv), toD4(dlv))
    }
  }

  case class ZeroHistoryFeatures(ts: Timestamp, currentHigh: Float, currentLogHigh: Float, currentVolume: Float, currentLogVolume: Float)

  val eventWithNoHistoryFeatures: Event[(String, ZeroHistoryFeatures)] =
    hkStockData.withTime.map { case (sd, ts) =>
      (sd.symbol,
        ZeroHistoryFeatures(ts, sd.high.toFloat, Targets.Log1(sd.high.toFloat), sd.volume.toFloat, Targets.Log1(sd.volume.toFloat)))
    }

  // with the decayed history:
  //
  val eventWithDecay = eventWithNoHistoryFeatures.postLookup(decayFeatures)

  object Targets {
    sealed trait Transform {
      def apply(f: Float): Float
    }
    case object Identity extends Transform {
      def apply(f: Float) = f
    }
    case object Log1 extends Transform {
      def apply(f: Float) = math.log(f + 1.0f).toFloat
    }

    case class Values[Min <: Int, T <: Transform](highPrice: Float, lowPrice: Float, volume: Float)

    def coreEvent[Min <: Int, T <: Transform: ValueOf]: Event[(String, Values[Min, T])] = {
      val fn = valueOf[T]
      hkStockData.map { sd =>
        (sd.symbol, Values[Min, T](fn(sd.high.toFloat), fn(sd.low.toFloat), fn(sd.volume.toFloat)))
      }
    }

    def makeValue[Min <: Int: ValueOf, T <: Transform: ValueOf]: Label[String, Option[Values[Min, T]]] = {
      val dur = Duration.min(valueOf[Min])

      Label(
        coreEvent[Min, T].latest(Duration.Infinite)
      )
      .lookForward(dur)
    }

    // use 45 and 90 minutes in the future
    case class Target(
      v45lin: Option[Values[45, Identity.type]], v45log: Option[Values[45, Log1.type]],
      v90lin: Option[Values[90, Identity.type]], v90log: Option[Values[90, Log1.type]])

    val label: Label[String, Target] =
      makeValue[45, Identity.type]
        .zip(makeValue[45, Log1.type])
        .zip(makeValue[90, Identity.type])
        .zip(makeValue[90, Log1.type])
        .map { case (((a, b), c), d) => Target(a, b, c, d) }

  }


  // here if the full labeled data

  val fullLabeled = LabeledEvent(eventWithDecay, Targets.label)
}

object ExampleApp extends LabeledApp(Example.fullLabeled)
