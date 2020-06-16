package dev.posco.hiona.jobs

import dev.posco.hiona._

import java.util.{Calendar, TimeZone}
import cats.implicits._
import cats.Monoid

object Example {

  val hours2: Duration = Duration.hour * 2
  val days3: Duration = Duration.day * 3

  // rows like
  // symbol,whatToShow,rth_only,bar_start_utc,open,high,low,close,volume
  // 1:HK,TRADES,False,2015-07-02 01:30:00 UTC,113.2,113.9,112.9,113.0,1039500
  case class StockData(
      symbol: String,
      whatToShow: "TRADES",
      rthOnly: Boolean,
      barStartUtc: String,
      open: BigDecimal,
      high: BigDecimal,
      low: BigDecimal,
      close: BigDecimal,
      volume: Long
  )

  val utc = TimeZone.getTimeZone("UTC")

  // if the UTC start hour is 1 (1:30) we end in 30 minutes
  // so the event would be sent 30 minutes later.
  // otherwise the bar ends 1 hour later
  val correctTs: Timestamp => Timestamp = {
    val thirtyMin = Duration.minutes(30)

    { ts: Timestamp =>
      val c = Calendar.getInstance()
      c.setTimeZone(utc)
      c.setTimeInMillis(ts.epochMillis)
      if (c.get(Calendar.HOUR_OF_DAY) == 1)
        ts + thirtyMin
      else ts + Duration.hour
    }
  }

  implicit class TimestampOps(private val ts: Timestamp) extends AnyVal {
    def calendarFeatures[A](fn: Calendar => A): A = {
      val c = Calendar.getInstance()
      c.setTimeZone(utc)
      c.setTimeInMillis(ts.epochMillis)
      fn(c)
    }
  }

  val dateValidator: Validator[StockData] =
    Validator.parseAndShiftUtc(
      "yyyy-MM-dd hh:mm:ss 'UTC'",
      _.barStartUtc,
      correctTs
    )

  val hkStockData: Event.Source[StockData] =
    Event.source("hk-stocks", dateValidator)

  val barEndSrc: Event.Source[StockData] = {
    // to signal the end of bars we emit timestamps 1 ms after the end
    // of a bar, so we know for sure that the bar would have been seen if it exists
    val barEndValidator: Validator[StockData] =
      dateValidator.shiftLater(Duration.millisecond)
    Event.source("bar-end", barEndValidator)
  }

  // all symbols we have ever seen
  val allSymbols: Feature[Unit, Set[String]] =
    hkStockData.map(sd => ((), Set(sd.symbol))).sum

  // just a debug string of all symbols
  val symbolChanges: Event[String] =
    allSymbols.changes
      .filter { case (_, (v1, v2)) => v1 != v2 }
      .map {
        case (_, (v1, v2)) =>
          s"new = ${(v2 -- v1).toList.sorted}"
      }

  case class PriceData(
      closePrice: BigDecimal,
      volume: Long,
      barCloseTime: Timestamp
  )

  val latestBar: Feature[String, Option[PriceData]] =
    hkStockData.withTime.map {
      case (sd, ts) => (sd.symbol, PriceData(sd.close, sd.volume, ts))
    }.latest

  // these are timestamps of bar closings 1 ms after they happen
  val barEnd: Event[Timestamp] = {
    val monoid = new Monoid[Boolean] {
      def empty = false
      def combine(a: Boolean, b: Boolean) = a || b
    }

    // de-duplicate this event stream
    barEndSrc.withTime
      .map { case (_, ts) => (ts - Duration.millisecond, true) }
      .sum(monoid)
      .changes
      .concatMap {
        case (bar, (false, true)) =>
          // this is the first event in this bar
          bar :: Nil
        case _ => Nil
      }
  }

  // this gets the full price table at the close of each bar
  val densePriceData: Event[(String, PriceData)] =
    barEnd
      .map(barEndTs => ((), barEndTs))
      .postLookup(allSymbols)
      .concatMap {
        case (_, (barEndTs, symbols)) =>
          symbols.toList.sorted.map((_, barEndTs))
      }
      .postLookup(latestBar)
      .concatMap {
        case (symbol, (barEndTs, Some(pd))) if pd.barCloseTime == barEndTs =>
          (symbol, pd) :: Nil
        case (symbol, (barEndTs, Some(pd))) =>
          // we had an empty bar, just use the previous close
          (symbol, PriceData(pd.closePrice, 0L, barEndTs)) :: Nil
        case (_, (_, None)) =>
          // we have never seen a price for this symbol, so don't start now
          Nil
      }

  // represents the current decayed value over four different decay half lifes
  case class DValues(
      close: Moments2,
      logClose: Moments2,
      vol: Moments2,
      logVol: Moments2
  )
  object DValues {
    import ShapelessMonoid._

    // DValues is just a named tuple, combine point-wise
    implicit val dvalueMonoid: Monoid[DValues] =
      genericMonoid

    // DValues is just a named tuple, combine point-wise
    implicit val dvalueModule: DoubleModule[DValues] =
      DoubleModule.genericModule
  }

  case class DecayedFeature(
      hour: DValues,
      hours2: DValues,
      day: DValues,
      days3: DValues
  )

  val decayFeatures: Feature[String, DecayedFeature] = {

    val inputs = densePriceData.withTime.map {
      case ((symbol, pd), ts) =>
        val dv = DValues(
          Moments2.numeric(pd.closePrice),
          Moments2.numeric(Targets.Log1(pd.closePrice.toFloat)),
          Moments2.numeric(pd.volume),
          Moments2.numeric(Targets.Log1(pd.volume.toFloat))
        )

        val decayed =
          (
            Decay.fromTimestamped[Duration.hour.type, DValues](ts, dv),
            Decay.fromTimestamped[hours2.type, DValues](ts, dv),
            Decay.fromTimestamped[Duration.day.type, DValues](ts, dv),
            Decay.fromTimestamped[days3.type, DValues](ts, dv)
          )

        (symbol, decayed)
    }.sum

    inputs.mapWithKeyTime {
      case (_, (dh, dlh, dv, dlv), ts) =>
        DecayedFeature(
          dh.atTimestamp(ts),
          dlh.atTimestamp(ts),
          dv.atTimestamp(ts),
          dlv.atTimestamp(ts)
        )
    }
  }

  /**
    * Useful for keeping digits of a price
    * the idea is that some values -- like a round price that ends in a '0' are useful for prediction
    * so each of the select digits is recoded as one of '0' .. '9'
    * @param tensDollarsDigit
    * @param dollarsDigit
    * @param tensCentsDigit
    * @param centsDigit
    */
  case class Digits(
      tensDollarsDigit: Byte,
      dollarsDigit: Byte,
      tensCentsDigit: Byte,
      centsDigit: Byte
  )

  implicit class DigitsMethods(private val price: BigDecimal) extends AnyVal {

    /**
      * Encode the price into its Digits
      *
      * @return event with new feature values
      */
    def toDigits: Digits = {
      @inline def d(i: BigDecimal): Byte = (i.toInt % 10).toByte

      val p = price
      Digits(d(p / 10), d(p), d(p * 10), d(p * 100))
    }
  }

  case class ZeroHistoryFeatures(
      ts: Timestamp,
      hourOfDay: Int,
      dayOfWeek: Int,
      closeDigits: Digits,
      currentClose: Float,
      currentLogClose: Float,
      currentVolume: Float,
      currentLogVolume: Float
  )

  val eventWithNoHistoryFeatures: Event[(String, ZeroHistoryFeatures)] =
    densePriceData.withTime.map {
      case ((sym, pd), ts) =>
        val (hour, day) = ts.calendarFeatures { c =>
          (c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.DAY_OF_WEEK))
        }
        (
          sym,
          ZeroHistoryFeatures(
            ts,
            hour,
            day,
            pd.closePrice.toDigits,
            pd.closePrice.toFloat,
            Targets.Log1(pd.closePrice.toFloat),
            pd.volume.toFloat,
            Targets.Log1(pd.volume.toFloat)
          )
        )
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
      def apply(f: Float) = math.log(f.toDouble + 1.0).toFloat
    }

    case class Values[Min <: Int, T <: Transform](
        closePrice: Float,
        volume: Float
    )

    def coreEvent[Min <: Int, T <: Transform: ValueOf]
        : Event[(String, Values[Min, T])] = {
      val fn = valueOf[T]
      densePriceData.map {
        case (sym, pd) =>
          (
            sym,
            Values[Min, T](
              fn(pd.closePrice.toFloat),
              fn(pd.volume.toFloat)
            )
          )
      }
    }

    def makeValue[Min <: Int: ValueOf, T <: Transform: ValueOf]
        : Label[String, Option[Values[Min, T]]] = {
      val dur = Duration.minutes(valueOf[Min].toLong)

      Label(
        coreEvent[Min, T].latest
      ).lookForward(dur)
    }

    // 1 hour in the future is always generally the next bar
    case class Target(
        v60lin: Option[Values[60, Identity.type]],
        v60log: Option[Values[60, Log1.type]]
    )

    val label: Label[String, Target] =
      makeValue[60, Identity.type]
        .zip(makeValue[60, Log1.type])
        .map { case (a, b) => Target(a, b) }

  }

  case class ZScores(close: Float, logClose: Float, vol: Float, logVol: Float)
  case class DecayZScores(
      hour: ZScores,
      hours2: ZScores,
      day: ZScores,
      days3: ZScores
  )

  def zscores(zh: ZeroHistoryFeatures, dv: DValues): ZScores =
    ZScores(
      dv.close.zscore(zh.currentClose.toDouble).toFloat,
      dv.logClose.zscore(zh.currentLogClose.toDouble).toFloat,
      dv.vol.zscore(zh.currentVolume.toDouble).toFloat,
      dv.logVol.zscore(zh.currentLogVolume.toDouble).toFloat
    )

  // here if the full labeled data
  case class Result(
      symbol: String,
      zeroHistory: ZeroHistoryFeatures,
      decayed: DecayedFeature,
      decayedZ: DecayZScores,
      target: Targets.Target
  )

  val fullLabeled = LabeledEvent(eventWithDecay, Targets.label)
    .map {
      case (k, ((z, d), t)) =>
        val dz = DecayZScores(
          hour = zscores(z, d.hour),
          hours2 = zscores(z, d.hours2),
          day = zscores(z, d.day),
          days3 = zscores(z, d.days3)
        )
        Result(k, z, d, dz, t)
    }
}

object ExampleApp extends App0(Output.labeledEvent(Example.fullLabeled))
