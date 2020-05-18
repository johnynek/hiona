package dev.posco.hiona.aws

import cats.effect.Sync
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.codec.binary.Base32

import cats.implicits._

trait UniqueName[F[_]] {
  def next: F[String]
}

object UniqueName {

  /**
    * This creates a unique base32 string of 128 bits
    * total:
    * low 44 bits of timestamp to millis (500 years)
    * 20 bits of a counter
    * 64 bits of secure RNG
    */
  def build[F[_]: Sync]: F[UniqueName[F]] =
    Sync[F]
      .delay(
        (
          new AtomicInteger(0),
          new Array[Byte](8),
          new Array[Byte](16),
          new SecureRandom(),
          new Base32()
        )
      )
      .map {
        case (counter, rngBytes, buf, secRand, b32) =>
          new UniqueName[F] {
            val F = Sync[F]

            /*
          @inline final def readLong(): Long =
            ((rngBytes(7) & 0xFFL) << 56) |
            ((rngBytes(6) & 0xFFL) << 48) |
            ((rngBytes(5) & 0xFFL) << 40) |
            ((rngBytes(4) & 0xFFL) << 32) |
            ((rngBytes(3) & 0xFFL) << 24) |
            ((rngBytes(2) & 0xFFL) << 16) |
            ((rngBytes(1) & 0xFFL) <<  8) |
            ((rngBytes(0) & 0xFFL))
             */

            @inline final def writeLong(l: Long): Unit = {
              buf(0) = ((l >> 56) & 0xFFL).toByte
              buf(1) = ((l >> 48) & 0xFFL).toByte
              buf(2) = ((l >> 40) & 0xFFL).toByte
              buf(3) = ((l >> 32) & 0xFFL).toByte
              buf(4) = ((l >> 24) & 0xFFL).toByte
              buf(5) = ((l >> 16) & 0xFFL).toByte
              buf(6) = ((l >> 8) & 0xFFL).toByte
              buf(7) = (l & 0xFFL).toByte
              buf(8) = rngBytes(0)
              buf(9) = rngBytes(1)
              buf(10) = rngBytes(2)
              buf(11) = rngBytes(3)
              buf(12) = rngBytes(4)
              buf(13) = rngBytes(5)
              buf(14) = rngBytes(6)
              buf(15) = rngBytes(7)
            }

            val next: F[String] =
              F.delay {
                val ts = (System.currentTimeMillis << 20)
                // 20 bits = 4 + 8 + 8
                val twentyBits = 0xFFFFF
                val cnt = (counter.incrementAndGet() & twentyBits).toLong
                rngBytes.synchronized {
                  secRand.nextBytes(rngBytes)
                  writeLong(ts | cnt)
                  // encode to base32, but drop all the = padding off
                  // the right, since we know this should be exactly
                  // 26 characters
                  b32.encodeToString(buf).dropRight(6)
                }
              }
          }
      }

}
