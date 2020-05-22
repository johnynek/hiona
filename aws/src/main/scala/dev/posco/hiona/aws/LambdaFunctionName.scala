package dev.posco.hiona.aws

import io.circe.{Decoder, Encoder}

case class LambdaFunctionName(asString: String)

object LambdaFunctionName {
  implicit val decoderLambdaFunctionName: Decoder[LambdaFunctionName] =
    Decoder[String].map(LambdaFunctionName(_))

  implicit val encoderLambdaFunctionName: Encoder[LambdaFunctionName] =
    Encoder[String].contramap[LambdaFunctionName](_.asString)
}
