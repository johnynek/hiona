package dev.posco.hiona.aws

import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import java.io.{InputStream, OutputStream}
import io.circe.Json

class ToyLambda extends RequestStreamHandler {
  def handleRequest(
      inputStream: InputStream,
      outputStream: OutputStream,
      context: Context
  ): Unit = {
    val msg = s"toy response: ${System.currentTimeMillis()}"
    outputStream.write(Json.fromString(msg).spaces2.getBytes("UTF-8"))
  }
}
