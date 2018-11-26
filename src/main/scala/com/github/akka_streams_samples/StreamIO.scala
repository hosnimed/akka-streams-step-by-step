package com.github.akka_streams_samples

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.util.{ByteString, Timeout}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}


object StreamIO extends App {

  implicit val system = ActorSystem("stream-io")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global

  private val host = "127.0.0.1"
  private val port = 8888
  val connections : Source[IncomingConnection, Future[ServerBinding]] = Tcp().bind(host, port)
//  val binding : Future[ServerBinding] = connections.to(Sink.ignore).run()

  connections runForeach { connection =>
    println(s"New connection received from ${connection.remoteAddress}")
    val echoServer: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString]
      .via(Framing.delimiter(
        delimiter = ByteString("\n"),//ByteString(System.lineSeparator()),
        maximumFrameLength = 256,
        allowTruncation = true)
      )
      .map(_.utf8String)
      .map(_.concat(s"from ${connection.localAddress} !!!").concat("\n")) //concat(System.lineSeparator()))
      .map(ByteString(_))

    connection.handleWith(echoServer)
  }

}
