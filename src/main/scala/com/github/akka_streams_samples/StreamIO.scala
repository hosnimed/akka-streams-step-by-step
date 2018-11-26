package com.github.akka_streams_samples

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn


object StreamIO extends App {

  implicit val system = ActorSystem("stream-io")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global

  val (host, port) = SocketUtil.temporaryServerHostnameAndPort()
  /*
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
*/
  val localhost = SocketUtil.temporaryServerAddress()
  val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] = Tcp().outgoingConnection(localhost)

  val myREPLParser: Flow[String, ByteString, NotUsed] = Flow[String]
      .takeWhile( _ != ":q")
      .concat(Source.single("Good bye!"))
      .map(cmd => ByteString(cmd.concat(System.lineSeparator())))

  val myREPL: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString]
    .via(Framing.delimiter(
      ByteString(System.lineSeparator()),
      256,
      true
    ))
    .map(_.utf8String)
    .map(text => println(s"Server : ${text}"))
    .map(_ => StdIn.readLine("> "))
    .via(myREPLParser)

  val connected: Future[OutgoingConnection] = connection.join(myREPL).run()
//  connected.onComplete(_ => system.terminate())



}

