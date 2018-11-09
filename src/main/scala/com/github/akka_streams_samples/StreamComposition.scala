package com.github.akka_streams_samples

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.pattern.FutureRef
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.{Future, Promise}

object StreamComposition {

  implicit val system = ActorSystem("stream-composition")
  implicit val materializer = ActorMaterializer()
  /**
    * Nested source
  */
  val source : Source[Int, Promise[Option[Int]]] = Source.maybe[Int]
  val flow1 : Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)
  val nestedSource : Source[Int, Promise[Option[Int]] ]   = source.viaMat(flow1)(Keep.left).named("nestedSource")

  /**
    * Nested Sink
  */
  /*Nested Flow*/
  val flowOfByeString : Flow[Int, ByteString, NotUsed] = Flow[Int].map(i => ByteString(i.toString))
  val flowOfTcpConnection : Flow[ByteString, ByteString, Future[OutgoingConnection]] = Tcp(system).outgoingConnection("localhost", 8080)
  val nestedFlow : Flow[Int, ByteString, Future[OutgoingConnection]] = flowOfByeString.viaMat(flowOfTcpConnection)(Keep.right)
  /*Simple Sink*/
  val sink: Sink[ByteString, Future[String]] = Sink.fold[String, ByteString]("")((s1, s2) => s1+s2.utf8String )
  /*Nested Sink*/
  val nestedSink: Sink[Int, (Future[OutgoingConnection], Future[String])] = nestedFlow.toMat(sink)(Keep.both).named("nestedSink")

  case class Combination (
                         private val p : Promise[Option[Int]],
                         private val connection: OutgoingConnection
                         )
  def combiner  (
                i : Promise[Option[Int]],
                pair : (Future[OutgoingConnection], Future[String])
                ): Future[Combination] = {
    val conn: Future[OutgoingConnection] = pair._1
    conn.map(f => Combination(i, f))
  }

  /**
    * Wrap all together : Materialize to Future[Combination]
    */
  val combinerGraph = nestedSource.toMat(nestedSink)(combiner)
}
