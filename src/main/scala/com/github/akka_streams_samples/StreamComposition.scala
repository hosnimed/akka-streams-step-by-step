package com.github.akka_streams_samples

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.pattern.FutureRef
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future, Promise}

object StreamComposition extends App{

  implicit val system = ActorSystem("stream-composition")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
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
  val combinerGraph: RunnableGraph[Future[Combination]] = nestedSource.toMat(nestedSink)(combiner)
 /* combinerGraph.run().onComplete(d => {
    print(d)
    system.terminate()
  })*/

  /**
    * Attributes
  */
    import Attributes._
  val nSrc = Source(1 to 3).map(_ * 2).named("nSrc") //no input buffer set
  val nFlw = Flow[Int].filter( _ % 2 == 0)
    .via(Flow[Int].map( _ - 1).withAttributes(inputBuffer(4, 4))) // nested flow with input buffer
    .named("nFlw") // no input buffer
  val nSnk: Sink[Int, Future[Int]] = nFlw.toMat(Sink.fold[Int, Int](0)(_ + _))(Keep.right).withAttributes(name("nSnk") and inputBuffer(2,2))
  nSrc.via(nFlw).toMat(nSnk)(Keep.right)
    .run()
    .onComplete(d => {
      print(d)
      system.terminate()
    })
}
