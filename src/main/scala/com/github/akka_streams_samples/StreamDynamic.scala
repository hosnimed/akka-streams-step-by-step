package com.github.akka_streams_samples

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Cancellable}
import akka.io.Udp.SO.Broadcast
import akka.stream._
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, RunnableGraph, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}

object StreamDynamic extends App {


  implicit val system = ActorSystem("stream-dynamic")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  /**
    * Single KillSwitch
    */
  val src = Source(Stream.from(1))
    .delay(1.seconds, DelayOverflowStrategy.backpressure)
  val snk = Sink.last[Int]

  val singleKillSwitch = KillSwitches.single[Int]
  val (killSwitch, lastInt) = src
    .viaMat(singleKillSwitch)(Keep.right)
    .toMat(snk)(Keep.both)
    .run()
  /**
    * So something before sending shutdown request
    */
  Thread.sleep(3000)

  killSwitch.shutdown()
//  killSwitch.abort(new RuntimeException("Fatal error!"))
  lastInt.onComplete(d => {
    print(s"single-kill-switch : ${d}")
  })
  /**
    * Shared KillSwitch
    */
  val sharedKillSwitch = KillSwitches.shared("shared-kill-switch")
  val last = src
    .via(sharedKillSwitch.flow)
    .toMat(snk)(Keep.right)
    .run()

  val lastDelayed = src.delay(1.seconds, DelayOverflowStrategy.backpressure)
    .via(sharedKillSwitch.flow)
    .toMat(snk)(Keep.right)
    .run()
  Thread.sleep(3000)

  sharedKillSwitch.shutdown()
  last.onComplete(d => {
    print(s"shared-kill-switch : ${d}")
  })
  lastDelayed.onComplete(d => {
    print(s"shared-kill-switch delayed :${d}")
//    system.terminate()
  })

  /**
    * *****Dynamic fan-in and fan-out **********
    */

  /**
    * 01 - Using the MergeHub
    */
  // prepare consumer
  val consumer: Sink[String, Future[Done]] = Sink.foreach[String](println)
  //  prepare th graph : attach the source of MH to consumer
  val consumerDynamicGraph: RunnableGraph[Sink[String, NotUsed]] = MergeHub.source[String](perProducerBufferSize = 16).to(consumer)
  // run/materialize the graph
  val toConsumer: Sink[String, NotUsed] = consumerDynamicGraph.run()
  // attach dynamic producer
  println("01 - Using the MergeHub")
  Source('A' to 'M').map(_.toString).toMat(toConsumer)(Keep.right)
    .run()
  Source('N' to 'Z').map(_.toString).toMat(toConsumer)(Keep.right)
    .run()

  /**
    * 02 - Using the BroadcastHub
    */
  // prepare producer
  val producer: Source[String, Cancellable] = Source.tick(1.seconds, 1.seconds, "Msg")
  //  prepare th graph : attach the BH to the producer
  val producerDynamicGraph: RunnableGraph[Source[String, NotUsed]] = producer.toMat(BroadcastHub.sink[String](bufferSize = math.pow(2,0).toInt))(Keep.right)
  // run/materialize the graph
  val fromProducer: Source[String, NotUsed] = producerDynamicGraph.run()
  // attach dynamic consumer
  println("02 - Using the BroadcastHub")
  fromProducer.zipWith(Source(1 to 10))(Keep.both)
    .toMat(Sink.foreach(println))(Keep.right)
    .run()
  fromProducer.zipWith(Source(11 to 20))(Keep.both)
//    .delay(10.seconds, OverflowStrategy.dropHead)
    .toMat(Sink.foreach(println))(Keep.right)
    .run()
    .onComplete(_ => system.terminate)

}
