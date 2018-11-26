package com.github.akka_streams_samples

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source, ZipWith}

import scala.concurrent.ExecutionContext

object StreamRate {


  implicit val system = ActorSystem("stream-rate")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global

  /**
    * Internal Buffers
    */
  import scala.concurrent.duration._

  case class Tick()
  RunnableGraph.fromGraph( GraphDSL.create() { implicit  b =>
    import GraphDSL.Implicits._

    val zipper = b.add(ZipWith[Tick, Int, Int]((_, i) => i)
      .async //default to 16
      //      .addAttributes(Attributes.inputBuffer(1,1)) // reset to 1
    )
    Source.tick(initialDelay = 0.seconds, interval = 3.seconds, Tick()) ~> zipper.in0
    Source.tick(initialDelay = 1.seconds, interval = 1.seconds, "tick")
      .conflateWithSeed(seed = (_ => 1) )( (i,_) => i+1 )~> zipper.in1

    zipper.out ~> Sink.foreach(println)

    ClosedShape
  })
//    .run()


}
