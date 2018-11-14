package com.github.akka_streams_samples

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.ExecutionContext

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
    system.terminate()
  })
}
