package com.github.akka_streams_samples

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, IOResult, OverflowStrategy}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.github.akka_streams_samples.TweetAPI.Author

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

object Main extends App {
  //#create-materializer
  implicit val system = ActorSystem("step-by-step")
  implicit val materializer = ActorMaterializer()
  //#create-materializer

  //#create-source
  val source : Source[Int, NotUsed] = Source(0 to 100)
  //#create-source
  //#run-source
  val done: Future[Done] = source.runForeach(println)(materializer)
  //#run-source

  //#transform-source
  val factorials = source.scan(BigInt(1))((acc, next) ⇒ acc * next)

  val result: Future[IOResult] =
    factorials
      .map(num ⇒ ByteString(s"$num\n"))
      .runWith(FileIO.toPath(Paths.get("factorials.txt")))
  //#transform-source

  //#use-transformed-sink
  factorials.map(_.toString).runWith(lineSink("factorial2.txt"))
  //#use-transformed-sink

 //#add-streams
  factorials
    .zipWith(Source(0 to 100))((num, idx) ⇒ s"$idx! = $num")
    .throttle(1, FiniteDuration(1, TimeUnit.SECONDS))
    //#add-streams
    .take(10)
    //#add-streams
//    .runForeach(s=>println(s"Factorials = $s"))
    .map(_.toString)
    .runWith(lineSink("factorial2.txt"))
  //#add-streams

  //#run-source-and-terminate
  implicit val ec : ExecutionContext = system.dispatcher
  done.onComplete(_ => system.terminate)
  //#run-source-and-terminate

  println(s"Done.Value = ${done.value}")

  //#transform-sink
  def lineSink(filename: String): Sink[String, Future[IOResult]] =
    Flow[String]
        .map(s ⇒ ByteString(s.concat(System.lineSeparator())))
        .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)
  //#transform-sink
}