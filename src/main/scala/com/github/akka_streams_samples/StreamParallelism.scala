package com.github.akka_streams_samples

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


object StreamParallelism extends App {

  implicit val system = ActorSystem("stream-parallelism")
  implicit val materializer = ActorMaterializer()

  implicit val ec : ExecutionContext = ExecutionContext.Implicits.global

  trait Cake {
    val value : String = "Cake"
    def name() = println(value)
  }
  case class ScoopOfBatter(override val value :String = "ScoopOfBatter") extends Cake
  case class HalfCookedPancake(override val value :String = "HalfCookedPancake") extends Cake
  case class Pancake(override val value :String = "Pancake") extends Cake

  // Takes a scoop of batter and creates a pancake with one side cooked
  val fryingPan1: Flow[ScoopOfBatter, HalfCookedPancake, NotUsed] =
    Flow[ScoopOfBatter].map { batter => HalfCookedPancake() }

  // Finishes a half-cooked pancake
  val fryingPan2: Flow[HalfCookedPancake, Pancake, NotUsed] =
    Flow[HalfCookedPancake].map { halfCooked => Pancake() }

  // With the two frying pans we can fully cook pancakes
  val pancakeChef : Flow[ScoopOfBatter, Pancake, NotUsed] = Flow[ScoopOfBatter].via(fryingPan1.async).via(fryingPan2.async)
  Source
    .tick(0.seconds, 1.seconds, ScoopOfBatter)
    .map(sob => {s"${sob}"; sob})
    .toMat(Sink.ignore)(Keep.right)
    .run()
    .onComplete(_ => system.terminate)

}

