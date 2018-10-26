package com.github.akka_streams_samples

import java.util.concurrent.atomic.AtomicInteger

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorSystem, Cancellable, Identify}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import com.github.akka_streams_samples.Main.materializer

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

object TweetAPI extends App {

  final case class Author(name: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] = body.split(" ").collect {
      case t if t.startsWith("#") => Hashtag(t.replaceAll("[^#\\w]", ""))
    }.toSet
  }

  val akkaTag = Hashtag("#akka")

  val tweets: Source[Tweet, NotUsed] = Source(
      Tweet(Author("melhosni"), System.currentTimeMillis(), "awesome #akka")::
      Tweet(Author("rolandkuhn"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("patriknw"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("bantonsson"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("drewhk"), System.currentTimeMillis, "#akka !") ::
      Tweet(Author("ktosopl"), System.currentTimeMillis, "#akka on the rocks!") ::
      Tweet(Author("mmartynas"), System.currentTimeMillis, "wow #akka !") ::
      Tweet(Author("akkateam"), System.currentTimeMillis, "#akka rocks!") ::
      Tweet(Author("bananaman"), System.currentTimeMillis, "#bananas rock!") ::
      Tweet(Author("appleman"), System.currentTimeMillis, "#apples rock!") ::
      Tweet(Author("drama"), System.currentTimeMillis, "we compared #apples to #oranges!") ::
      Nil
  )

  implicit val system: ActorSystem = ActorSystem("akka-tweets")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def handleTags (tag: Hashtag): Future[String] = Future {tag.name.toUpperCase}
  def showTags (t: String): Future[Unit] = Future { println(t)}
/*
  val graph: RunnableGraph[NotUsed] = tweets.map(t => t.hashtags)
    .reduce((s1,s2) => s1++s2)
    .mapConcat(x=>x)
    .mapAsyncUnordered(4)(handleTags)
    .to(Sink.foreachAsync(4)(showTags))*/

/* graph.run()(materializer)*/

  //#tweets-slow-consumption-dropHead
  def heavyComputation(tweet : Tweet): Future[String] = Future{
    Thread.sleep(500)
    tweet.author.name.toStream.collect{
      case c if c.toInt > 65 => c.toUpper
    }.mkString
  }
  def slowComputation(tweet : Tweet): Flow[Tweet, Author, NotUsed] = Flow[Tweet].map(t => t.author)

  def foreach[T](f: T ⇒ Unit): Sink[T, Future[Done]] =
    Flow[T]
      .map(f)
      .toMat(Sink.ignore)(Keep.right)

  def printAuthor(): Sink[Author, NotUsed] =
    Flow[Author]
        .to(foreach(a => println(a.name)))

  val flow: Flow[Tweet, Author, NotUsed] =
    Flow[Tweet]
    .map(t=>{
     Author(t.author.name.toUpperCase)
      })
      .filterNot(a => a.name.startsWith("M"))

  val sink: Sink[Author, Future[Done]] = Sink.foreach(a =>println(a.name))

  val source: Source[Author, NotUsed] = tweets.via(flow)

  val runnable1 :RunnableGraph[Future[Done]] = source.toMat(sink)(Keep.right)

  val runnable2 : RunnableGraph[NotUsed] =
    source
      .buffer(10 , OverflowStrategy.dropHead)
      .toMat(printAuthor)(Keep.right)

  //  done.onComplete(_ => system.terminate())
  //  val done = runnable2.run()

  /**
    * defining asynchronous boundaries
    */
  def add(x: Int, y: Int) = {
    Thread.sleep(1000)
    x + y
  }
  def mult(x: Int, y: Int) = {
    Thread.sleep(1000)
    x * y
  }
   val runnable3 : RunnableGraph[Future[Int]]=
    Source(List(1,2,3,4,5,6,7,8,9))
      .map(x => add(x, 10)).async
      .map(x => mult(x, 2))
      .toMat(Sink.reduce((x,y)=>add(x,y)))(Keep.right)
    val runnable4 : RunnableGraph[Future[Int]]=
    Source(List(1,2,3,4,5,6,7,8,9))
      .map(x => add(x, 10))
      .map(x => mult(x, 2))
      .toMat(Sink.reduce((x,y)=>add(x,y)))(Keep.right)

 /* var start = System.currentTimeMillis()
  var finish = 0L
  runnable3.run().onComplete(r =>{
    println(s"RunnableGraph 3 :  ${r}")
    finish = System.currentTimeMillis()
    val duration1 = Duration.fromNanos(finish - start)
    println(duration1)
    system.terminate()
  })
  start = System.currentTimeMillis()
  finish = 0L
  runnable4.run().onComplete(r =>{
    println(s"RunnableGraph 4 :  ${r}")
    finish = System.currentTimeMillis()
    val duration2 = Duration.fromNanos(finish - start)
    println(duration2)
    system.terminate()
  })*/

  //# PreMaterialized  Source
  val preMaterializedValues: Source[String, ActorRef] = Source.actorRef[String](bufferSize = 10, overflowStrategy = OverflowStrategy.fail)
  val (actorRef, sourcePreMaterialized) = preMaterializedValues.preMaterialize()
  actorRef ! "Start!"
  sourcePreMaterialized.runWith(Sink.foreach(println)).onComplete(_ => system.terminate())
}