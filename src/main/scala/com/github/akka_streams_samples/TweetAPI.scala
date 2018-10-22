package com.github.akka_streams_samples

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}

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

 val graph = GraphDSLSample.graph
 graph.run()(materializer)
}