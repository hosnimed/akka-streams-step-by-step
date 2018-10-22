package com.github.akka_streams_samples

import akka.{Done, NotUsed}
import akka.stream.ClosedShape
import akka.stream.scaladsl._
import com.github.akka_streams_samples.TweetAPI._

import scala.concurrent.Future

object GraphDSLSample {

  object HiddenDefinitions {
    //#graph-dsl-broadcast
    val writeAuthors: Sink[Author, Future[Done]] = Sink.foreach(author => print(s"Author ${author.name} \n"))
    val writeHashtags: Sink[List[Hashtag], Future[Done]] =Sink.foreach(tags => {
      val allTags =  tags.map(tag => tag.name).reduce(_.concat("\t").concat(_))
      print(s"All Tags: ${allTags}\n")
      })
      //#graph-dsl-broadcast
  }


  val tweets = TweetAPI.tweets;

  val graph: RunnableGraph[NotUsed] = RunnableGraph.fromGraph( GraphDSL.create() { implicit graphBuilder =>
    import GraphDSL.Implicits._
    //#junction (fan-out/fan-in)
    val builderCast = graphBuilder.add(Broadcast[TweetAPI.Tweet](2))
    tweets ~> builderCast.in

    builderCast.out(0) ~> Flow[TweetAPI.Tweet].map(_.author) ~> HiddenDefinitions.writeAuthors
    builderCast.out(1) ~> Flow[TweetAPI.Tweet].map(_.hashtags.toList) ~> HiddenDefinitions.writeHashtags


    ClosedShape
  })

}
