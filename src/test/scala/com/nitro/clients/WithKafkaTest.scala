package com.nitro.clients

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import com.nitro.messages.NitroMeta
import org.scalacheck.Gen
import org.scalatest.FunSuite

import com.nitro.clients.kafka._

import scala.concurrent.Await
import scala.language.postfixOps

class WithKafkaTest extends FunSuite {

  import WithKafkaTest._

  test("test simple producer and consumer, no processing") {

    val nMessages = 30

    implicit val ic = new ImplicitContextN(ActorSystem("InOutTest"))
    try {

      val nitroMetaMessages =
        Gen.containerOfN[Vector, NitroMeta](
          nMessages,
          NitroMeta._arbitrary
        )
          .sample
          .get

      val received =
        KafkaUtilsN.withKafka { kafka =>

          import ic._

          Source(nitroMetaMessages)
            .runWith(kafka.produceGeneric[NitroMeta](nitroMetaTopic))

          import scala.concurrent.duration._
          Await.result(
            kafka.consumeGeneric[NitroMeta](nitroMetaTopic)
              .take(nitroMetaMessages.size)
              .runWith(Sink.fold(Seq.empty[NitroMeta])(_ ++ Seq(_))),
            5 seconds
          )
        }

      assert(received.size == nitroMetaMessages.size)

      nitroMetaMessages.zip(received)
        .foreach {
          case (expected, actual) => assert(expected == actual)
        }

    } finally {
      ic.as.shutdown()
      ic.mat.shutdown()
    }

  }

  test("test 1 type-changing processing step") {

    val nMessages = 30

    implicit val ic = new ImplicitContextN(ActorSystem("TypeChangeTest"))
    try {

      val nitroMetaMessages =
        Gen.containerOfN[Vector, NitroMeta](
          nMessages,
          NitroMeta._arbitrary
        )
          .sample
          .get

      val received =
        KafkaUtilsN.withKafka { kafka =>

          import ic._

          Source(nitroMetaMessages)
            .runWith(kafka.produceGeneric[NitroMeta](nitroMetaTopic))

          kafka.consumeGeneric[NitroMeta](nitroMetaTopic)
            .map(convert)
            .runWith(kafka.produceGeneric[SimpleMessage](simpleMsgTopic))

          import scala.concurrent.duration._
          Await.result(
            kafka.consumeGeneric[SimpleMessage](simpleMsgTopic)
              .take(nMessages)
              .runWith(Sink.fold(Seq.empty[SimpleMessage])(_ ++ Seq(_))),
            5 seconds
          )
        }

      assert(received.size == nMessages)

      val expecting = nitroMetaMessages.map(convert).toSet
      received
        .foreach { actual =>
          assert(expecting contains actual)
        }

    } finally {
      ic.as.shutdown()
      ic.mat.shutdown()
    }

  }

}

object WithKafkaTest {

  val nitroMetaTopic = "topic_nitro_meta"
  val simpleMsgTopic = "topic_simple_message"

  def convert(meta: NitroMeta): SimpleMessage =
    SimpleMessage(
      content = "",
      timestamp = meta.timestamp
    )

}
