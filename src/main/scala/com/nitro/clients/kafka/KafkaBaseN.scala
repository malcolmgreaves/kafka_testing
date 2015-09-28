package com.nitro.clients.kafka

import java.io.ByteArrayOutputStream
import java.util.UUID

import akka.event.LoggingAdapter
import akka.stream.scaladsl.{ Source, Sink }
import com.nitro.scalaAvro.runtime.{ GeneratedMessageCompanion, Message, GeneratedMessage }
import com.softwaremill.react.kafka.ReactiveKafka
import kafka.serializer.{ Decoder, Encoder }
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord, GenericDatumWriter }
import org.apache.avro.io.{ DecoderFactory, EncoderFactory }

import scala.util.Try
import scala.util.control.NonFatal

trait KafkaBaseN {

  def produceGeneric[T <: GeneratedMessage with Message[T]](
    topic:   String,
    groupId: String = UUID.randomUUID().toString
  )(
    implicit
    companion: GeneratedMessageCompanion[T]
  ): Sink[T, Unit]

  def consumeGeneric[T <: GeneratedMessage with Message[T]](
    topic:   String,
    groupId: String = UUID.randomUUID().toString
  )(
    implicit
    companion: GeneratedMessageCompanion[T]
  ): Source[T, Unit]
}

class KafkaN(
    kafkaConfiguration: KafkaConfiguration,
    logger:             LoggingAdapter
)(
    implicit
    implicitContext: ImplicitContext
) extends KafkaBaseN() {

  import implicitContext._

  private lazy val kafka = new ReactiveKafka(
    host = kafkaConfiguration.kafkaHost,
    zooKeeperHost = kafkaConfiguration.zookeeperHost
  )

  override def produceGeneric[T <: GeneratedMessage with Message[T]](
    topic:   String,
    groupId: String = UUID.randomUUID.toString
  )(
    implicit
    companion: GeneratedMessageCompanion[T]
  ) =
    Sink(kafka.publish(topic, UUID.randomUUID().toString, new Encoder[T] {
      override def toBytes(t: T): Array[Byte] = {
        println(s"sending to kafka topic $topic")
        AvroMessageEncoder.encode(logger)(t)(companion)
      }
    }))

  override def consumeGeneric[T <: GeneratedMessage with Message[T]](
    topic:   String,
    groupId: String = UUID.randomUUID.toString
  )(
    implicit
    companion: GeneratedMessageCompanion[T]
  ) =
    Source(kafka.consume(topic, UUID.randomUUID().toString, new Decoder[T] {
      override def fromBytes(bytes: Array[Byte]): T = {
        val res = AvroMessageEncoder.decode(logger)(companion)(bytes)
        println(s"read from kafka topic $topic")
        res
      }
    }))
}

object AvroMessageEncoder {

  import language.postfixOps
  def encode[T <: GeneratedMessage with Message[T]](
    logger: LoggingAdapter
  )(
    message: T
  )(
    implicit
    companion: GeneratedMessageCompanion[T]
  ): Array[Byte] =
    Try {
      val baos = new ByteArrayOutputStream()
      println("before encoder")
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      println("before writer")
      val writer = new GenericDatumWriter[GenericRecord](companion.schema)
      println("before toMutable")
      val gen = message.toMutable
      println("before write")
      writer.write(gen, encoder)
      encoder.flush()
      println("before byte array")
      baos.toByteArray

    } recover {

      case NonFatal(t: Throwable) =>
        println(s"Error encoding ${t.getMessage}")
        throw t

      case unk =>
        println(s"Cannot handle: ${unk.getMessage}")
        throw unk

    } get

  def decode[T <: GeneratedMessage with Message[T]](
    logger: LoggingAdapter
  )(
    companion: GeneratedMessageCompanion[T]
  )(
    bytes: Array[Byte]
  ): T =
    Try {
      println("before decoder")
      val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      println("before reader")
      val reader = new GenericDatumReader[GenericRecord](companion.schema)
      println("before read")
      val r = reader.read(null, decoder)
      println("before fromMutable")
      companion.fromMutable(r)

    } recover {

      case NonFatal(t: Throwable) =>
        println(s"Error decoding message: ${t.getMessage}")
        throw t

      case unk =>
        println(s"Cannot handle: ${unk.getMessage}")
        throw unk

    } get
}