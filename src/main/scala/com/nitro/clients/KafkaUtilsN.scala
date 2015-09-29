package com.nitro.clients

import java.io.IOException
import java.net.ServerSocket
import java.util
import util.Properties

import com.nitro.clients.kafka._

import akka.event.LoggingAdapter
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.helpers.NOPLogger

import scala.language.implicitConversions
import scala.util.{ Success, Try }

/**
 * Kafka configuration class for the conf macros
 */
case class KafkaConfigurationN(kafkaHost: String, zookeeperHost: String)

object KafkaConfigurationN {
  val local: KafkaConfigurationN =
    KafkaConfigurationN(
      kafkaHost = "localhost:9092",
      zookeeperHost = "localhost:2181"
    )
}

object KafkaUtilsN {

  /**
   * Configuration for running a Kafka-enabled function.
   */
  case class WithKafkaConf(props: Properties, log: Logger)

  object WithKafkaConf {

    /**
     * Empty properties and the no-op logger.
     */
    val empty: WithKafkaConf =
      WithKafkaConf(
        props = new Properties(),
        log = Logger(NOPLogger.NOP_LOGGER)
      )
  }

  /**
   * Executes a Kafka-enabled function. Stands up a Kafka and Zookeeper instance
   * to run the supplied function. Ensures that these services are shutdown when
   * this method returns.
   */
  def withKafka[T](
    kafkaFn: KafkaBaseN => T,
    kp:      WithKafkaConf  = WithKafkaConf.empty
  )(
    implicit
    ic: ImplicitContextN
  ): T = {

    // create the embedded Kafka and Zookeeper instances
    val embeddedZookeeper = new EmbeddedZookeeper(-1)
    val embeddedKafkaCluster = new EmbeddedKafkaCluster(
      embeddedZookeeper.getConnection,
      kp.props,
      {
        val kafkaPorts = new util.ArrayList[Integer]()
        // -1 for any available port
        kafkaPorts.add(-1)
        kafkaPorts
      }
    )

    // start up the Kafka + Zookeeper cluster
    embeddedZookeeper.startup()
    embeddedKafkaCluster.startup()

    // run the KafkaBase to T function
    try {
      import AdapterForTsLogger.Implicits._
      kafkaFn(
        new KafkaN(
          KafkaConfigurationN(
            kafkaHost = embeddedKafkaCluster.getBrokerList,
            zookeeperHost = embeddedZookeeper.getConnection
          ),
          kp.log
        )
      )

    } finally {
      // shutdown and destroy our system
      embeddedKafkaCluster.shutdown()
      embeddedZookeeper.shutdown()
    }
  }

  /**
   * Evaluates to Success of the input port iff port is negative.
   * Otherwise it uses findAvailablePort to get an unused one from the system.
   */
  def resolvePort(port: Int): Try[Int] =
    if (port <= -1)
      findAvailablePort()
    else
      Success(port)

  /**
   * Same thing as resolvePort, but calls get on the resulting Try. (For Java)
   */
  def unsafeResolvePort(port: Int): Int =
    resolvePort(port).get

  /**
   * Internally, creates a new ServerSocket to get a new, fresh, unused port.
   * If successful, it will evaluate to this port number. It always shuts-down
   * all state created within this method.
   */
  def findAvailablePort(): Try[Int] =
    Try {
      try {
        val socket = new ServerSocket(0)
        try {
          socket.getLocalPort
        } finally {
          if (socket != null) {
            socket.close()
          }
        }
      } catch {
        case e: IOException =>
          throw new IllegalStateException(
            s"Cannot find available port: ${e.getMessage}",
            e
          )
      }
    }

  /**
   * Same as findAvailablePort, but calls get on the resulting try. (For Java)
   */
  def unsafeFindAvailablePort: Int =
    findAvailablePort().get

  /**
   * An implicit conversion providing evidence that a Typesafe Logger adheres
   * to the Akka event Logging Adapter interface.
   */
  object AdapterForTsLogger {
    object Implicits {
      implicit def adaptLogger(l: Logger): LoggingAdapter =
        new LoggingAdapter {

          // logging methods

          override protected def notifyDebug(message: String): Unit =
            l.debug(message)

          override protected def notifyInfo(message: String): Unit =
            l.info(message)

          override protected def notifyWarning(message: String): Unit =
            l.warn(message)

          override protected def notifyError(message: String): Unit =
            l.error(message)

          override protected def notifyError(cause: Throwable, message: String): Unit =
            l.error(message, cause)

          // state checking methods

          override def isDebugEnabled: Boolean =
            l.underlying.isDebugEnabled

          override def isInfoEnabled: Boolean =
            l.underlying.isInfoEnabled

          override def isWarningEnabled: Boolean =
            l.underlying.isWarnEnabled

          override def isErrorEnabled: Boolean =
            l.underlying.isErrorEnabled
        }
    }
  }
}