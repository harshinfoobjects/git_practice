package com.rockwell.twb.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.KafkaProducer


/**
 * KafkaSink contains kafka producer api
 */
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()

  /**
   * send given message to kafka topic
   *
   *  @param msg message in KeyedMessage format which needs to be written
   *  @param readUrl gateway url
   */
  //def send(msg: ProducerRecord[String, String]): Unit = producer.send(msg).get()
  
  def send(msg: ProducerRecord[String, String]): Unit = producer.send(msg)
}

/**
 * KafkaSink companion object
 */
object KafkaSink {
  /**
   * apply method to return KafkaSink object.
   *
   *  @param config Kafka configuration properties
   *  
   *  @return KafkaSink object
   */
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}