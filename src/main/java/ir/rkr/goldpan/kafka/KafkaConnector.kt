package ir.rkr.goldpan.kafka

import com.typesafe.config.Config
import ir.rkr.goldpan.utils.GoldPanMetrics
import mu.KotlinLogging
import java.util.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.net.InetAddress
import java.time.Duration
import kotlin.collections.HashMap


class KafkaConnector(val topicName: String, config: Config, val goldPanMetrics: GoldPanMetrics) {

    val consumer: KafkaConsumer<ByteArray, ByteArray>
    val producer: KafkaProducer<ByteArray, ByteArray>
    private val logger = KotlinLogging.logger {}

    init {

        val hostName = InetAddress.getLocalHost().hostName

        val producerCfg = Properties()
        config.getObject("kafka.producer").forEach({ x, y -> println("$x --> $y"); producerCfg.put(x, y.unwrapped()) })
        producerCfg.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        producerCfg.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        producer = KafkaProducer(producerCfg)

        val consumerCfg = Properties()
        config.getObject("kafka.consumer").forEach({ x, y -> println("kafka config $x --> $y"); consumerCfg.put(x, y.unwrapped()) })
        consumerCfg.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        consumerCfg.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
//        consumerCfg.put("group.id", "${hostName}${System.currentTimeMillis()}")
//        consumerCfg.put("auto.offset.reset", "earliest")
//        consumerCfg.put("enable.auto.commit", "false")
        consumer = KafkaConsumer(consumerCfg)

        Thread.sleep(100)
    }

    fun get(): HashMap<String, String> {

//        goldPanMetrics.MarkKafkaGetCall(1)
        val msg = HashMap<String, String>()

        return try {

            consumer.subscribe(Collections.singletonList(topicName))
            val res = consumer.poll(Duration.ofMillis(700))
            res.records(topicName).forEach { it ->  msg[if (it.key() != null)  String(it.key()) else "${it.partition()}${it.offset()}"] = String(it.value())   }
            goldPanMetrics.MarkKafkaGetRecords(msg.size.toLong())
            msg

        } catch (e: java.lang.Exception) {
            println(e)
            goldPanMetrics.MarkKafkaGetFail(1)
            msg
        }
    }


    fun put(key: String, value: String): Boolean {

        goldPanMetrics.MarkKafkaPutCall(1)

        try {
            val res = producer.send(ProducerRecord(topicName, key.toByteArray(), value.toByteArray()), object : Callback {
                override fun onCompletion(p0: RecordMetadata?, p1: Exception?) {

                    if (p1 != null) {

                        logger.error { "key=$key value=$value" }
                    }
                }
            })

            if (res.isDone) {
                goldPanMetrics.MarkKafkaPutFail(1)
                return false
            }

            goldPanMetrics.MarkKafkaPutRecords(1)
            return true

        } catch (e: Exception) {
            goldPanMetrics.MarkKafkaPutFail(1)
            logger.error { "Error in try catch" }
            return false
        }
    }

    fun commit() {
        consumer.commitAsync()
    }

}