package ir.rkr.goldpan


import com.typesafe.config.ConfigFactory
import ir.rkr.goldpan.h2.H2Builder
import ir.rkr.goldpan.h2.H2Builder4Traffic
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.rest.JettyRestServer
import ir.rkr.goldpan.utils.GoldPanMetrics
import mu.KotlinLogging

const val version = 0.1


/**
 * GoldPan main entry point.
 */

fun main(args: Array<String>) {

    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val goldPanMetrics = GoldPanMetrics()
    val kafka = KafkaConnector(config.getString("kafka.topic"), config, goldPanMetrics)
    val h2 = H2Builder4Traffic(kafka, config, goldPanMetrics)

    JettyRestServer(h2, config, goldPanMetrics)

}