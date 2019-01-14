package ir.rkr.goldpan.h2

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.utils.GoldPanMetrics
import ir.rkr.goldpan.utils.fromJson
import mu.KotlinLogging
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// {"userId":"1546149287270","stats":{"e":1545567471,"s":1545563871,"gr":4,"mr":4,"ms":0,"gp":0,"vc":0},"receivedTime":"Dec 30, 2018 9:25:06 AM"}


class H2Builder4Traffic(val kafka: KafkaConnector, config: Config, goldPanMetrics: GoldPanMetrics) {

    data class Media(val bytesReceived: Int, val bytesSent: Int, val mediaType: Int, val received: Int, val sent: Int, val totalTime: Int)

    data class Schema(val carrierName: String, val mediaUsageStatistics: List<Media>, val time: Long, val type: Int)

    data class Records(val userId: Long, val userFullName: String, val userAgent: String, val appId: Int,
                       val appVersion: Int, val apiVersion: Int, val version: Int, val info: String, val type: String)


    val config1 = HikariConfig().apply {
        jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;mode=MySQL"
//        driverClassName = "org.h2.jdbcx.JdbcDataSource"
        maximumPoolSize = 50
    }
    val logger = KotlinLogging.logger {}

    val ds1 = HikariDataSource(config1)
    val con1 = ds1.connection
    val statement1 = con1.createStatement()

    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00")
    val gson = GsonBuilder().disableHtmlEscaping().create()


    init {
        val ds = HikariDataSource(config1)
        val con = ds.connection
        val statement = con.createStatement()
//        Class.forName("org.h2.jdbcx.JdbcDataSource").kotlin
        statement.executeUpdate("CREATE TABLE TRAFFIC(id BIGINT, net INT, mediaType INT, sent BIGINT, received BIGINT, sentByte BIGINT," +
                " receivedByte BIGINT, sentPerDay BIGINT, receivedPerDay BIGINT, t TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(id, net, mediaType) );")
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            try {
                val events = kafka.get()
                if (events.size > 0) {
                    events.forEach { t, u ->

                        val parsed = gson.fromJson(u, Records::class.java)

                        val id = parsed.userId
                        val info = gson.fromJson<@JvmSuppressWildcards List<Schema>>(parsed.info)

                        for (i in 0..2) {
                            val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(info[i].time), ZoneId.systemDefault())
                            val t = dateTime.minusMinutes(dateTime.minute % 10L).format(format)

                            for (j in 0..6) {

                                val net = i
                                val mediaType = j
//                                val t = info[i].time

                                val sent = info[i].mediaUsageStatistics[j].sent
                                val sentByte = info[i].mediaUsageStatistics[j].bytesSent
                                val sentPerDay = info[i].mediaUsageStatistics[j].bytesSent / ((System.currentTimeMillis() - info[i].time) / (3600L * 1000L * 24L))

                                val received = info[i].mediaUsageStatistics[j].received
                                val receivedByte = info[i].mediaUsageStatistics[j].bytesReceived
                                val receivedPerDay = info[i].mediaUsageStatistics[j].bytesReceived / ((System.currentTimeMillis() - info[i].time) / (3600L * 1000L * 24L))
                                statement.executeUpdate("INSERT into TRAFFIC(id, net, mediaType, sent, received, sentByte, receivedByte, sentPerDay, receivedPerDay, t ) " +
                                        "values($id, $net, $mediaType, $sent, $received, $sentByte, $receivedByte, $sentPerDay, $receivedPerDay , \'$t\' )    " +
                                        "ON DUPLICATE KEY UPDATE sent=$sent, received=$received, sentByte=$sentByte, receivedByte=$receivedByte, sentPerDay=$sentPerDay, receivedPerDay=$receivedPerDay, t=\'$t\' ; ")
                            }
                        }


//                        statement.executeUpdate("INSERT into TRAFFIC(id,sent,received ) " +
//                                "values('${parsed.userId}', ${totalSent} , ${totalReceived})  " +
//                                "  ON DUPLICATE KEY UPDATE sent=${totalSent} ,received= ${totalReceived} ;")
//
//
//                        val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s * 1000), ZoneId.systemDefault())
//                        statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} )  " +
//                                " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} ;")
//                    } else {
//
//                        val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s), ZoneId.systemDefault())
//                        statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} )  " +
//                                " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} ;")
//                    }
                    }
                    kafka.commit()
                }
            } catch (e: Exception) {
                logger.error { e }
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }

    fun executeQuery(sql: String): ResultSet {
        return statement1.executeQuery(sql)
    }

    fun executeUpdate(sql: String): Int {

        return statement1.executeUpdate(sql)
    }


}