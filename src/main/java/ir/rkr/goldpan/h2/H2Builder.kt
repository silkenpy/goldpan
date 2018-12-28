package ir.rkr.goldpan.h2

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.utils.GoldPanMetrics
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.swing.text.html.Option

data class Events(val name: String, val id: Int)


class H2Builder(val kafka: KafkaConnector, config: Config, goldPanMetrics: GoldPanMetrics) {

//    val connection : Connection
//    var statement= object : Statement


    val connection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;")
    val statement = connection.createStatement()
    val gson = GsonBuilder().disableHtmlEscaping().create()

    init {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            val events = kafka.get()
            if (events.size > 0) {
                events.forEach { t, u ->
                    val parsed = gson.fromJson(u, Events::class.java)
                    statement.executeUpdate("INSERT into  TEST(ID,NAME) values(${parsed.id},'${parsed.name}');")
                }
                kafka.commit()
            }
        }, 0, 100, TimeUnit.MILLISECONDS)

    }

    fun executeQuery(sql: String): ResultSet {
          return   statement.executeQuery(sql)
    }

    fun executeUpdate(sql: String): Int {

            return statement.executeUpdate(sql)


    }




}