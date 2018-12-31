package ir.rkr.goldpan.h2

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.utils.GoldPanMetrics
import java.sql.Date
import java.sql.DriverManager
import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// {"userId":"1546149287270","stats":{"e":1545567471,"s":1545563871,"gr":4,"mr":4,"ms":0,"gp":0,"vc":0},"receivedTime":"Dec 30, 2018 9:25:06 AM"}

data class Stats(val s: Long, val e: Long, val gr: Int, val gp: Int, val mr: Int, val ms: Int, val vc: Int)
data class Events(val userId: String, val stats: Stats, val receivedTime: String)


class H2Builder(val kafka: KafkaConnector, config: Config, goldPanMetrics: GoldPanMetrics) {


//    val connection = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;mode=MySQL")
//    val statement1 = connection.createStatement()

    val config1 = HikariConfig().apply {
        jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;mode=MySQL"
        driverClassName = "org.h2.jdbcx.JdbcDataSource"
        maximumPoolSize = 50
    }



    val ds1 = HikariDataSource(config1)
    val con1 = ds1.connection
    val statement1 = con1.createStatement()

    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:00")
    val gson = GsonBuilder().disableHtmlEscaping().create()

//    val props = Properties()

    init {

        Class.forName("org.h2.jdbcx.JdbcDataSource").kotlin
//        props.setProperty("dataSourceClassName",  "org.h2.jdbcx.JdbcDataSource")

        val config1 = HikariConfig()
        config1.jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;mode=MySQL"
        config1.driverClassName = "org.h2.jdbcx.JdbcDataSource"
        config1.maximumPoolSize = 50

        val ds = HikariDataSource(config1)
        val con = ds.connection
        val statement = con.createStatement()

//        statement.executeUpdate("CREATE TABLE TEST(t TIMESTAMP DEFAULT 0  PRIMARY KEY, gr int , gp int , mr int , ms int , vc int );")
        statement.executeUpdate("CREATE TABLE TEST(t TIMESTAMP DEFAULT CURRENT_TIMESTAMP PRIMARY KEY, gr int , gp int , mr int , ms int , vc int );")
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            val events = kafka.get()
            if (events.size > 0) {
                events.forEach { t, u ->
                    val parsed = gson.fromJson(u, Events::class.java)
//                    val parsed = gson.fromJson(u, HashMap::class.java)
//                    parsed["stat"]["gr"]
//                    println(format.format( Date(parsed.stats.s ) ))
//                    statement.executeUpdate("INSERT into TEST(t,gr) values(${format.format( Date(parsed.stats.s *1000) )},${parsed.stats.gr}) ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr};")

                    if (parsed.stats.s < 15454658340L) {

                        val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s * 1000), ZoneId.systemDefault())
                        statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} )  " +
                                " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} ;")
                    }else {

                        val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s ), ZoneId.systemDefault())
                        statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} )  " +
                                " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} ;")
                    }
                }
                kafka.commit()
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }

    fun executeQuery(sql: String): ResultSet {
//        val statement = connection.createStatement()
        return statement1.executeQuery(sql)
    }

    fun executeUpdate(sql: String): Int {

        return statement1.executeUpdate(sql)
    }


}