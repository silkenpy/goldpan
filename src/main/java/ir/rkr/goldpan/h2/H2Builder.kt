package ir.rkr.goldpan.h2

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.utils.GoldPanMetrics
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Supplier


// {"userId":"1546149287270","stats":{"e":1545567471,"s":1545563871,"gr":4,"mr":4,"ms":0,"gp":0,"vc":0},"receivedTime":"Dec 30, 2018 9:25:06 AM"}


class H2Builder(val kafka: KafkaConnector, config: Config, goldPanMetrics: GoldPanMetrics) {


    data class Stats(val s: Long, val e: Long, val gr: Int, val gp: Int, val mr: Int, val ms: Int, val vc: Int)
    data class Events(val userId: String, val stats: Stats, val receivedTime: String)

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
//    val engine = KotlinJsr223JvmLocalScriptEngineFactory().scriptEngine


    init {

//        engine.eval("data class Stats(val s: Long, val e: Long, val gr: Int, val gp: Int, val mr: Int, val ms: Int, val vc: Int)")
//        engine.eval("data class Events(val userId: String, val stats: Stats, val receivedTime: String)")

//        Class.forName("org.h2.jdbcx.JdbcDataSource").kotlin
        val config1 = HikariConfig()
        config1.jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;mode=MySQL"
        config1.driverClassName = "org.h2.jdbcx.JdbcDataSource"
        config1.maximumPoolSize = 50

        val ds = HikariDataSource(config1)
        val con = ds.connection
        val statement = con.createStatement()


        val cache = Caffeine.newBuilder().maximumSize(40000000L).build<Long, Long>()
        goldPanMetrics.addGauge("CaffeineEstimatedSize", Supplier { cache.estimatedSize() })

        statement.executeUpdate("CREATE TABLE TEST(t TIMESTAMP DEFAULT CURRENT_TIMESTAMP PRIMARY KEY, gr int , gp int , mr int , ms int , vc int, count int );")
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            val events = kafka.get()
            if (events.size > 0) {
                events.forEach { t, u ->

                    val parsed = gson.fromJson(u, Events::class.java)
                    val userId = parsed.userId.toLong()
                    val lastTime = cache.getIfPresent(userId) ?: 0

                    if (userId < Int.MAX_VALUE.toLong()) {
                        if (lastTime < parsed.stats.e) {
                            cache.put(userId, parsed.stats.e)
//                            if (parsed.stats.gp > 0L || parsed.stats.gr > 0L || parsed.stats.mr > 0L || parsed.stats.ms > 0L || parsed.stats.vc > 0L) {
                            if (parsed.stats.gp > 0L || parsed.stats.gr > 0L || parsed.stats.mr > 0L || parsed.stats.ms > 0L) {
                                if (parsed.stats.s < 15454658340L) {

                                    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s * 1000), ZoneId.systemDefault())
                                    statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc,count) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} , 1 )  " +
                                            " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} , count=count+1 ;")
                                } else {

                                    val dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(parsed.stats.s), ZoneId.systemDefault())
                                    statement.executeUpdate("INSERT into TEST(t,gr,gp,mr,ms,vc,count) values('${dateTime.minusMinutes(dateTime.minute % 10L).format(format)}', ${parsed.stats.gr} , ${parsed.stats.gp} , ${parsed.stats.mr} , ${parsed.stats.ms} , ${parsed.stats.vc} , 1 )  " +
                                            " ON DUPLICATE KEY UPDATE gr=gr+${parsed.stats.gr} ,  gp=gp+${parsed.stats.gp} ,  mr=mr+${parsed.stats.mr} ,  ms=ms+${parsed.stats.ms} , vc=vc+${parsed.stats.vc} , count=count+1 ;")
                                }
                            }
                        } else
                            goldPanMetrics.MarkDuplicateRecords(1)
                    }else
                        goldPanMetrics.MarkInvalidUser(1)
                }
                kafka.commit()
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