package ir.rkr.goldpan


import com.google.gson.GsonBuilder
import com.typesafe.config.ConfigFactory
import ir.rkr.goldpan.h2.H2Builder
import ir.rkr.goldpan.kafka.KafkaConnector
import ir.rkr.goldpan.rest.JettyRestServer
import ir.rkr.goldpan.utils.GoldPanMetrics
import mu.KotlinLogging
import org.json.JSONObject


const val version = 0.1


/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {

    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val goldPanMetrics = GoldPanMetrics()
    val kafka = KafkaConnector(config.getString("kafka.topic"), config, goldPanMetrics)
    val h2 = H2Builder(kafka, config, goldPanMetrics)
    val gson = GsonBuilder().disableHtmlEscaping().create()

//    h2.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));")
//    data class Stats(val s: Long, val e: Long, val gr: Int, val gp: Int, val mr: Int, val ms: Int, val vc: Int)
//    h2.executeUpdate("CREATE TABLE CSTATS(ID INT PRIMARY KEY, NAME VARCHAR(255));")
    JettyRestServer(h2, config, goldPanMetrics)

    Thread.sleep(2000)

//    while (true) {
////        val rs = h2.executeQuery("select * from TEST where t='2018-12-25 01:16:00.0' ;")
//        val rs = h2.executeQuery("select * from TEST  ;")
//        while (rs.next()) {
//            val numColumns = rs.metaData.getColumnCount()
//            val obj = JSONObject()
//
//            for (i in 1 until numColumns + 1) {
//                val column_name = rs.metaData.getColumnName(i)
//
//                when (rs.metaData.getColumnType(i)) {
//                    java.sql.Types.ARRAY -> obj.put(column_name, rs.getArray(column_name))
//                    java.sql.Types.BIGINT -> obj.put(column_name, rs.getInt(column_name))
//                    java.sql.Types.BOOLEAN -> obj.put(column_name, rs.getBoolean(column_name))
//                    java.sql.Types.BLOB -> obj.put(column_name, rs.getBlob(column_name))
//                    java.sql.Types.DOUBLE -> obj.put(column_name, rs.getDouble(column_name))
//                    java.sql.Types.FLOAT -> obj.put(column_name, rs.getFloat(column_name))
//                    java.sql.Types.INTEGER -> obj.put(column_name, rs.getInt(column_name))
//                    java.sql.Types.NVARCHAR -> obj.put(column_name, rs.getNString(column_name))
//                    java.sql.Types.VARCHAR -> obj.put(column_name, rs.getString(column_name))
//                    java.sql.Types.TINYINT -> obj.put(column_name, rs.getInt(column_name))
//                    java.sql.Types.SMALLINT -> obj.put(column_name, rs.getInt(column_name))
//                    java.sql.Types.DATE -> obj.put(column_name, rs.getDate(column_name))
//                    java.sql.Types.TIMESTAMP -> obj.put(column_name, rs.getTimestamp(column_name))
//                    else -> obj.put(column_name, rs.getObject(column_name))
//                }
//            }
//            println(obj.toString())
//        }
//
//        Thread.sleep(10000)
//    }
}
//    while (true) {
//
//        val res = h2.executeQuery("select * from TEST where t='2018-12-25 01:16:00.0' ;")
//        while (res.next())
//            println(res)
//
//
////            for (i in 1..(res.metaData.columnCount)) {
////                println(res.getTimestamp("t").toString() + ", " + res.getInt("gr").toString() +", "
////                            + res.getInt("gp").toString()+ ", " + res.getInt("mr").toString() + ", "
////                            + res.getInt("ms").toString() + ", " + res.getInt("vc").toString())
////            }
//
//            println("")
//        Thread.sleep(1000)
//    }



//    val logger = KotlinLogging.logger {}
//    val config = ConfigFactory.defaultApplication()

//
//    val server = Server.createTcpServer("-tcpPort", "6969", "-tcpAllowOthers", "-tcpDaemon")
//
//    server.start()

//    Class.forName("org.h2.jdbcx.JdbcDataSource").kotlin
//    val props = Properties()
//    props.setProperty("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource")
//    props.setProperty("dataSource.user", "test")
//    props.setProperty("dataSource.password", "test")
//    props.setProperty("dataSource.databaseName", "mydb")
//    props.put("dataSource.logWriter", PrintWriter(System.out))
//
//    val config = HikariConfig(props)
//    config.jdbcUrl="jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;"
//
//    val ds = HikariDataSource(config)
//    val con =  ds.connection
//    val st = con.createStatement()

////    println(con.prepareStatement("show tables").execute().toString())
////    val con = DriverManager.getConnection("jdbc:h2:mem:test")
//    val con = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;")
////    val con = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:6969/mem:db1;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;")
////    val con = DriverManager.getConnection("jdbc:hsqldb:mem:test;")
////    Class.forName("org.sqlite.JDBC")
////
////    val con = DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=shared")
//
//    val st = con.createStatement()
//    // println(st.executeUpdate("CREATE SCHEMA ali;").toString())
//    println(st.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));"))
////    println(st.executeUpdate("INSERT into  ali.TEST(ID,NAME) values(1,'ali');"))
////    println(st.executeUpdate("INSERT into  ali.TEST(ID,NAME) values(2,'ali2');"))
//
//    println(System.currentTimeMillis())
//
////    thread {
//        println("1111  " + System.currentTimeMillis())
//        for (i in 1..700000)
//            st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'ali$i');")
//
//        println("1222  " + System.currentTimeMillis())
////    }
//
////    thread {
////        println("2" + System.currentTimeMillis())
////        for (i in 700001..1400000)
////            st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'azade$i');")
////        println("2" + System.currentTimeMillis())
////    }
////    println("3" + System.currentTimeMillis())
////    for (i in 1400001..2000000)
////        st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'azade$i');")
////    println("3" + System.currentTimeMillis())
////
////    Thread.sleep(10000)
////    println("4" + System.currentTimeMillis())
////    for (i in 1..1000000) {
////        val res1 = st.executeQuery("select count(*) from TEST where ID=$i ;")
////        res1.next()
////    }
//    val res = st.executeQuery("select * from TEST where ID='29' ;")
//
//
//    res.next()
//    println("4 " + res.getString("NAME"))
//    println(res.next())
//    println(res.getInt(1))


