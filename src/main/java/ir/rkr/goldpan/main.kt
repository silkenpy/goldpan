package ir.rkr.goldpan


import java.sql.DriverManager


const val version = 0.1

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
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

//    println(con.prepareStatement("show tables").execute().toString())
//    val con = DriverManager.getConnection("jdbc:h2:mem:test")
    val con = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;")
//    val con = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:6969/mem:db1;DB_CLOSE_DELAY=-1;MULTI_THREADED=TRUE;")
//    val con = DriverManager.getConnection("jdbc:hsqldb:mem:test;")
//    Class.forName("org.sqlite.JDBC")
//
//    val con = DriverManager.getConnection("jdbc:sqlite:file::memory:?cache=shared")

    val st = con.createStatement()
    // println(st.executeUpdate("CREATE SCHEMA ali;").toString())
    println(st.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));"))
//    println(st.executeUpdate("INSERT into  ali.TEST(ID,NAME) values(1,'ali');"))
//    println(st.executeUpdate("INSERT into  ali.TEST(ID,NAME) values(2,'ali2');"))

    println(System.currentTimeMillis())

//    thread {
        println("1111  " + System.currentTimeMillis())
        for (i in 1..700000)
            st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'ali$i');")

        println("1222  " + System.currentTimeMillis())
//    }

//    thread {
//        println("2" + System.currentTimeMillis())
//        for (i in 700001..1400000)
//            st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'azade$i');")
//        println("2" + System.currentTimeMillis())
//    }
//    println("3" + System.currentTimeMillis())
//    for (i in 1400001..2000000)
//        st.executeUpdate("INSERT into  TEST(ID,NAME) values($i,'azade$i');")
//    println("3" + System.currentTimeMillis())
//
//    Thread.sleep(10000)
//    println("4" + System.currentTimeMillis())
//    for (i in 1..1000000) {
//        val res1 = st.executeQuery("select count(*) from TEST where ID=$i ;")
//        res1.next()
//    }
    val res = st.executeQuery("select * from TEST where ID='29' ;")


    res.next()
    println("4 " + res.getString("NAME"))
//    println(res.next())
//    println(res.getInt(1))

}
