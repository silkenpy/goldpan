package ir.rkr.goldpan.rest


import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.goldpan.h2.H2Builder
import ir.rkr.goldpan.utils.GoldPanMetrics
import ir.rkr.goldpan.utils.fromJson

import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.json.JSONObject
import java.sql.ResultSet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */

fun resultsetToJson( rs: ResultSet) : String{

//    val result = JSONObject()

    var result = StringBuilder()
//    var count = 0
    while (rs.next()) {

        val numColumns = rs.metaData.getColumnCount()
        val obj = JSONObject()

        for (i in 1 until numColumns + 1) {
            val column_name = rs.metaData.getColumnName(i)

            when (rs.metaData.getColumnType(i)) {
                java.sql.Types.ARRAY -> obj.put(column_name, rs.getArray(column_name))
                java.sql.Types.BIGINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.BOOLEAN -> obj.put(column_name, rs.getBoolean(column_name))
                java.sql.Types.BLOB -> obj.put(column_name, rs.getBlob(column_name))
                java.sql.Types.DOUBLE -> obj.put(column_name, rs.getDouble(column_name))
                java.sql.Types.FLOAT -> obj.put(column_name, rs.getFloat(column_name))
                java.sql.Types.INTEGER -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.NVARCHAR -> obj.put(column_name, rs.getNString(column_name))
                java.sql.Types.VARCHAR -> obj.put(column_name, rs.getString(column_name))
                java.sql.Types.TINYINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.SMALLINT -> obj.put(column_name, rs.getInt(column_name))
                java.sql.Types.DATE -> obj.put(column_name, rs.getDate(column_name))
                java.sql.Types.TIMESTAMP -> obj.put(column_name, rs.getTimestamp(column_name))
                else -> obj.put(column_name, rs.getObject(column_name))
            }
        }
        result.append("\n"+obj.toString())
        // println(obj.toString())
//        result.put(count.toString(), obj.toString())
//        count += 1
    }

    return result.toString()

}

class JettyRestServer( h2: H2Builder, config: Config,  goldPanMetrics: GoldPanMetrics) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()

    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool(100, 20)
        val server = Server(threadPool)
        val http = ServerConnector(server).apply {
            host = config.getString("metrics.ip")
            port = config.getInt("metrics.port")
        }
        server.addConnector(http)

        val handler = ServletContextHandler(server, "/")

        /**
         * It can handle multi-get requests for Urls in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {

            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson("server is busy"))
                }
            }

        }), "/")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())
                val rs = h2.executeQuery(parsedJson[0])

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                     writer.write(resultsetToJson(rs))
                }
            }
        }), "/query")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(goldPanMetrics.getInfo()))
                }
            }
        }), "/metrics")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "text/plain; charset=utf-8")
                    addHeader("Connection", "close")
                    writer.write("server  is running :D")
                }
            }
        }), "/version")

        server.start()

    }
}