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
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse


/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */
class JettyRestServer(val h2: H2Builder, config: Config, val goldPanMetrics: GoldPanMetrics) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()
    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool(100, 20)
        val server = Server(threadPool)
        val http = ServerConnector(server).apply {
            host= config.getString("metrics.ip")
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

        })   , "/")


        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                   // writer.write(gson.toJson(ignite.query(parsedJson[0])))
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