package com.cosyan.ui;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;

import com.cosyan.db.DBApi;
import com.cosyan.db.conf.Config;
import com.cosyan.ui.admin.AdminServlet;
import com.cosyan.ui.admin.IndexServlet;
import com.cosyan.ui.admin.LoginServlet;
import com.cosyan.ui.admin.LogoutServlet;
import com.cosyan.ui.admin.MonitoringServlet;
import com.cosyan.ui.entity.EntityLoadServlet;
import com.cosyan.ui.entity.EntityMetaServlet;
import com.cosyan.ui.sql.SQLServlet;

public class WebServer {
  public static void main(String[] args) throws Exception {
    ThreadPool threadPool = new QueuedThreadPool(20, 4);
    Server server = new Server(threadPool);
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(7070);
    server.setConnectors(new Connector[] { connector });

    ServletContextHandler handler = new ServletContextHandler(server, "/cosyan");
    Config config = new Config(System.getenv("COSYAN_CONF"));
    DBApi dbApi = new DBApi(config);
    SessionHandler sessionHandler = new SessionHandler(dbApi);

    handler.addServlet(new ServletHolder(new AdminServlet(sessionHandler)), "/admin");
    handler.addServlet(new ServletHolder(new LoginServlet(sessionHandler)), "/login");
    handler.addServlet(new ServletHolder(new LogoutServlet(sessionHandler)), "/logout");
    handler.addServlet(new ServletHolder(new MonitoringServlet(sessionHandler)), "/monitoring");
    handler.addServlet(new ServletHolder(new IndexServlet(sessionHandler)), "/index");
    handler.addServlet(new ServletHolder(new SQLServlet(sessionHandler)), "/sql");
    handler.addServlet(new ServletHolder(new EntityMetaServlet(dbApi, sessionHandler)), "/entityMeta");
    handler.addServlet(new ServletHolder(new EntityLoadServlet(dbApi, sessionHandler)), "/loadEntity");

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(false);
    resourceHandler.setWelcomeFiles(new String[] { "index.html" });
    resourceHandler.setResourceBase("web/app");

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] { resourceHandler, handler });

    server.setHandler(handlers);
    server.start();
    server.join();
  }
}
