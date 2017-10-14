package com.cosyan.ui;

import java.util.Properties;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.cosyan.db.DBApi;
import com.cosyan.db.conf.Config;
import com.cosyan.ui.admin.AdminServlet;
import com.cosyan.ui.admin.MonitoringServlet;
import com.cosyan.ui.sql.SQLServlet;

public class WebServer {
  public static void main(String[] args) throws Exception {
    Server server = new Server(7070);
    ServletContextHandler handler = new ServletContextHandler(server, "/cosyan");
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/webserver");
    Config config = new Config(props);
    DBApi dbApi = new DBApi(config);

    handler.addServlet(new ServletHolder(new AdminServlet(dbApi.getMetaRepo())), "/admin");
    handler.addServlet(new ServletHolder(new MonitoringServlet(dbApi.getMetaRepo())), "/monitoring");
    handler.addServlet(new ServletHolder(new SQLServlet(dbApi)), "/sql");

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(false);
    resourceHandler.setWelcomeFiles(new String[] { "index.html" });
    resourceHandler.setResourceBase("web/app/");

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] { resourceHandler, handler });

    server.setHandler(handlers);
    server.start();
  }
}
