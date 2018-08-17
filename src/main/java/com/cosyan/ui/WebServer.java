/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.cosyan.ui.admin.MonitoringServlet;
import com.cosyan.ui.admin.SessionServlets.CloseSessionServlet;
import com.cosyan.ui.admin.SessionServlets.CreateSessionServlet;
import com.cosyan.ui.admin.SessionServlets.LoginServlet;
import com.cosyan.ui.admin.SessionServlets.LogoutServlet;
import com.cosyan.ui.entity.EntityLoadServlet;
import com.cosyan.ui.entity.EntityMetaServlet;
import com.cosyan.ui.sql.SQLServlets.CancelServlet;
import com.cosyan.ui.sql.SQLServlets.SQLServlet;

public class WebServer {
  public static void main(String[] args) throws Exception {
    Config config = new Config(System.getenv("COSYAN_CONF"));
    ThreadPool threadPool = new QueuedThreadPool(
        Math.max(2, config.getInt(Config.WEBSERVER_NUM_THREADS)), 2);
    Server server = new Server(threadPool);
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(config.port());
    server.setConnectors(new Connector[] { connector });

    ServletContextHandler handler = new ServletContextHandler(server, "/cosyan");
    DBApi dbApi = new DBApi(config);
    SessionHandler sessionHandler = new SessionHandler(dbApi);

    handler.addServlet(new ServletHolder(new AdminServlet(sessionHandler)), "/admin");
    handler.addServlet(new ServletHolder(new LoginServlet(sessionHandler)), "/login");
    handler.addServlet(new ServletHolder(new LogoutServlet(sessionHandler)), "/logout");
    handler.addServlet(new ServletHolder(new MonitoringServlet(sessionHandler)), "/monitoring");
    handler.addServlet(new ServletHolder(new IndexServlet(sessionHandler)), "/index");
    handler.addServlet(new ServletHolder(new SQLServlet(sessionHandler)), "/sql");
    handler.addServlet(new ServletHolder(new CancelServlet(sessionHandler)), "/cancel");
    handler.addServlet(new ServletHolder(new CreateSessionServlet(sessionHandler)),
        "/createSession");
    handler.addServlet(new ServletHolder(new CloseSessionServlet(sessionHandler)), "/closeSession");
    handler.addServlet(new ServletHolder(new EntityMetaServlet(dbApi, sessionHandler)),
        "/entityMeta");
    handler.addServlet(new ServletHolder(new EntityLoadServlet(dbApi, sessionHandler)),
        "/loadEntity");

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
