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
import com.cosyan.ui.ParamServlet.Servlet;
import com.cosyan.ui.admin.AdminServlet;
import com.cosyan.ui.admin.IndexServlet;
import com.cosyan.ui.admin.MonitoringServlet;
import com.cosyan.ui.admin.SessionServlets.CloseSessionServlet;
import com.cosyan.ui.admin.SessionServlets.CreateSessionServlet;
import com.cosyan.ui.admin.SessionServlets.LoginServlet;
import com.cosyan.ui.admin.UsersServlet;
import com.cosyan.ui.entity.EntityLoadServlet;
import com.cosyan.ui.entity.EntityMetaServlet;
import com.cosyan.ui.sql.SQLServlets.CancelServlet;
import com.cosyan.ui.sql.SQLServlets.SQLServlet;
import com.google.common.collect.ImmutableList;

public class WebServer {

  public final static ImmutableList<Class<? extends ParamServlet>> SERVLETS = ImmutableList.<Class<? extends ParamServlet>>builder()
      .add(AdminServlet.class)
      .add(LoginServlet.class)
      .add(MonitoringServlet.class)
      .add(UsersServlet.class)
      .add(IndexServlet.class)
      .add(SQLServlet.class)
      .add(CancelServlet.class)
      .add(CreateSessionServlet.class)
      .add(CloseSessionServlet.class)
      .add(EntityMetaServlet.class)
      .add(EntityLoadServlet.class)
      .build();

  private static void add(ServletContextHandler handler, ParamServlet servlet) {
    handler.addServlet(new ServletHolder(servlet), "/" + servlet.getClass().getAnnotation(Servlet.class).path());
  }

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

    for (Class<? extends ParamServlet> clss : SERVLETS) {
      ParamServlet servlet;
      try {
        servlet = clss.getConstructor(SessionHandler.class).newInstance(sessionHandler);
      } catch (NoSuchMethodException e) {
        servlet = clss.getConstructor(DBApi.class, SessionHandler.class).newInstance(dbApi, sessionHandler);
      }
      add(handler, servlet);
    }

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
