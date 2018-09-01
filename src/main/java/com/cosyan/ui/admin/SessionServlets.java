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
package com.cosyan.ui.admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.ui.ParamServlet;
import com.cosyan.ui.ParamServlet.Servlet;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.SessionHandler.NoSessionExpression;
import com.google.common.collect.ImmutableMap;

public class SessionServlets {

  @Servlet(path = "login", doc = "Logs in to the DB and returns the user authentication token.")
  public static class LoginServlet extends ParamServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public LoginServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Param(name = "username", mandatory = true, doc = "The name of the DB user.")
    @Param(name = "password", mandatory = true, doc = "The unencrypted password of the DB user.")
    @Param(name = "method", mandatory = true, doc = "Authentication method (LOCAL, LDAP).")
    @Override
    protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        String token = sessionHandler.login(
            req.getParameter("username"),
            req.getParameter("password"),
            req.getParameter("method"));
        obj.put("token", token);
        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println(obj);
      } catch (AuthException | ConfigException | NoSessionExpression e) {
        JSONObject error = new JSONObject();
        error.put("error", new JSONObject(ImmutableMap.of("msg", e.getMessage())));
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        resp.getWriter().println(error);
      }
    }
  }

  @Servlet(path = "logout", doc = "Logs out the DB user.")
  public static class LogoutServlet extends ParamServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public LogoutServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Param(name = "token", mandatory = true, doc = "User authentication token.")
    @Override
    protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        sessionHandler.logout(req.getParameter("token"));
        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println(obj);
      } catch (NoSessionExpression | ConfigException e) {
        JSONObject error = new JSONObject();
        error.put("error", new JSONObject(ImmutableMap.of("msg", "Invalid user token.")));
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        resp.getWriter().println(error);
      }
    }
  }

  @Servlet(path = "createSession", doc = "Creates a session for a given user and returns the session ID.")
  public static class CreateSessionServlet extends ParamServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public CreateSessionServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Param(name = "token", doc = "User authentication token.")
    @Override
    protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        String sessionId = sessionHandler.createSession(req.getParameter("token"));
        obj.put("session", sessionId);
        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println(obj);
      } catch (NoSessionExpression | ConfigException e) {
        JSONObject error = new JSONObject();
        error.put("error", new JSONObject(ImmutableMap.of("msg", "Invalid user token.")));
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        resp.getWriter().println(error);
      }
    }
  }

  @Servlet(path = "closeSession", doc = "Closes the session.")
  public static class CloseSessionServlet extends ParamServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public CloseSessionServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Param(name = "token", doc = "User authentication token.")
    @Param(name = "session", mandatory = true, doc = "Session ID.")
    @Override
    protected void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        sessionHandler.closeSession(req.getParameter("token"), req.getParameter("session"));
        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println(obj);
      } catch (NoSessionExpression | ConfigException e) {
        JSONObject error = new JSONObject();
        error.put("error", new JSONObject(ImmutableMap.of("msg", "Invalid user token.")));
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        resp.getWriter().println(error);
      }
    }
  }
}