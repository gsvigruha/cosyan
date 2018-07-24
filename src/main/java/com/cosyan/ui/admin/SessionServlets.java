package com.cosyan.ui.admin;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.SessionHandler.NoSessionExpression;
import com.google.common.collect.ImmutableMap;

public class SessionServlets {
  public static class LoginServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public LoginServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        String token = sessionHandler.login(req.getParameter("username"),
            req.getParameter("password"), req.getParameter("method"));
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

  public static class LogoutServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public LogoutServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
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

  public static class CreateSessionServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public CreateSessionServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        JSONObject obj = new JSONObject();
        String token = Optional.ofNullable(req.getParameter("token")).orElse("no_token");
        String sessionId = sessionHandler.createSession(token);
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

  public static class CloseSessionServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public CloseSessionServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
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