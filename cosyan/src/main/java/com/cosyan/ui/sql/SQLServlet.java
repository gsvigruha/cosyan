package com.cosyan.ui.sql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;
import com.cosyan.ui.SessionHandler.NoSessionExpression;
import com.google.common.collect.ImmutableMap;

public class SQLServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;

  public SQLServlet(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      Session session = sessionHandler.session(req.getParameter("user"));
      String sql = req.getParameter("sql");
      JSONObject result = session.execute(sql).toJSON();
      if (result.has("error")) {
        resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      } else {
        resp.setStatus(HttpStatus.OK_200);
      }
      resp.getWriter().println(result);
    } catch (NoSessionExpression | ConfigException e) {
      resp.setStatus(HttpStatus.UNAUTHORIZED_401);
      resp.getWriter().println(new JSONObject(ImmutableMap.of("error",
          new JSONObject(ImmutableMap.of("msg", "Need to login.")))));
    }
  }
}
