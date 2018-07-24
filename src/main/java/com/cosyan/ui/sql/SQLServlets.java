package com.cosyan.ui.sql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;

public class SQLServlets {
  public static class SQLServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public SQLServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      sessionHandler.execute(req, resp, (Session task) -> {
        String sql = req.getParameter("sql");
        return task.execute(sql).toJSON();
      });
    }
  }

  public static class CancelServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private final SessionHandler sessionHandler;

    public CancelServlet(SessionHandler sessionHandler) {
      this.sessionHandler = sessionHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      sessionHandler.cancel(req, resp);
    }
  }
}