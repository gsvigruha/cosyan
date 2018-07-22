package com.cosyan.ui.entity;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cosyan.db.DBApi;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.session.Session;
import com.cosyan.ui.SessionHandler;

public class EntityLoadServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;
  private final EntityHandler entityHandler;

  public EntityLoadServlet(DBApi dbApi, SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
    this.entityHandler = dbApi.entityHandler();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    sessionHandler.execute(req, resp, (Session session) -> {
      String table = req.getParameter("table");
      String id = req.getParameter("id");
      return entityHandler.loadEntity(table, id, session).toJSON();
    });
  }
}
