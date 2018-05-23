package com.cosyan.ui.entity;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cosyan.db.DBApi;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.session.Session;

public class EntityDeleteServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final DBApi dbApi;
  private Session session;
  private EntityHandler entityHandler;

  public EntityDeleteServlet(DBApi dbApi) {
    this.dbApi = dbApi;
    session = dbApi.adminSession();
    entityHandler = dbApi.entityHandler();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String table = req.getParameter("table");
    String id = req.getParameter("id");
    Result result = entityHandler.deleteEntity(table, id, session);
    resp.getWriter().println(result.toJSON().toString());
  }
}
