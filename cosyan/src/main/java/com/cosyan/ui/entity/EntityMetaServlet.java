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

public class EntityMetaServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final DBApi dbApi;
  private Session session;
  private EntityHandler entityHandler;

  public EntityMetaServlet(DBApi dbApi) {
    this.dbApi = dbApi;
    session = dbApi.adminSession();
    entityHandler = dbApi.entityHandler();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Result result = entityHandler.entityMeta(session);
    resp.getWriter().println(result.toJSON().toString());
  }
}
