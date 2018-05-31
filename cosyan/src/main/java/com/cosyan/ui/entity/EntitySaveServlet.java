package com.cosyan.ui.entity;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cosyan.db.DBApi;
import com.cosyan.db.entity.EntityFields.ValueField;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableList;

public class EntitySaveServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final DBApi dbApi;
  private Session session;
  private EntityHandler entityHandler;

  public EntitySaveServlet(DBApi dbApi) {
    this.dbApi = dbApi;
    session = dbApi.adminSession();
    entityHandler = dbApi.entityHandler();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String table = req.getParameter("table");
    ImmutableList.Builder<ValueField> fields = ImmutableList.builder();
    for (String param : req.getParameterMap().keySet()) {
      if (param.startsWith("param_")) {
        fields.add(new ValueField(param.substring(6), req.getParameter(param)));
      }
    }
    ValueField idField = new ValueField(req.getParameter("id_name"), req.getParameter("id_value"));
    Result result = entityHandler.saveEntity(table, fields.build(), idField, session);
    resp.getWriter().println(result.toJSON().toString());
  }
}
