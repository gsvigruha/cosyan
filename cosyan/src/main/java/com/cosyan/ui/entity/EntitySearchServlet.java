package com.cosyan.ui.entity;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.entity.EntityFields.ValueField;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableList;

public class EntitySearchServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final DBApi dbApi;
  private Session session;
  private EntityHandler entityHandler;

  public EntitySearchServlet(DBApi dbApi) {
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
      if (param.startsWith("filter_")) {
        fields.add(new ValueField(param.substring(7), req.getParameter(param)));
      }
    }
    JSONObject result = entityHandler.searchEntity(table, fields.build(), session).toJSON();
    if (result.has("error")) {
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);  
    } else {
      resp.setStatus(HttpStatus.OK_200);  
    }
    resp.getWriter().println(result);
  }
}
