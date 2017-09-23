package com.cosyan.ui.sql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.simple.JSONObject;

import com.cosyan.db.DBApi;

public class SQLServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SQLConnector sqlConnector;

  public SQLServlet(DBApi dbApi) {
    this.sqlConnector = new SQLConnector(dbApi);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String sql = req.getParameter("sql");
    JSONObject result = sqlConnector.run(sql);
    if (result.containsKey("error")) {
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);  
    } else {
      resp.setStatus(HttpStatus.OK_200);  
    }
    resp.getWriter().println(result);
  }
}
