package com.cosyan.ui.sql;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.simple.JSONObject;

import com.cosyan.db.sql.Compiler;

public class SQLServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SQLConnector sqlConnector;

  public SQLServlet(Compiler compiler) {
    this.sqlConnector = new SQLConnector(compiler);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String sql = req.getParameter("sql");
    try {
      JSONObject result = sqlConnector.run(sql);
      resp.setStatus(HttpStatus.OK_200);
      resp.getWriter().println(result);
    } catch(Exception e) {
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
    } catch (OutOfMemoryError e) {
      e.printStackTrace();
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
    }
  }
}
