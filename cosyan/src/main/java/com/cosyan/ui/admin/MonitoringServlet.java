package com.cosyan.ui.admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.simple.JSONObject;

import com.cosyan.db.model.MetaRepo;

public class MonitoringServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SystemMonitoring systemMonitoring;

  public MonitoringServlet(MetaRepo metaRepo) {
    this.systemMonitoring = new SystemMonitoring(metaRepo);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      resp.setStatus(HttpStatus.OK_200);
      resp.getWriter().println(systemMonitoring.usage());
    } catch (IOException e) {
      e.printStackTrace();
      JSONObject error = new JSONObject();
      error.put("error", e.getMessage());
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      resp.getWriter().println(error);
    }
  }

}
