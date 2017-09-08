package com.cosyan.ui.admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;

import com.cosyan.db.model.MetaRepo;

public class AdminServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final MetaRepoConnector metaRepoConnector;

  public AdminServlet(MetaRepo metaRepo) {
    this.metaRepoConnector = new MetaRepoConnector(metaRepo);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setStatus(HttpStatus.OK_200);
    resp.getWriter().println(metaRepoConnector.tables());
  }
}
