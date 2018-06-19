package com.cosyan.ui.admin;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.ui.SessionHandler;

public class LoginServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  private final SessionHandler sessionHandler;

  public LoginServlet(SessionHandler sessionHandler) {
    this.sessionHandler = sessionHandler;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      JSONObject obj = new JSONObject();
      String token = sessionHandler.login(req.getParameter("username"), req.getParameter("password"), req.getParameter("method"));
      obj.put("token", token);
      resp.setStatus(HttpStatus.OK_200);
      resp.getWriter().println(obj);
    } catch (AuthException e) {
      JSONObject error = new JSONObject();
      error.put("error", e.getMessage());
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      resp.getWriter().println(error);
    }
  }
}
