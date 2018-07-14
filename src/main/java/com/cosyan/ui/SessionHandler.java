package com.cosyan.ui;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableMap;

public class SessionHandler {

  @FunctionalInterface
  public interface CheckedFunction {
    void apply(Session session) throws IOException, AuthException, RuleException;
  }

  private final Map<String, Session> sessions = new HashMap<>();
  private final DBApi dbApi;
  private final Random random;
  private final Session adminSession;

  public SessionHandler(DBApi dbApi) {
    this.dbApi = dbApi;
    this.random = new Random(System.currentTimeMillis());
    this.adminSession = dbApi.adminSession();
  }

  public Session session(String userToken) throws NoSessionExpression, ConfigException {
    if (dbApi.config().auth()) {
      if (sessions.containsKey(userToken)) {
        return sessions.get(userToken);
      }
    } else {
      return adminSession;
    }
    throw new NoSessionExpression();
  }

  public void execute(HttpServletRequest req, HttpServletResponse resp, CheckedFunction func)
      throws IOException {
    try {
      Session session = session(req.getParameter("user"));
      func.apply(session);
    } catch (NoSessionExpression e) {
      resp.setStatus(HttpStatus.UNAUTHORIZED_401);
      resp.getWriter().println(new JSONObject(ImmutableMap.of("error",
          new JSONObject(ImmutableMap.of("msg", "Need to login.")))));
    } catch (ConfigException | RuleException e) {
      resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
      resp.getWriter().println(new JSONObject(ImmutableMap.of("error",
          new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
    } catch (AuthException e) {
      resp.setStatus(HttpStatus.UNAUTHORIZED_401);
      resp.getWriter().println(new JSONObject(ImmutableMap.of("error",
          new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
    }
  }

  public String login(String username, String password, String method) throws AuthException {
    String token = String.valueOf(random.nextLong());
    Session session = dbApi.authSession(username, password, Authenticator.Method.valueOf(method));
    sessions.put(token, session);
    return token;
  }

  public void logout(String userToken) throws NoSessionExpression, ConfigException {
    if (dbApi.config().auth()) {
      if (sessions.containsKey(userToken)) {
        sessions.remove(userToken);
      }
    }
    throw new NoSessionExpression();
  }

  public static class NoSessionExpression extends Exception {

    private static final long serialVersionUID = 1L;

  }
}
