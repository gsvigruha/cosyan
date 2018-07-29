package com.cosyan.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableMap;

public class SessionHandler {

  @FunctionalInterface
  public interface CheckedFunction {
    JSONObject apply(Session session)
        throws IOException, AuthException, RuleException, ConfigException;
  }

  private final Map<String, AuthToken> tokens;
  private final DBApi dbApi;
  private final Map<String, Session> sessions;

  public SessionHandler(DBApi dbApi) throws ConfigException {
    this.dbApi = dbApi;
    this.sessions = new HashMap<>();
    this.tokens = new HashMap<>();
  }

  private Session getSession(HttpServletRequest req) throws NoSessionExpression, ConfigException {
    String sessionId = req.getParameter("session");
    String token = req.getParameter("token");
    if (sessionId == null) {
      return session(token);
    } else {
      return getSession(token, sessionId);
    }
  }

  private Session getSession(String token, String sessionId) throws NoSessionExpression {
    Session session = sessions.get(sessionId);
    if (session == null) {
      throw new NoSessionExpression("Invalid session.");
    }
    if (!Objects.equals(session.authToken().token(), token)) {
      throw new NoSessionExpression("Token mismatch.");
    }
    return session;
  }

  public synchronized void cancel(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    PrintWriter pw = resp.getWriter();
    try {
      Session session = getSession(req.getParameter("token"), req.getParameter("session"));
      session.cancel();
      resp.setStatus(HttpStatus.OK_200);
    } catch (NoSessionExpression e) {
      resp.setStatus(HttpStatus.UNAUTHORIZED_401);
      pw.println(new JSONObject(
          ImmutableMap.of("error", new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
    } finally {
      pw.close();
    }
  }

  public synchronized void execute(HttpServletRequest req, HttpServletResponse resp, CheckedFunction func)
      throws IOException {
    AsyncContext async = req.startAsync(req, resp);
    async.setTimeout(0);
    PrintWriter pw = resp.getWriter();
    dbApi.execute(new Runnable() {

      @Override
      public void run() {
        try {
          Session session = getSession(req);
          JSONObject result = func.apply(session);
          if (result.has("error")) {
            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
          } else {
            resp.setStatus(HttpStatus.OK_200);
          }
          pw.write(result.toString());
        } catch (ConfigException | RuleException | IOException e) {
          resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
          pw.println(new JSONObject(
              ImmutableMap.of("error", new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
        } catch (AuthException | NoSessionExpression e) {
          resp.setStatus(HttpStatus.UNAUTHORIZED_401);
          pw.println(new JSONObject(
              ImmutableMap.of("error", new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
        } finally {
          async.complete();
          pw.close();
        }
      }
    });
  }

  public synchronized String login(String username, String password, String method)
      throws AuthException, ConfigException, NoSessionExpression {
    if (dbApi.config().auth()) {
      AuthToken token = dbApi.authenticator().auth(username, password,
          Authenticator.Method.valueOf(method));
      tokens.put(token.token(), token);
      return token.token();
    }
    throw new NoSessionExpression("Login not enabled.");
  }

  public synchronized void logout(String token) throws NoSessionExpression, ConfigException {
    if (dbApi.config().auth()) {
      if (tokens.containsKey(token)) {
        tokens.remove(token);
        sessions.entrySet().removeIf(e -> e.getValue().authToken().token().equals(token));
        return;
      }
    }
    throw new NoSessionExpression("Logout not enabled or invalid token.");
  }

  public synchronized String createSession(String token) throws NoSessionExpression, ConfigException {
    String sessionId = dbApi.authenticator().token();
    sessions.put(sessionId, session(token));
    return sessionId;
  }

  public synchronized void closeSession(String token, String sessionId)
      throws NoSessionExpression, ConfigException {
    getSession(token, sessionId);
    sessions.remove(sessionId);
  }

  public synchronized Session session(String token) throws NoSessionExpression, ConfigException {
    if (!dbApi.config().auth()) {
      return dbApi.newAdminSession(token);
    } else if (tokens.containsKey(token)) {
      return dbApi.authSession(tokens.get(token));
    }
    throw new NoSessionExpression("Invalid token.");
  }

  public static class NoSessionExpression extends Exception {

    private static final long serialVersionUID = 1L;

    public NoSessionExpression(String msg) {
      super(msg);
    }
  }
}
