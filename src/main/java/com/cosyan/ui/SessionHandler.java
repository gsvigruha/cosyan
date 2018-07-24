package com.cosyan.ui;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.json.JSONObject;

import com.cosyan.db.DBApi;
import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config;
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
  private final ThreadPoolExecutor threadPoolExecutor;
  private final ArrayBlockingQueue<Runnable> queue;
  private final Map<String, Session> sessions;

  public SessionHandler(DBApi dbApi) throws ConfigException {
    this.dbApi = dbApi;
    this.sessions = new HashMap<>();
    this.tokens = new HashMap<>();
    int numThreads = dbApi.config().getInt(Config.DB_NUM_THREADS);
    // TODO: figure out capacity.
    this.queue = new ArrayBlockingQueue<>(numThreads * 16);
    this.threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, Long.MAX_VALUE,
        TimeUnit.SECONDS, queue);
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
    if (!session.authToken().token().equals(token)) {
      throw new NoSessionExpression("Token mismatch.");
    }
    return session;
  }

  public void cancel(HttpServletRequest req, HttpServletResponse resp) throws IOException {
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

  public void execute(HttpServletRequest req, HttpServletResponse resp, CheckedFunction func)
      throws IOException {
    AsyncContext async = req.startAsync(req, resp);
    PrintWriter pw = resp.getWriter();
    threadPoolExecutor.execute(new Runnable() {

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

  public String login(String username, String password, String method)
      throws AuthException, ConfigException, NoSessionExpression {
    if (dbApi.config().auth()) {
      AuthToken token = dbApi.authenticator().auth(username, password,
          Authenticator.Method.valueOf(method));
      tokens.put(token.token(), token);
      return token.token();
    }
    throw new NoSessionExpression("Login not enabled.");
  }

  public void logout(String token) throws NoSessionExpression, ConfigException {
    if (dbApi.config().auth()) {
      if (tokens.containsKey(token)) {
        tokens.remove(token);
        sessions.entrySet().removeIf(e -> e.getValue().authToken().token().equals(token));
        return;
      }
    }
    throw new NoSessionExpression("Logout not enabled or invalid token.");
  }

  public String createSession(String token) throws NoSessionExpression, ConfigException {
    String sessionId = dbApi.authenticator().token();
    sessions.put(sessionId, session(token));
    return sessionId;
  }

  public void closeSession(String token, String sessionId)
      throws NoSessionExpression, ConfigException {
    getSession(token, sessionId);
    sessions.remove(sessionId);
  }

  public Session session(String token) throws NoSessionExpression, ConfigException {
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
