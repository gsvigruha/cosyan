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

  private final Map<String, Session> sessions = new HashMap<>();
  private final DBApi dbApi;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final ArrayBlockingQueue<Runnable> queue;
  private final Session adminSession;

  public SessionHandler(DBApi dbApi) throws ConfigException {
    this.dbApi = dbApi;
    int numThreads = dbApi.config().getInt(Config.DB_NUM_THREADS);
    // TODO: figure out capacity.
    this.queue = new ArrayBlockingQueue<>(numThreads * 16);
    this.threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, Long.MAX_VALUE,
        TimeUnit.SECONDS, queue);
    this.adminSession = dbApi.newAdminSession();
  }

  public Session session(String token) throws NoSessionExpression, ConfigException {
    if (!dbApi.config().auth()) {
      return adminSession;
    } else if (sessions.containsKey(token)) {
      return sessions.get(token);
    }
    throw new NoSessionExpression();
  }

  public void execute(HttpServletRequest req, HttpServletResponse resp, CheckedFunction func)
      throws IOException {
    AsyncContext async = req.startAsync(req, resp);
    PrintWriter pw = resp.getWriter();
    threadPoolExecutor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          Session session = session(req.getParameter("token"));
          JSONObject result = func.apply(session);
          if (result.has("error")) {
            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
          } else {
            resp.setStatus(HttpStatus.OK_200);
          }
          pw.write(result.toString());
        } catch (NoSessionExpression e) {
          resp.setStatus(HttpStatus.UNAUTHORIZED_401);
          pw.println(new JSONObject(
              ImmutableMap.of("error", new JSONObject(ImmutableMap.of("msg", "Need to login.")))));
        } catch (ConfigException | RuleException | IOException e) {
          resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
          pw.println(new JSONObject(
              ImmutableMap.of("error", new JSONObject(ImmutableMap.of("msg", e.getMessage())))));
        } catch (AuthException e) {
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
      sessions.put(token.token(), dbApi.authSession(token));
      return token.token();
    }
    throw new NoSessionExpression();
  }

  public void logout(String token) throws NoSessionExpression, ConfigException {
    if (dbApi.config().auth()) {
      if (sessions.containsKey(token)) {
        sessions.remove(token);
        return;
      }
    }
    throw new NoSessionExpression();
  }

  public static class NoSessionExpression extends Exception {

    private static final long serialVersionUID = 1L;

  }
}
