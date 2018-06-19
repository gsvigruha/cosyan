package com.cosyan.ui;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.cosyan.db.DBApi;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.session.Session;

public class SessionHandler {

  private final Map<String, Session> sessions = new HashMap<>();
  private final DBApi dbApi;
  private final Random random;

  public SessionHandler(DBApi dbApi) {
    this.dbApi = dbApi;
    this.random = new Random(System.currentTimeMillis());
  }

  public Session session(String userToken) throws NoSessionExpression {
    if (sessions.containsKey(userToken)) {
      return sessions.get(userToken);
    }
    throw new NoSessionExpression();
  }

  public String login(String username, String password, String method) throws AuthException {
    String token = String.valueOf(random.nextLong());
    Session session = dbApi.authSession(username, password, Authenticator.Method.valueOf(method));
    sessions.put(token, session);
    return token;
  }

  public void logout(String userToken) throws NoSessionExpression {
    if (sessions.containsKey(userToken)) {
      sessions.remove(userToken);
    } else {
      throw new NoSessionExpression();
    }
  }

  public static class NoSessionExpression extends Exception {

    private static final long serialVersionUID = 1L;

  }
}
