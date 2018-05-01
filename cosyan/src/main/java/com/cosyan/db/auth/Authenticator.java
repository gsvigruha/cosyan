package com.cosyan.db.auth;

import java.io.IOException;

import com.cosyan.db.conf.Config;

public class Authenticator {

  private final LDAPConnector ldapConnector;
  private final LocalUsers localUsers;

  public Authenticator(Config config) throws IOException {
    ldapConnector = new LDAPConnector(config);
    localUsers = new LocalUsers(config);
  }

  public LocalUsers localUsers() {
    return localUsers;
  }

  public static enum Method {
    LDAP, LOCAL
  }

  public AuthToken auth(String username, String password, Method method) throws AuthException {
    switch (method) {
    case LDAP:
      return ldapConnector.auth(username, password);
    case LOCAL:
      return localUsers.auth(username, password);
    default:
      throw new AuthException(String.format("Invalid authentication method '%s'.", method));
    }
  }

  public static class AuthException extends Exception {

    private static final long serialVersionUID = 1L;

    public AuthException(String msg) {
      super(msg);
    }
  }
}
