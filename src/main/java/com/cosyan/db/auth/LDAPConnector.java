package com.cosyan.db.auth;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;

public class LDAPConnector {

  public static class LDAPToken implements AuthToken {

    private final LDAPConnection connection;
    private final String username;
    private final String token;

    public LDAPToken(LDAPConnection connection, String username, String token) {
      this.connection = connection;
      this.username = username;
      this.token = token;
    }

    @Override
    public void close() {
      connection.close();
    }

    @Override
    public String username() {
      return username;
    }

    @Override
    public boolean isAdmin() {
      return false;
    }

    @Override
    public String token() {
      return token;
    }
  }

  private final Config config;

  public LDAPConnector(Config config) {
    this.config = config;
  }

  public AuthToken auth(String username, String password, String token) throws AuthException {
    String ldapHost = config.get(Config.LDAP_HOST);
    try {
      LDAPConnection connection = new LDAPConnection(
          ldapHost,
          Integer.valueOf(config.get(Config.LDAP_PORT)),
          username + "@" + ldapHost,
          password);

      if (connection.isConnected()) {
        return new LDAPToken(connection, username, token);
      } else {
        connection.close();
        throw new AuthException("Connection not connected.");
      }
    } catch (LDAPException e) {
      throw new AuthException(e.getExceptionMessage());
    }
  }
}
