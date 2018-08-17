/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
