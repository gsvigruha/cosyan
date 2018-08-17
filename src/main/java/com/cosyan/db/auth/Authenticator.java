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

import java.io.IOException;
import java.util.Random;

import com.cosyan.db.conf.Config;

public class Authenticator {

  private final LDAPConnector ldapConnector;
  private final LocalUsers localUsers;
  private final Random random;

  public Authenticator(Config config) throws IOException {
    ldapConnector = new LDAPConnector(config);
    localUsers = new LocalUsers();
    random = new Random(System.currentTimeMillis());
  }

  public LocalUsers localUsers() {
    return localUsers;
  }

  public static enum Method {
    LDAP, LOCAL
  }

  public String token() {
    return String.valueOf(random.nextLong());
  }

  public AuthToken auth(String username, String password, Method method) throws AuthException {
    String token = token();
    switch (method) {
    case LDAP:
      return ldapConnector.auth(username, password, token);
    case LOCAL:
      return localUsers.auth(username, password, token);
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
