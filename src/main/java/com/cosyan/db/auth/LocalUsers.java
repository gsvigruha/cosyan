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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.cosyan.db.auth.Authenticator.AuthException;

public class LocalUsers {

  public static class LocalUserToken implements AuthToken {

    private final String username;
    private final String token;

    public LocalUserToken(String username, String token) {
      this.username = username;
      this.token = token;
    }

    @Override
    public void close() {
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

  private final Map<String, String> users;

  public LocalUsers() throws IOException {
    users = new HashMap<>();
  }

  private String hash(String password) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return javax.xml.bind.DatatypeConverter
        .printHexBinary(digest.digest(password.getBytes(StandardCharsets.UTF_8)));
  }

  public AuthToken auth(String username, String password, String token) throws AuthException {
    try {
      String hex = hash(password);
      if (users.containsKey(username) && users.get(username).equals(hex)) {
        if (username.equals("admin")) {
          return AuthToken.adminToken(token);
        } else {
          return new LocalUserToken(username, token);
        }
      } else {
        throw new AuthException("Wrong username/password.");
      }
    } catch (NoSuchAlgorithmException e) {
      throw new AuthException(e.getMessage());
    }
  }

  public void createUser(String username, String password) throws AuthException {
    if (users.containsKey(username)) {
      throw new AuthException(String.format("User '%s' already exists.", username));
    }
    try {
      String hashedPW = hash(password);
      users.put(username, hashedPW);
    } catch (NoSuchAlgorithmException e) {
      throw new AuthException(e.getMessage());
    }
  }

  public boolean isLocalUser(String username) {
    return users.containsKey(username);
  }

  public String hashedPW(String username) {
    return users.get(username);
  }

  public void reload(Map<String, String> users) {
    this.users.clear();
    this.users.putAll(users);
  }

  public Set<String> users() {
    return users.keySet();
  }
}
