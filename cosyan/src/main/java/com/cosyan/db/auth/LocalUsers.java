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

    public LocalUserToken(String username) {
      this.username = username;
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

  public AuthToken auth(String username, String password) throws AuthException {
    try {
      String hex = hash(password);
      if (users.containsKey(username) && users.get(username).equals(hex)) {
        if (username.equals("admin")) {
          return AuthToken.ADMIN_AUTH;
        } else {
          return new LocalUserToken(username);
        }
      } else {
        throw new AuthException("Wrong username/password.");
      }
    } catch (NoSuchAlgorithmException e) {
      throw new AuthException(e.getMessage());
    }
  }

  public void createUser(String username, String password) throws AuthException, IOException {
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
