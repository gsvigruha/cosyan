package com.cosyan.db.auth;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config;

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

  private final Config config;
  private final Map<String, String> users;

  public LocalUsers(Config config) throws IOException {
    this.config = config;
    users = new HashMap<>();
    BufferedReader reader = new BufferedReader(new FileReader(config.usersFile()));
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(":");
        users.put(parts[0], parts[1]);
      }
    } finally {
      reader.close();
    }
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

    BufferedWriter writer = new BufferedWriter(new FileWriter(config.usersFile(), /* append= */true));
    try {
      String hashedPW = hash(password);
      writer.write("\n" + username + ":" + hashedPW);
      users.put(username, hashedPW);
    } catch (NoSuchAlgorithmException e) {
      throw new AuthException(e.getMessage());
    } finally {
      writer.close();
    }
  }
}
