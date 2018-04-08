package com.cosyan.db.auth;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

  public LocalUsers(Config config) {
    this.config = config;
  }

  public static String hash(String password) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return javax.xml.bind.DatatypeConverter
        .printHexBinary(digest.digest(password.getBytes(StandardCharsets.UTF_8)));
  }

  public AuthToken auth(String username, String password) throws AuthException {
    try {
      String hex = hash(password);
      BufferedReader reader = new BufferedReader(new FileReader(config.usersFile()));
      String line = null;
      try {
        while ((line = reader.readLine()) != null) {
          String[] parts = line.split(":");
          if (username.equals(parts[0]) && hex.equals(parts[1])) {
            if (username.equals("admin")) {
              return AuthToken.ADMIN_AUTH;
            } else {
              return new LocalUserToken(username);
            }
          }
        }
        throw new AuthException("Wrong username/password.");
      } finally {
        reader.close();
      }
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new AuthException(e.getMessage());
    }
  }
}
