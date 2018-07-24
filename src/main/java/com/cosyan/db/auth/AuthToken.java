package com.cosyan.db.auth;

import com.google.common.base.Preconditions;

public interface AuthToken {

  public void close();

  public String username();

  public boolean isAdmin();

  public String token();

  public static class AdminToken implements AuthToken {

    private final String token;

    public AdminToken(String token) {
      this.token = Preconditions.checkNotNull(token);
    }

    @Override
    public void close() {
    }

    @Override
    public String username() {
      return "admin";
    }

    @Override
    public boolean isAdmin() {
      return true;
    }

    @Override
    public String token() {
      return token;
    }
  }

  public static AdminToken adminToken(String token) {
    return new AdminToken(token);
  }
}
