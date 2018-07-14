package com.cosyan.db.auth;

public interface AuthToken {

  public void close();

  public String username();

  public boolean isAdmin();

  public static class AdminToken implements AuthToken {

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
  }

  public static final AuthToken ADMIN_AUTH = new AdminToken();
}
