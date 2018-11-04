package com.cosyan.db.meta;

public abstract class DBObject {

  private final String name;
  private final String owner;

  public DBObject(String name, String owner) {
    this.name = name;
    this.owner = owner;
  }

  public String name() {
    return name;
  }

  public String owner() {
    return owner;
  }

  public String fullName() {
    return owner + "." + name;
  }
}
