package com.cosyan.db.meta;

import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Rule;

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

  protected abstract void addReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, Rule rule);

  protected abstract void removeReverseRuleDependency(Iterable<Ref> reverseForeignKeyChain, Rule rule);
}
