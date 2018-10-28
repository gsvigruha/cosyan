package com.cosyan.db.meta;

import java.io.IOException;

import com.cosyan.db.io.TableReader.SeekableTableReader;

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

  public abstract SeekableTableReader createReader() throws IOException;
}
