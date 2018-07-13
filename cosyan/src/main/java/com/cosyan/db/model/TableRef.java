package com.cosyan.db.model;

import com.cosyan.db.model.References.AggRefTableMeta;

import lombok.Data;

@Data
public class TableRef {
  
  private final String name;
  private final String expr;
  private final int index;
  protected final transient AggRefTableMeta tableMeta;

  @Override
  public String toString() {
    return name + " [" + expr + "]";
  }
}
