package com.cosyan.db.model;

import com.cosyan.db.model.DataTypes.DataType;

import lombok.Data;

@Data
public class BasicColumn {

  private final DataType<?> type;
  private final int index;
  private final String name;
  private boolean nullable;
  private boolean unique;
  private boolean indexed;
  private boolean deleted;
  private boolean immutable;

  public BasicColumn(int index, String name, DataType<?> type) {
    this(index, name, type, true, false, false, false);
  }

  public BasicColumn(
      int index,
      String name,
      DataType<?> type,
      boolean nullable,
      boolean unique,
      boolean immutable) {
    this(index, name, type, nullable, unique, /* indexed= */unique, immutable);
  }

  public BasicColumn(
      int index,
      String name,
      DataType<?> type,
      boolean nullable,
      boolean unique,
      boolean indexed,
      boolean immutable) {
    this.type = type;
    this.index = index;
    this.name = name;
    this.nullable = nullable;
    this.unique = unique;
    this.indexed = indexed;
    this.immutable = immutable;
    this.deleted = false;
    assert !unique || indexed;
    assert immutable || type != DataTypes.IDType;
  }
}