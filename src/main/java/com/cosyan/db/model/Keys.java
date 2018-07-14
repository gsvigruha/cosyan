package com.cosyan.db.model;

import com.cosyan.db.meta.MaterializedTable;

import lombok.Data;

public class Keys {

  @Data
  public static class PrimaryKey {
    private final Ident name;
    private final BasicColumn column;
  }

  public static interface Ref {

    String getName();

    String getRevName();

    MaterializedTable getTable();

    BasicColumn getColumn();

    MaterializedTable getRefTable();

    BasicColumn getRefColumn();

    Ref getReverse();
  }

  @Data
  public static class ForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + column.getName() + " -> " + refTable.tableName() + "." + refColumn.getName() + "]";
    }

    public ReverseForeignKey createReverse() {
      return new ReverseForeignKey(revName, name, refTable, refColumn, table, column);
    }

    public ReverseForeignKey getReverse() {
      return refTable.reverseForeignKeys().get(revName);
    }
  }

  @Data
  public static class ReverseForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + refTable.tableName() + "." + refColumn.getName() + " -> " + column.getName() + "]";
    }

    public ForeignKey getReverse() {
      return refTable.foreignKeys().get(revName);
    }
  }
}
