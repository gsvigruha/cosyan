package com.cosyan.db.model;

import com.cosyan.db.model.ColumnMeta.BasicColumn;

import lombok.Data;

public class Keys {

  @Data
  public static class PrimaryKey {
    private final String name;
    private final BasicColumn column;
  }

  @Data
  public static class ForeignKey {
    private final String name;
    private final MaterializedTableMeta table;
    private final BasicColumn column;
    private final MaterializedTableMeta refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + column.getName() + " -> " + refTable.tableName() + "." + refColumn.getName() + "]";
    }

    public ReverseForeignKey createReverse() {
      return new ReverseForeignKey(name, refTable, refColumn, table, column);
    }

    public ReverseForeignKey getReverse() {
      return refTable.reverseForeignKeys().get(name);
    }
  }

  @Data
  public static class ReverseForeignKey {
    private final String name;
    private final MaterializedTableMeta table;
    private final BasicColumn column;
    private final MaterializedTableMeta refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + refTable.tableName() + "." + refColumn.getName() + " -> " + column.getName() + "]";
    }
    
    public ForeignKey getReverse() {
      return refTable.foreignKeys().get(name);
    }
  }
}
