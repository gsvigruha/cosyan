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
    private final BasicColumn column;
    private final MaterializedTableMeta refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name;
    }
  }
}
