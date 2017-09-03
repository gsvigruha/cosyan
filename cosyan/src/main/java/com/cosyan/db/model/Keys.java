package com.cosyan.db.model;

import lombok.Data;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;

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
  }
}
