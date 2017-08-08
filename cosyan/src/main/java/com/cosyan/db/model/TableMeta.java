package com.cosyan.db.model;

import java.util.Map;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public abstract class TableMeta {

  public abstract ImmutableMap<String, ? extends ColumnMeta> columns();

  public abstract ImmutableMap<String, Object> read();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class MaterializedTableMeta extends TableMeta {
    private final String tableName;
    private final ImmutableMap<String, BasicColumn> columns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public ImmutableMap<String, Object> read() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DerivedTableMeta extends TableMeta {
    private final TableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public ImmutableMap<String, Object> read() {
      ImmutableMap<String, Object> sourceValues = sourceTable.read();
      ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
      for (Map.Entry<String, ColumnMeta> entry : columns.entrySet()) {
        values.put(entry.getKey(), entry.getValue().getValue(sourceValues));
      }
      return values.build();
    }
  }
}
