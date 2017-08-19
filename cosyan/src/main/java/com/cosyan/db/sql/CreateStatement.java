package com.cosyan.db.sql;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class CreateStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Create extends Node implements Statement {
    private final String name;
    private final ImmutableList<ColumnDefinition> columns;

    @Override
    public boolean execute(MetaRepo metaRepo) {
      ImmutableMap.Builder<String, BasicColumn> builder = ImmutableMap.builder();
      int i = 0;
      for (ColumnDefinition column : columns) {
        builder.put(column.getName(), new BasicColumn(i++, column.getType(), column.isNullable()));
      }
      MaterializedTableMeta tableMeta = new MaterializedTableMeta(name, builder.build(), metaRepo);
      metaRepo.registerTable(name, tableMeta);
      return true;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnDefinition extends Node {
    private final String name;
    private final DataType<?> type;
    private final boolean nullable;
  }
}
