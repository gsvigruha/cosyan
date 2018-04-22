package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.sql.CreateStatement.ColumnDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementColumns {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddColumn extends Node implements MetaStatement {
    private final Ident table;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      BasicColumn basicColumn = new BasicColumn(
          tableMeta.allColumns().size(),
          column.getName(),
          column.getType(),
          column.isNullable(),
          column.isUnique(),
          column.isImmutable());
      tableMeta.addColumn(basicColumn);
      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropColumn extends Node implements MetaStatement {
    private final Ident table;
    private final Ident column;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      tableMeta.deleteColumn(column, metaRepo);
      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAlterColumn extends Node implements MetaStatement {
    private final Ident table;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo, AuthToken authToken) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      BasicColumn originalColumn = tableMeta.column(column.getName());
      if (originalColumn.getType() != column.getType()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', type has to remain the same.", column.getName()), column.getName());
      }
      if (originalColumn.isUnique() != column.isUnique()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', uniqueness has to remain the same.", column.getName()), column.getName());
      
      }
      originalColumn.setNullable(column.isNullable());
      originalColumn.setImmutable(column.isImmutable());
      return new MetaStatementResult();
    }
  }
}
