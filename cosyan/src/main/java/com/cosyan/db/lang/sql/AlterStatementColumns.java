package com.cosyan.db.lang.sql;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.TableDefinition.ColumnDefinition;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatementColumns {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddColumn extends Node implements AlterStatement {
    private final Ident table;
    private final ColumnDefinition column;

    private BasicColumn basicColumn;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table);
      basicColumn = tableMeta.createColumn(column);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      MaterializedTable tableMeta = resources.meta(table.getString());
      tableMeta.addColumn(basicColumn);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropColumn extends Node implements AlterStatement {
    private final Ident table;
    private final Ident column;

    private BasicColumn basicColumn;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table);
      basicColumn = tableMeta.column(column);
      tableMeta.checkDeleteColumn(column);
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      basicColumn.setDeleted(true);
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAlterColumn extends Node implements AlterStatement {
    private final Ident table;
    private final ColumnDefinition column;

    private BasicColumn originalColumn;

    @Override
    public MetaResources compile(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
      MaterializedTable tableMeta = metaRepo.table(table);
      originalColumn = tableMeta.column(column.getName());
      if (originalColumn.getType() != column.getType()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', type has to remain the same.", column.getName()),
            column.getName());
      }
      if (originalColumn.isUnique() != column.isUnique()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', uniqueness has to remain the same.", column.getName()),
            column.getName());

      }
      if (originalColumn.isNullable() && !column.isNullable()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', column has to remain nullable.", column.getName()),
            column.getName());
      }
      return MetaResources.tableMeta(tableMeta);
    }

    @Override
    public boolean log() {
      return true;
    }

    @Override
    public Result execute(MetaRepoExecutor metaRepo, Resources resources) throws RuleException, IOException {
      originalColumn.setNullable(column.isNullable());
      originalColumn.setImmutable(column.isImmutable());
      return Result.META_OK;
    }

    @Override
    public void cancel() {
    }
  }
}
