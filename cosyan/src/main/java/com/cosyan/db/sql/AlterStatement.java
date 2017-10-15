package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.CreateStatement.ColumnDefinition;
import com.cosyan.db.sql.CreateStatement.SimpleCheckDefinition;
import com.cosyan.db.sql.Result.MetaStatementResult;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.sql.SyntaxTree.Node;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddColumn extends Node implements MetaStatement {
    private final Ident table;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      if (!column.isNullable()) {
        throw new ModelException(
            String.format("Cannot add column '%s', new columns have to be nullable.", column.getName()));
      }
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      if (tableMeta.indexOf(new Ident(column.getName())) >= 0) {
        throw new ModelException(
            String.format("Cannot add column '%s', column with the same name already exists.", column.getName()));
      }
      BasicColumn basicColumn = new BasicColumn(
          tableMeta.columns().size(),
          column.getName(),
          column.getType(),
          column.isNullable(),
          column.isUnique());
      tableMeta.addColumn(basicColumn);
      if (basicColumn.isIndexed()) {
        if (basicColumn.isUnique()) {
          metaRepo.registerUniqueIndex(tableMeta, basicColumn);
        } else {
          metaRepo.registerMultiIndex(tableMeta, basicColumn);
        }
      }
      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropColumn extends Node implements MetaStatement {
    private final Ident table;
    private final Ident column;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      BasicColumn basicColumn = tableMeta.column(column);
      basicColumn.setDeleted(true);
      try {
        for (SimpleCheckDefinition simpleCheckDefinition : tableMeta.simpleCheckDefinitions()) {
          try {
            simpleCheckDefinition.getExpr().compile(tableMeta);
          } catch (ModelException e) {
            throw new ModelException(String.format(
                "Cannot drop column '%s', check '%s' fails.\n%s", column, simpleCheckDefinition, e.getMessage()));
          }
        }
        for (ForeignKey foreignKey : tableMeta.foreignKeys().values()) {
          if (foreignKey.getColumn().getName().equals(basicColumn.getName())) {
            throw new ModelException(String.format(
                "Cannot drop column '%s', it is used by foreign key '%s'.", column, foreignKey));
          }
        }
        for (ReverseForeignKey foreignKey : tableMeta.reverseForeignKeys().values()) {
          if (foreignKey.getColumn().getName().equals(basicColumn.getName())) {
            throw new ModelException(String.format(
                "Cannot drop column '%s', it is referenced by foreign key '%s'.", column, foreignKey));
          }
        }
      } finally {
        basicColumn.setDeleted(false);
      }
      basicColumn.setDeleted(true);
      if (basicColumn.isIndexed()) {
        if (basicColumn.isUnique()) {
          metaRepo.dropUniqueIndex(tableMeta, basicColumn);
        } else {
          metaRepo.dropMultiIndex(tableMeta, basicColumn);
        }
      }
      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAlterColumn extends Node implements MetaStatement {
    private final Ident table;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {
      MaterializedTableMeta tableMeta = metaRepo.table(table);
      if (tableMeta.indexOf(new Ident(column.getName())) < 0) {
        throw new ModelException(
            String.format("Cannot alter column '%s', column does not exist.", column.getName()));
      }
      BasicColumn originalColumn = tableMeta.column(new Ident(column.getName()));
      if (originalColumn.getType() != column.getType()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', type has to remain the same.", column.getName()));
      }
      if (originalColumn.isNullable() && !column.isNullable()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', column has to remain nullable.", column.getName()));
      }
      if (!originalColumn.isUnique() && column.isUnique()) {
        throw new ModelException(
            String.format("Cannot alter column '%s', column cannot be unique.", column.getName()));
      }
      if (originalColumn.isUnique() && !column.isUnique()) {
        originalColumn.setIndexed(false);
        metaRepo.dropUniqueIndex(tableMeta, originalColumn);
      }
      originalColumn.setNullable(column.isNullable());
      originalColumn.setUnique(column.isUnique());
      return new MetaStatementResult();
    }
  }
}
