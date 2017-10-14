package com.cosyan.db.sql;

import java.io.IOException;

import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.sql.CreateStatement.ColumnDefinition;
import com.cosyan.db.sql.Result.MetaStatementResult;
import com.cosyan.db.sql.SyntaxTree.MetaStatement;
import com.cosyan.db.sql.SyntaxTree.Node;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class AlterStatement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAddColumn extends Node implements MetaStatement {
    private final String tableName;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {

      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableDropColumn extends Node implements MetaStatement {
    private final String tableName;
    private final String columnName;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {

      return new MetaStatementResult();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AlterTableAlterColumn extends Node implements MetaStatement {
    private final String tableName;
    private final ColumnDefinition column;

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException {

      return new MetaStatementResult();
    }
  }
}
