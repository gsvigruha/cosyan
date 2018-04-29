package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class SyntaxTree {

  public static enum AggregationExpression {
    YES, NO, EITHER
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static abstract class Node {

  }

  public static interface Statement {
    public MetaResources compile(MetaRepo metaRepo) throws ModelException;

    public Result execute(Resources resources) throws RuleException, IOException;

    public void cancel();
  }

  public static interface MetaStatement {

    public Result execute(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, IndexException, IOException, GrantException;

    public boolean log();
  }

  public static void assertType(DataType<?> expectedType, DataType<?> dataType, Expression expr) throws ModelException {
    if (!expectedType.javaClass().equals(dataType.javaClass())) {
      throw new ModelException(
          "Data type " + dataType + " did not match expected type " + expectedType + ".", expr);
    }
  }

  public static void assertType(DataType<?> expectedType, DataType<?> dataType, Token token) throws ModelException {
    if (!expectedType.javaClass().equals(dataType.javaClass())) {
      throw new ModelException(
          "Data type " + dataType + " did not match expected type " + expectedType + ".", token);
    }
  }
  
  public static void assertType(DataType<?> expectedType, DataType<?> dataType) throws ModelException {
    if (!expectedType.javaClass().equals(dataType.javaClass())) {
      throw new ModelException(
          "Data type " + dataType + " did not match expected type " + expectedType + ".");
    }
  }
}
