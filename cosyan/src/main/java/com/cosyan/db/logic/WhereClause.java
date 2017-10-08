package com.cosyan.db.logic;

import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.sql.BinaryExpression;
import com.cosyan.db.sql.Tokens;
import com.google.common.collect.ImmutableList;

import lombok.Data;

import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
import com.cosyan.db.sql.SyntaxTree.Literal;
import com.cosyan.db.sql.SyntaxTree.LongLiteral;
import com.cosyan.db.sql.SyntaxTree.StringLiteral;
import com.cosyan.db.sql.SyntaxTree.UnaryExpression;

public class WhereClause {

  @Data
  public static class IndexLookup {
    private final Ident ident;
    private final Object value;
  }

  public static ImmutableList<IndexLookup> getIndex(Expression whereClause) {
    List<IndexLookup> indexLookups = new ArrayList<>();
    get(whereClause, indexLookups);
    return ImmutableList.copyOf(indexLookups);
  }

  private static void get(Expression node, List<IndexLookup> lookupsToCollect) {
    if (node instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) node;
      if (binaryExpression.getToken().is(Tokens.AND)) {
        get(binaryExpression.getLeft(), lookupsToCollect);
        get(binaryExpression.getRight(), lookupsToCollect);
      } else if (binaryExpression.getToken().is(Tokens.EQ)) {
        collect(binaryExpression.getLeft(), binaryExpression.getRight(), lookupsToCollect);
        collect(binaryExpression.getRight(), binaryExpression.getLeft(), lookupsToCollect);
      }
    } else if (node instanceof UnaryExpression) {
      // TODO
    }
  }

  private static void collect(Expression first, Expression second, List<IndexLookup> lookupsToCollect) {
    if (first instanceof IdentExpression && second instanceof Literal) {
      Ident ident = ((IdentExpression) first).getIdent();
      if (second instanceof StringLiteral) {
        lookupsToCollect.add(new IndexLookup(ident, ((StringLiteral) second).getValue()));
      } else if (second instanceof LongLiteral) {
        lookupsToCollect.add(new IndexLookup(ident, ((LongLiteral) second).getValue()));
      } 
    }
  }
}
