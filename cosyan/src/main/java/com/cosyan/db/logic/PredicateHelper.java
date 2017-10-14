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

public class PredicateHelper {

  @Data
  public static class VariableEquals {
    private final Ident ident;
    private final Object value;
  }

  public static ImmutableList<VariableEquals> extractClauses(Expression expression) {
    List<VariableEquals> predicates = new ArrayList<>();
    extractClauses(expression, predicates);
    return ImmutableList.copyOf(predicates);
  }

  private static void extractClauses(Expression node, List<VariableEquals> predicates) {
    if (node instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) node;
      if (binaryExpression.getToken().is(Tokens.AND)) {
        extractClauses(binaryExpression.getLeft(), predicates);
        extractClauses(binaryExpression.getRight(), predicates);
      } else if (binaryExpression.getToken().is(Tokens.EQ)) {
        collectClause(binaryExpression.getLeft(), binaryExpression.getRight(), predicates);
        collectClause(binaryExpression.getRight(), binaryExpression.getLeft(), predicates);
      }
    } else if (node instanceof UnaryExpression) {
      // TODO
    }
  }

  private static void collectClause(Expression first, Expression second, List<VariableEquals> lookupsToCollect) {
    if (first instanceof IdentExpression && second instanceof Literal) {
      Ident ident = ((IdentExpression) first).getIdent();
      if (second instanceof StringLiteral) {
        lookupsToCollect.add(new VariableEquals(ident, ((StringLiteral) second).getValue()));
      } else if (second instanceof LongLiteral) {
        lookupsToCollect.add(new VariableEquals(ident, ((LongLiteral) second).getValue()));
      } 
    }
  }
}