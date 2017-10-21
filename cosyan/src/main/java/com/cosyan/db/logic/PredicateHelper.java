package com.cosyan.db.logic;

import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Expression.IdentExpression;
import com.cosyan.db.lang.expr.Expression.UnaryExpression;
import com.cosyan.db.lang.expr.Literals.Literal;
import com.cosyan.db.lang.expr.Literals.LongLiteral;
import com.cosyan.db.lang.expr.Literals.StringLiteral;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.model.Ident;
import com.google.common.collect.ImmutableList;

import lombok.Data;

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
