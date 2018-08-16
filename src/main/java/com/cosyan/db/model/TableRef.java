package com.cosyan.db.model;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.TableDefinition.AggRefDefinition;
import com.cosyan.db.lang.expr.TableDefinition.FlatRefDefinition;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class TableRef {

  private final String name;
  private final String expr;
  private final int index;
  private final boolean aggr;
  protected final transient TableMeta tableMeta;

  @Override
  public String toString() {
    return name + " [" + expr + "]";
  }

  public void reCompile(MaterializedTable tableMeta) throws ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      if (aggr) {
        Select select = parser.parseSelect(lexer.tokenizeExpression(expr + ";"));
        tableMeta.createAggRef(new AggRefDefinition(new Ident(name), select));
      } else {
        ImmutableList<Expression> exprs = parser.parseExpressions(lexer.tokenize(expr + ";"));
        tableMeta.createFlatRef(new FlatRefDefinition(new Ident(name), exprs));
      }
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }
}
