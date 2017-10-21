package com.cosyan.db.model;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DataTypes.DataType;

import lombok.Data;

@Data
public class Rule {

  private final String name;
  private final Expression expr;
  protected final transient ColumnMeta column;

  public Rule(String name, ColumnMeta column, Expression expr) {
    this.name = name;
    this.column = column;
    this.expr = expr;
  }

  public String name() {
    return name;
  }

  public String print() {
    return expr.print();
  }

  public DataType<?> getType() {
    return column.getType();
  }

  public void reCompile(MaterializedTableMeta tableMeta) throws ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      parser.parseExpression(lexer.tokenize(expr.print())).compile(tableMeta);
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }

  @Override
  public String toString() {
    return name + " [" + expr.print() + "]";
  }

  public BooleanRule toBooleanRule() {
    return new BooleanRule(name, column, expr);
  }

  public static class BooleanRule extends Rule {
    public BooleanRule(String name, ColumnMeta column, Expression expr) {
      super(name, column, expr);
      assert column.getType() == DataTypes.BoolType;
    }

    public boolean check(SourceValues values) {
      return (boolean) column.getValue(values);
    }
  }
}
