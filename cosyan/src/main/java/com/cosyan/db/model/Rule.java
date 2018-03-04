package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.MaterializedTableMeta.SeekableTableMeta;
import com.cosyan.db.transaction.Resources;

import lombok.Data;

@Data
public class Rule {

  private final String name;
  private final Expression expr;
  protected final boolean nullIsTrue;
  protected final transient SeekableTableMeta table;
  protected final transient ColumnMeta column;
  private final transient TableDependencies deps;

  public Rule(String name, SeekableTableMeta table, ColumnMeta column, Expression expr, TableDependencies deps) {
    this.name = name;
    this.column = column;
    this.expr = expr;
    this.deps = deps;
    this.table = table;
    this.nullIsTrue = true;
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
      parser.parseExpression(lexer.tokenize(expr.print())).compileColumn(tableMeta.reader());
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }

  @Override
  public String toString() {
    return name + " [" + expr.print() + "]";
  }

  public BooleanRule toBooleanRule() {
    return new BooleanRule(name, table, column, expr, deps);
  }

  public static class BooleanRule extends Rule {
    public BooleanRule(String name, SeekableTableMeta table, ColumnMeta column, Expression expr,
        TableDependencies deps) {
      super(name, table, column, expr, deps);
      assert column.getType() == DataTypes.BoolType;
    }

    public boolean check(Resources resources, long fileIndex) throws IOException {
      Object[] values = table.get(resources, fileIndex).getValues();
      Object check = column.value(values, resources);
      if (check == DataTypes.NULL) {
        return nullIsTrue;
      }
      return (boolean) check;
    }
  }
}
