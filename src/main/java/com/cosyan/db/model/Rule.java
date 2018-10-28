/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.model;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.View;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.Resources;

import lombok.Data;

@Data
@Immutable
public abstract class Rule {

  private final String name;
  private final Expression expr;
  protected final boolean nullIsTrue;
  protected final transient ColumnMeta column;
  private final transient TableDependencies deps;

  public Rule(
      String name,
      ColumnMeta column,
      Expression expr,
      boolean nullIsTrue,
      TableDependencies deps) {
    this.name = name;
    this.column = column;
    this.expr = expr;
    this.deps = deps;
    this.nullIsTrue = nullIsTrue;
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

  public void reCompile() throws ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    try {
      parser.parseExpression(lexer.tokenizeExpression(expr.print())).compileColumn(tableMeta());
    } catch (ParserException e) {
      throw new RuntimeException(e); // This should not happen.
    }
  }

  @Override
  public String toString() {
    return name + " [" + expr.print() + "]";
  }

  public abstract TableMeta tableMeta();

  public abstract boolean check(Resources resources, Object[] values) throws IOException;

  public static class BooleanRule extends Rule {

    private final transient SeekableTableMeta table;

    public BooleanRule(String name, SeekableTableMeta table, ColumnMeta column, Expression expr,
        boolean nullIsTrue, TableDependencies deps) {
      super(name, column, expr, nullIsTrue, deps);
      assert column.getType() == DataTypes.BoolType;
      this.table = table;
    }

    public boolean check(Resources resources, long fileIndex) throws IOException {
      Object[] values = table.get(resources, fileIndex).getValues();
      return check(resources, values);
    }

    @Override
    public boolean check(Resources resources, Object[] sourceValues) throws IOException {
      Object check = column.value(sourceValues, resources, TableContext.EMPTY);
      if (check == null) {
        return nullIsTrue;
      }
      return (boolean) check;
    }

    public String print(Resources resources, long fileIndex) throws IOException {
      return getExpr().print();
    }

    public SeekableTableMeta getTable() {
      return table;
    }

    @Override
    public TableMeta tableMeta() {
      return table;
    }
  }

  public static class BooleanViewRule extends Rule {

    private final transient View view;

    public BooleanViewRule(String name, View view, ColumnMeta column, Expression expr,
        boolean nullIsTrue, TableDependencies deps) {
      super(name, column, expr, nullIsTrue, deps);
      assert column.getType() == DataTypes.BoolType;
      this.view = view;
    }

    @Override
    public boolean check(Resources resources, Object[] sourceValues) throws IOException {
      Object[] values = view.refTable().values(sourceValues, resources, TableContext.EMPTY);
      Object check = column.value(values, resources, TableContext.EMPTY);
      if (check == null) {
        return nullIsTrue;
      }
      return (boolean) check;
    }

    public String print(Resources resources, Object[] sourceValues) throws IOException {
      return getExpr().print();
    }

    public View getView() {
      return view;
    }

    @Override
    public TableMeta tableMeta() {
      return view.table();
    }
  }
}
