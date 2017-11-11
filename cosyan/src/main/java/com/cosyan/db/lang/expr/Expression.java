package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableMeta;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class Expression extends Node {

  public static class ExtraInfoCollector {
    private final List<AggrColumn> aggrColumns = new ArrayList<>();

    public void addAggrColumn(AggrColumn aggrColumn) {
      aggrColumns.add(aggrColumn);
    }

    public int numAggrColumns() {
      return aggrColumns.size();
    }

    public ImmutableList<AggrColumn> aggrColumns() {
      return ImmutableList.copyOf(aggrColumns);
    }
  }

  public ColumnMeta compileColumn(
      TableMeta sourceTable) throws ModelException {
    ExtraInfoCollector collector = new ExtraInfoCollector();
    ColumnMeta column = compileColumn(sourceTable, collector);
    if (!collector.aggrColumns().isEmpty()) {
      throw new ModelException("Aggregators are not allowed here.");
    }
    return column;
  }

  public ColumnMeta compileColumn(
      TableMeta sourceTable,
      ExtraInfoCollector collector) throws ModelException {
    CompiledObject obj = compile(sourceTable, collector);
    return (ColumnMeta) obj;
  }

  public CompiledObject compile(TableMeta sourceTable) throws ModelException {
    ExtraInfoCollector collector = new ExtraInfoCollector();
    CompiledObject obj = compile(sourceTable, collector);
    if (!collector.aggrColumns().isEmpty()) {
      throw new ModelException("Aggregators are not allowed here.");
    }
    return obj;
  }

  public abstract CompiledObject compile(
      TableMeta sourceTable,
      ExtraInfoCollector collector) throws ModelException;

  public String getName(String def) {
    return def;
  }

  public abstract String print();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Token token;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, ExtraInfoCollector collector) throws ModelException {
      if (token.is(Tokens.NOT)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable, collector);
        SyntaxTree.assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumnWithDeps(DataTypes.BoolType, exprColumn.tableDependencies()) {

          @Override
          public Object getValue(SourceValues values) throws IOException {
            return !((Boolean) exprColumn.getValue(values));
          }
        };
      } else if (token.is(Tokens.ASC)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable, collector);
        return new OrderColumn(exprColumn, true);
      } else if (token.is(Tokens.DESC)) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable, collector);
        return new OrderColumn(exprColumn, false);
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable, collector);
        return new DerivedColumnWithDeps(DataTypes.BoolType, exprColumn.tableDependencies()) {

          @Override
          public Object getValue(SourceValues values) throws IOException {
            return exprColumn.getValue(values) != DataTypes.NULL;
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compileColumn(sourceTable, collector);
        return new DerivedColumnWithDeps(DataTypes.BoolType, exprColumn.tableDependencies()) {

          @Override
          public Object getValue(SourceValues values) throws IOException {
            return exprColumn.getValue(values) == DataTypes.NULL;
          }
        };
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public String print() {
      return token.getString() + " " + expr.print();
    }
  }
}