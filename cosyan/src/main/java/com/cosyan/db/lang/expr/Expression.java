package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.MaterializedTableMeta.MaterializedColumn;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.Column;
import com.cosyan.db.transaction.MetaResources;
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

  public ColumnMeta compile(
      TableMeta sourceTable) throws ModelException {
    ExtraInfoCollector collector = new ExtraInfoCollector();
    ColumnMeta column = compile(sourceTable, collector);
    if (!collector.aggrColumns().isEmpty()) {
      throw new ModelException("Aggregators are not allowed here.");
    }
    return column;
  }

  public abstract ColumnMeta compile(
      TableMeta sourceTable,
      ExtraInfoCollector collector) throws ModelException;

  public String getName(String def) {
    return def;
  }

  public abstract AggregationExpression isAggregation();

  public abstract String print();

  public abstract MetaResources readResources(MaterializedTableMeta tableMeta) throws ModelException;

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class UnaryExpression extends Expression {
    private final Token token;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, ExtraInfoCollector collector) throws ModelException {
      if (token.is(Tokens.NOT)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, collector);
        SyntaxTree.assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumnWithDeps(DataTypes.BoolType, exprColumn.tableDependencies()) {

          @Override
          public Object getValue(SourceValues values) throws IOException {
            return !((Boolean) exprColumn.getValue(values));
          }
        };
      } else if (token.is(Tokens.ASC)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, collector);
        return new OrderColumn(exprColumn, true);
      } else if (token.is(Tokens.DESC)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, collector);
        return new OrderColumn(exprColumn, false);
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compile(sourceTable, collector);
        return new DerivedColumnWithDeps(DataTypes.BoolType, exprColumn.tableDependencies()) {

          @Override
          public Object getValue(SourceValues values) throws IOException {
            return exprColumn.getValue(values) != DataTypes.NULL;
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compile(sourceTable, collector);
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
    public AggregationExpression isAggregation() {
      return expr.isAggregation();
    }

    @Override
    public String print() {
      return token.getString() + " " + expr.print();
    }

    @Override
    public MetaResources readResources(MaterializedTableMeta tableMeta) throws ModelException {
      return expr.readResources(tableMeta);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IdentExpression extends Expression {
    private final Ident ident;

    @Override
    public ColumnMeta compile(
        TableMeta sourceTable, ExtraInfoCollector collector) throws ModelException {
      Column column = sourceTable.column(ident);
      if (column.usesSourceValues()) {
        final int index = column.getIndex();
        return new DerivedColumnWithDeps(column.getMeta().getType(), new TableDependencies()) {
          @Override
          public Object getValue(SourceValues values) {
            return values.sourceValue(index);
          }
        };
      } else {
        TableDependencies tableDependencies = new TableDependencies();
        final MaterializedColumn materializedColumn = (MaterializedColumn) column;
        tableDependencies.addTableDependency(materializedColumn);
        return new DerivedColumnWithDeps(column.getMeta().getType(), tableDependencies) {
          @Override
          public Object getValue(SourceValues values) throws IOException {
            return values.refTableValue(materializedColumn);
          }
        };
      }
    }

    @Override
    public String getName(String def) {
      if (ident.isSimple()) {
        return ident.getString();
      } else {
        return ident.last();
      }
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.NO;
    }

    @Override
    public String print() {
      return ident.getString();
    }

    @Override
    public MetaResources readResources(MaterializedTableMeta tableMeta) throws ModelException {
      MetaResources readResources = MetaResources.empty();
      for (ForeignKey foreignKey : tableMeta.column(ident).getForeignKeyChain()) {
        readResources = readResources.merge(MetaResources.readTable(foreignKey.getRefTable()));
      }
      return readResources;
    }
  }
}