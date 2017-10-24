package com.cosyan.db.lang.expr;

import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableDependencies;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.Column;
import com.cosyan.db.transaction.MetaResources;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public abstract class Expression extends Node {

  public ColumnMeta compile(
      TableMeta sourceTable) throws ModelException {
    TableDependencies deps = new TableDependencies();
    ColumnMeta column = compile(sourceTable, deps);
    if (!deps.getAggrColumns().isEmpty()) {
      throw new ModelException("Aggregators are not allowed here.");
    }
    return column;
  }

  public abstract ColumnMeta compile(
      TableMeta sourceTable,
      TableDependencies deps) throws ModelException;

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
        TableMeta sourceTable, TableDependencies deps) throws ModelException {
      if (token.is(Tokens.NOT)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, deps);
        SyntaxTree.assertType(DataTypes.BoolType, exprColumn.getType());
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(SourceValues values) {
            return !((Boolean) exprColumn.getValue(values));
          }
        };
      } else if (token.is(Tokens.ASC)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, deps);
        return new OrderColumn(exprColumn, true);
      } else if (token.is(Tokens.DESC)) {
        ColumnMeta exprColumn = expr.compile(sourceTable, deps);
        return new OrderColumn(exprColumn, false);
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NOT, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compile(sourceTable, deps);
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(SourceValues values) {
            return exprColumn.getValue(values) != DataTypes.NULL;
          }
        };
      } else if (token.is(Token.concat(Tokens.IS, Tokens.NULL).getString())) {
        ColumnMeta exprColumn = expr.compile(sourceTable, deps);
        return new DerivedColumn(DataTypes.BoolType) {

          @Override
          public Object getValue(SourceValues values) {
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
        TableMeta sourceTable, TableDependencies deps) throws ModelException {
      Column column = sourceTable.column(ident);
      if (column.usesSourceValues()) {
        final int index = column.getIndex();
        return new DerivedColumn(column.getMeta().getType()) {
          @Override
          public Object getValue(SourceValues values) {
            return values.sourceValue(index);
          }
        };
      } else {
        deps.add(column.foreignKeyChain());
        final String tableIdent = column.tableIdent();
        final int index = column.getIndex();
        return new DerivedColumn(column.getMeta().getType()) {
          @Override
          public Object getValue(SourceValues values) {
            return values.refTableValue(tableIdent, index);
          }
        };
      }
    }

    @Override
    public String getName(String def) {
      if (ident.isSimple()) {
        return ident.getString();
      } else {
        return ident.tail().getString();
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