package com.cosyan.db.sql;

import static com.cosyan.db.sql.SyntaxTree.assertType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lock.ResourceLock;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DerivedTables.AggrTableMeta;
import com.cosyan.db.model.DerivedTables.AliasedTableMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.JoinTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.SortedTableMeta;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
import com.cosyan.db.sql.SyntaxTree.Node;
import com.cosyan.db.sql.SyntaxTree.ResourceHolder;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.cosyan.db.sql.Tokens.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class SelectStatement {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Select extends Node implements Statement {
    private final ImmutableList<Expression> columns;
    private final Table table;
    private final Optional<Expression> where;
    private final Optional<ImmutableList<Expression>> groupBy;
    private final Optional<Expression> having;
    private final Optional<ImmutableList<Expression>> orderBy;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(metaRepo);
      ExposedTableMeta filteredTable = filteredTable(metaRepo, sourceTable, where);
      DerivedTableMeta fullTable;
      if (isAggregation(columns) || groupBy.isPresent()) {
        KeyValueTableMeta intermediateTable = keyValueTable(metaRepo, filteredTable, groupBy);
        List<AggrColumn> aggrColumns = new LinkedList<>();
        ImmutableMap<String, ColumnMeta> tableColumns = tableColumns(metaRepo, intermediateTable, columns,
            aggrColumns);
        DerivedColumn havingColumn = havingExpression(metaRepo, intermediateTable, having,
            aggrColumns);
        fullTable = new DerivedTableMeta(new AggrTableMeta(
            intermediateTable,
            ImmutableList.copyOf(aggrColumns),
            havingColumn), tableColumns);
      } else {
        ImmutableMap<String, ColumnMeta> tableColumns = tableColumns(metaRepo, filteredTable, columns);
        fullTable = new DerivedTableMeta(filteredTable, tableColumns);
      }
      if (orderBy.isPresent()) {
        ImmutableList<OrderColumn> orderColumns = orderColumns(metaRepo, fullTable, orderBy.get());
        return new SortedTableMeta(fullTable, orderColumns);
      } else {
        return fullTable;
      }
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      table.collectLocks(locks);
    }

    @Override
    public Result execute(MetaRepo metaRepo) throws ModelException, IOException, IndexException {
      ExposedTableReader reader = compile(metaRepo).reader();
      List<ImmutableList<Object>> values = new ArrayList<>();
      Object[] row = reader.read();
      while (row != null) {
        values.add(ImmutableList.copyOf(row));
        row = reader.read();
      }
      return new QueryResult(reader.getColumns().keySet().asList(), values);
    }

    @Override
    public void rollback() {
    }

    @Override
    public void commit() throws IOException {
    }

    private ImmutableMap<String, ColumnMeta> tableColumns(
        MetaRepo metaRepo,
        TableMeta sourceTable,
        ImmutableList<Expression> columns,
        List<AggrColumn> aggrColumns) throws ModelException {
      ImmutableMap.Builder<String, ColumnMeta> tableColumns = ImmutableMap.builder();
      int i = 0;
      for (Expression expr : columns) {
        tableColumns.put(expr.getName("_c" + (i++)), expr.compile(sourceTable, metaRepo, aggrColumns));
      }
      return tableColumns.build();
    }

    private ImmutableMap<String, ColumnMeta> tableColumns(
        MetaRepo metaRepo,
        ExposedTableMeta sourceTable,
        ImmutableList<Expression> columns) throws ModelException {
      ImmutableMap.Builder<String, ColumnMeta> tableColumns = ImmutableMap.builder();
      int i = 0;
      for (Expression expr : columns) {
        if (expr instanceof AsteriskExpression) {
          for (String columnName : sourceTable.columns().keySet()) {
            tableColumns.put(columnName,
                new IdentExpression(new Ident(columnName)).compile(sourceTable, metaRepo));
          }
        } else {
          tableColumns.put(expr.getName("_c" + (i++)), expr.compile(sourceTable, metaRepo));
        }
      }
      return tableColumns.build();
    }

    private ExposedTableMeta filteredTable(
        MetaRepo metaRepo, ExposedTableMeta sourceTable, Optional<Expression> where) throws ModelException {
      if (where.isPresent()) {
        DerivedColumn whereColumn = where.get().compile(sourceTable, metaRepo);
        assertType(DataTypes.BoolType, whereColumn.getType());
        return new FilteredTableMeta(sourceTable, whereColumn);
      } else {
        return sourceTable;
      }
    }

    private DerivedColumn havingExpression(MetaRepo metaRepo, TableMeta sourceTable,
        Optional<Expression> having, List<AggrColumn> aggrColumns) throws ModelException {
      if (having.isPresent()) {
        DerivedColumn havingColumn = having.get().compile(sourceTable, metaRepo, aggrColumns);
        assertType(DataTypes.BoolType, havingColumn.getType());
        return havingColumn;
      } else {
        return ColumnMeta.TRUE_COLUMN;
      }
    }

    private boolean isAggregation(ImmutableList<Expression> columns) {
      return columns.stream().anyMatch(column -> column.isAggregation() == AggregationExpression.YES);
    }

    private KeyValueTableMeta keyValueTable(
        MetaRepo metaRepo,
        ExposedTableMeta sourceTable,
        Optional<ImmutableList<Expression>> groupBy) throws ModelException {
      if (groupBy.isPresent()) {
        ImmutableMap.Builder<String, ColumnMeta> keyColumnsBuilder = ImmutableMap.builder();
        for (Expression expr : groupBy.get()) {
          DerivedColumn keyColumn = expr.compile(sourceTable, metaRepo);
          String name = expr.getName(null);
          if (name == null) {
            throw new ModelException("Expression in group by must be named: '" + expr + "'.");
          }
          keyColumnsBuilder.put(name, keyColumn);
        }
        ImmutableMap<String, ColumnMeta> keyColumns = keyColumnsBuilder.build();
        return new KeyValueTableMeta(
            sourceTable,
            keyColumns);
      } else {
        return new KeyValueTableMeta(
            sourceTable,
            TableMeta.wholeTableKeys);
      }
    }

    private ImmutableList<OrderColumn> orderColumns(MetaRepo metaRepo, DerivedTableMeta sourceTable,
        ImmutableList<Expression> orderBy) throws ModelException {
      ImmutableList.Builder<OrderColumn> orderColumnsBuilder = ImmutableList.builder();
      for (Expression expr : orderBy) {
        DerivedColumn column = expr.compile(sourceTable, metaRepo);
        if (column instanceof OrderColumn) {
          orderColumnsBuilder.add((OrderColumn) column);
        } else {
          orderColumnsBuilder.add(new OrderColumn(column, true));
        }
      }
      return orderColumnsBuilder.build();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Table extends Node implements ResourceHolder {
    public abstract ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return metaRepo.table(ident);
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      locks.add(ResourceLock.read(ident));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return select.compile(metaRepo);
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      select.collectLocks(locks);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class JoinExpr extends Table {
    private final Token joinType;
    private final Table left;
    private final Table right;
    private final Expression onExpr;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta leftTable = left.compile(metaRepo);
      ExposedTableMeta rightTable = right.compile(metaRepo);
      ImmutableList.Builder<ColumnMeta> leftJoinColumns = ImmutableList.builder();
      ImmutableList.Builder<ColumnMeta> rightJoinColumns = ImmutableList.builder();
      ImmutableList<BinaryExpression> exprs = ImmutableList
          .copyOf(decompose(onExpr, new LinkedList<BinaryExpression>()));
      for (BinaryExpression expr : exprs) {
        leftJoinColumns.add(expr.getLeft().compile(leftTable, metaRepo));
        rightJoinColumns.add(expr.getRight().compile(rightTable, metaRepo));
      }
      return new JoinTableMeta(joinType, leftTable, rightTable, leftJoinColumns.build(), rightJoinColumns.build());
    }

    private List<BinaryExpression> decompose(Expression expr, LinkedList<BinaryExpression> collector)
        throws ModelException {
      if (expr instanceof BinaryExpression) {
        BinaryExpression binaryExpr = (BinaryExpression) expr;
        if (binaryExpr.getToken().is(Tokens.AND)) {
          decompose(binaryExpr.getLeft(), collector);
          decompose(binaryExpr.getRight(), collector);
        } else if (binaryExpr.getToken().is(Tokens.EQ)) {
          collector.add(binaryExpr);
        } else {
          throw new ModelException(
              "Only 'and' and '=' binary expressions are allowed in the 'on' expression of joins.");
        }
      }
      return collector;
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      left.collectLocks(locks);
      right.collectLocks(locks);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public DerivedColumn compile(
        TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) throws ModelException {
      return expr.compile(sourceTable, metaRepo, aggrColumns);
    }

    @Override
    public AggregationExpression isAggregation() {
      return expr.isAggregation();
    }

    @Override
    public String getName(String def) {
      return ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsTable extends Table {
    private final Ident ident;
    private final Table table;

    @Override
    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return new AliasedTableMeta(ident, table.compile(metaRepo));
    }

    @Override
    public void collectLocks(List<ResourceLock> locks) {
      table.collectLocks(locks);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {

    @Override
    public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo, List<AggrColumn> aggrColumns) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName(String def) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.NO;
    }
  }
}
