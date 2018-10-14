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
package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.expr.Node;
import com.cosyan.db.lang.expr.Statements.Statement;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaReader;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.meta.TableProvider.TableWithOwner;
import com.cosyan.db.model.AggrTables;
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.AggrTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.AggrTables.NotAggrTableException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.CompiledObject.ColumnList;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DerivedTables.AliasedTableMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.DistinctTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.IndexFilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.LimitedTableMeta;
import com.cosyan.db.model.DerivedTables.SortedTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.JoinTables.JoinTableMeta;
import com.cosyan.db.model.JoinTables.JoinType;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableContext;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SelectStatement extends Statement {

  private final Select select;

  private ExposedTableMeta tableMeta;
  private IterableTableReader reader;

  @Override
  public MetaResources compile(MetaReader metaRepo, AuthToken authToken) throws ModelException {
    tableMeta = select.compileTable(metaRepo, authToken.username());
    return tableMeta.readResources();
  }

  @Override
  public Result execute(Resources resources) throws RuleException, IOException {
    List<Object[]> valuess = new ArrayList<>();
    reader = tableMeta.reader(resources, TableContext.EMPTY);
    try {
      Object[] values = null;
      while ((values = reader.next()) != null) {
        valuess.add(values);
      }
    } finally {
      reader.close();
    }
    return new QueryResult(tableMeta.columnNames(), tableMeta.columnTypes(), valuess);
  }

  @Override
  public void cancel() {
    reader.cancel();
  }

  @Data
  public static class Select {
    private final ImmutableList<Expression> columns;
    private final Table table;
    private final Optional<Expression> where;
    private final Optional<ImmutableList<Expression>> groupBy;
    private final Optional<Expression> having;
    private final Optional<ImmutableList<Expression>> orderBy;
    private final boolean distinct;
    private final Optional<Long> limit;

    public ExposedTableMeta compileTable(TableProvider tableProvider, String owner) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(tableProvider, owner);
      ExposedTableMeta filteredTable;
      if (where.isPresent()) {
        filteredTable = filteredTable(sourceTable, where.get());
      } else {
        filteredTable = sourceTable;
      }
      DerivedTableMeta fullTable;
      if (groupBy.isPresent()) {
        KeyValueTableMeta intermediateTable = keyValueTable(filteredTable, groupBy.get());
        AggrTables aggrTable = new KeyValueAggrTableMeta(intermediateTable);
        TableColumns tableColumns = tableColumns(aggrTable, columns);
        ColumnMeta havingColumn = havingExpression(aggrTable, having);
        aggrTable.setHavingColumn(havingColumn);
        fullTable = new DerivedTableMeta(aggrTable, tableColumns.columns);
      } else {
        fullTable = selectTable(filteredTable, columns);
      }

      ExposedTableMeta distinctTable;
      if (distinct) {
        distinctTable = new DistinctTableMeta(fullTable);
      } else {
        distinctTable = fullTable;
      }

      ExposedTableMeta orderedTable;
      if (orderBy.isPresent()) {
        ImmutableList<OrderColumn> orderColumns = orderColumns(distinctTable, orderBy.get());
        orderedTable = new SortedTableMeta(distinctTable, orderColumns);
      } else {
        orderedTable = distinctTable;
      }

      ExposedTableMeta limitedTable;
      if (limit.isPresent()) {
        limitedTable = new LimitedTableMeta(orderedTable, limit.get());
      } else {
        limitedTable = orderedTable;
      }
      return limitedTable;
    }

    public static DerivedTableMeta selectTable(
        IterableTableMeta sourceTable,
        ImmutableList<Expression> columns) throws ModelException {
      try {
        TableColumns tableColumns = tableColumns(sourceTable, columns);
        return new DerivedTableMeta(sourceTable, tableColumns.columns);
      } catch (NotAggrTableException e) {
        AggrTables aggrTable = new GlobalAggrTableMeta(
            new KeyValueTableMeta(
                sourceTable,
                TableMeta.wholeTableKeys));
        // Columns have aggregations, recompile with KeyValueTableMeta.
        TableColumns tableColumns = tableColumns(aggrTable, columns);
        return new DerivedTableMeta(aggrTable, tableColumns.columns);
      }
    }

    @Data
    public static class TableColumns {
      private final ImmutableMap<String, ColumnMeta> columns;
      private final ImmutableList<TableMeta> tables;
    }

    public static TableColumns tableColumns(
        TableMeta sourceTable,
        ImmutableList<Expression> columns) throws NotAggrTableException, ModelException {
      ImmutableList.Builder<TableMeta> tables = ImmutableList.builder();
      LinkedHashMap<String, ColumnMeta> tableColumns = new LinkedHashMap<>();
      int i = 0;
      for (Expression expr : columns) {
        if (expr instanceof AsteriskExpression) {
          AsteriskExpression asteriskExpression = (AsteriskExpression) expr;
          if (!(sourceTable instanceof ExposedTableMeta)) {
            throw new ModelException("Asterisk expression is not allowed here.", expr);
          }
          for (String columnName : ((ExposedTableMeta) sourceTable).columnNames()) {
            Expression columnExpr = FuncCallExpression.of(new Ident(columnName, expr.loc()));
            if (!asteriskExpression.excludes(columnExpr.getName(null))) {
              addColumn(columnName, columnExpr.compileColumn(sourceTable), columnExpr, tableColumns);
            }
          }
        } else {
          CompiledObject obj = expr.compile(sourceTable);
          if (obj instanceof ColumnMeta) {
            addColumn(expr.getName("_c" + (i++)), (ColumnMeta) obj, expr, tableColumns);
          } else if (obj instanceof ColumnList) {
            for (Map.Entry<String, ColumnMeta> columnMeta : ((ColumnList) obj).getColumns().entrySet()) {
              addColumn(columnMeta.getKey(), columnMeta.getValue(), expr, tableColumns);
            }
          } else {
            throw new ModelException("Expected column or column list.", expr);
          }
        }
      }
      return new TableColumns(ImmutableMap.copyOf(tableColumns), tables.build());
    }

    private static void addColumn(
        String columnName,
        ColumnMeta columnMeta,
        Expression columnExpr,
        LinkedHashMap<String, ColumnMeta> tableColumns) throws ModelException {
      if (tableColumns.containsKey(columnName)) {
        throw new ModelException(String.format("Duplicate column name '%s'.", columnName), columnExpr);
      }
      tableColumns.put(columnName, columnMeta);
    }

    public static ExposedTableMeta filteredTable(
        ExposedTableMeta sourceTable, Expression where)
        throws ModelException {
      ColumnMeta whereColumn = where.compileColumn(sourceTable);
      Node.assertType(DataTypes.BoolType, whereColumn.getType(), where.loc());
      if (sourceTable instanceof SeekableTableMeta) {
        SeekableTableMeta tableMeta = (SeekableTableMeta) sourceTable;
        VariableEquals clause = PredicateHelper.getBestClause(tableMeta, where);
        if (clause != null) {
          return new IndexFilteredTableMeta(tableMeta, whereColumn, clause);
        } else {
          return new FilteredTableMeta(sourceTable, whereColumn);
        }
      } else {
        return new FilteredTableMeta(sourceTable, whereColumn);
      }
    }

    private ColumnMeta havingExpression(
        TableMeta sourceTable,
        Optional<Expression> having) throws ModelException {
      if (having.isPresent()) {
        ColumnMeta havingColumn = having.get().compileColumn(sourceTable);
        Node.assertType(DataTypes.BoolType, havingColumn.getType(), having.get().loc());
        return havingColumn;
      } else {
        return ColumnMeta.TRUE_COLUMN;
      }
    }

    public static ImmutableMap<String, ColumnMeta> groupByColumns(
        ExposedTableMeta sourceTable,
        ImmutableList<Expression> groupBy) throws ModelException {
      ImmutableMap.Builder<String, ColumnMeta> keyColumnsBuilder = ImmutableMap.builder();
      for (Expression expr : groupBy) {
        ColumnMeta keyColumn = expr.compileColumn(sourceTable);
        String name = expr.getName(null);
        if (name == null) {
          throw new ModelException(String.format("Expression in group by must be named: '%s'.", expr.print()), expr);
        }
        keyColumnsBuilder.put(name, keyColumn);
      }
      return keyColumnsBuilder.build();
    }

    public static KeyValueTableMeta keyValueTable(
        ExposedTableMeta sourceTable,
        ImmutableList<Expression> groupBy) throws ModelException {
      return new KeyValueTableMeta(sourceTable, groupByColumns(sourceTable, groupBy));
    }

    private ImmutableList<OrderColumn> orderColumns(
        ExposedTableMeta sourceTable,
        ImmutableList<Expression> orderBy) throws ModelException {
      ImmutableList.Builder<OrderColumn> orderColumnsBuilder = ImmutableList.builder();
      for (Expression expr : orderBy) {
        ColumnMeta column = expr.compileColumn(sourceTable);
        if (column instanceof OrderColumn) {
          orderColumnsBuilder.add((OrderColumn) column);
        } else {
          orderColumnsBuilder.add(new OrderColumn(column, true));
        }
      }
      return orderColumnsBuilder.build();
    }

    public String print() {
      StringBuilder sb = new StringBuilder();
      sb.append("select ");
      if (distinct) {
        sb.append("distinct ");
      }
      sb.append(Joiner.on(", ").join(columns.stream().map(c -> c.print()).iterator())).append(" ");
      sb.append("from ");
      sb.append(table.print()).append(" ");
      if (where.isPresent()) {
        sb.append("where ").append(where.get().print());
      }
      if (groupBy.isPresent()) {
        sb.append("group by ")
            .append(Joiner.on(", ").join(groupBy.get().stream().map(e -> e.print()).iterator()))
            .append(" ");
      }
      if (having.isPresent()) {
        sb.append("having ").append(having.get().print());
      }
      if (orderBy.isPresent()) {
        sb.append("order by ")
            .append(Joiner.on(", ").join(orderBy.get().stream().map(e -> e.print()).iterator()))
            .append(" ");
      }
      return sb.toString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Table extends Node {
    public abstract ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException;

    public abstract String print();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    @Override
    public ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException {
      return tableProvider.tableMeta(TableWithOwner.of(ident, owner));
    }

    @Override
    public String print() {
      return ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRefChain extends Table {
    private final Ident ident;
    private final Table table;

    @Override
    public ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException {
      return table.compile(tableProvider.tableProvider(ident, owner), owner);
    }

    @Override
    public String print() {
      return ident.getString() + "." + table.print();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException {
      return select.compileTable(tableProvider, owner);
    }

    @Override
    public String print() {
      return select.print();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class JoinExpr extends Table {
    private final Token joinType;
    private final Table left;
    private final Table right;
    private final Expression onExpr;

    public ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException {
      ExposedTableMeta leftTable = left.compile(tableProvider, owner);
      ExposedTableMeta rightTable = right.compile(tableProvider, owner);
      ImmutableList.Builder<ColumnMeta> leftJoinColumns = ImmutableList.builder();
      ImmutableList.Builder<ColumnMeta> rightJoinColumns = ImmutableList.builder();
      ImmutableList<BinaryExpression> exprs = ImmutableList
          .copyOf(decompose(onExpr, new LinkedList<BinaryExpression>()));
      for (BinaryExpression expr : exprs) {
        leftJoinColumns.add(expr.getLeft().compileColumn(leftTable));
        rightJoinColumns.add(expr.getRight().compileColumn(rightTable));
      }
      return new JoinTableMeta(
          JoinType.valueOf(joinType.getString().toUpperCase()),
          leftTable,
          rightTable,
          leftJoinColumns.build(),
          rightJoinColumns.build());
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
          throw new ModelException(String.format(
              "Only 'and' and '=' binary expressions are allowed in the 'on' expression of joins, not '%s'.",
              binaryExpr.getToken().getString()), expr);
        }
      }
      return collector;
    }

    @Override
    public String print() {
      return left.print() + " " + joinType.getString() + " join " + right.print() + " on " + onExpr.print();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public ColumnMeta compile(TableMeta sourceTable) throws ModelException {
      return expr.compileColumn(sourceTable);
    }

    @Override
    public String getName(String def) {
      return ident.getString();
    }

    @Override
    public String print() {
      return expr.print() + " as " + ident.getString();
    }

    @Override
    public Loc loc() {
      return ident.getLoc();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsTable extends Table {
    private final Ident ident;
    private final Table table;

    @Override
    public ExposedTableMeta compile(TableProvider tableProvider, String owner) throws ModelException {
      return new AliasedTableMeta(ident, table.compile(tableProvider, owner));
    }

    @Override
    public String print() {
      return table.print() + " as " + ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {
    private final Loc loc;
    private final Optional<ImmutableList<Ident>> exclude;
    private final Set<String> excludeSet;

    public AsteriskExpression(Loc loc, Optional<ImmutableList<Ident>> exclude) {
      this.loc = loc;
      this.exclude = exclude;
      this.excludeSet = exclude
          .map(l -> l.stream().map(i -> i.getString()).collect(Collectors.toSet()))
          .orElse(ImmutableSet.of());
    }

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      throw new UnsupportedOperationException();
    }

    public boolean excludes(String name) {
      return excludeSet.contains(name);
    }

    @Override
    public String getName(String def) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String print() {
      if (exclude.isPresent()) {
        return "* - (" + Joiner.on(", ").join(excludeSet) + ")";
      } else {
        return "*";
      }
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }
}
