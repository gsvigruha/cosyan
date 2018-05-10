package com.cosyan.db.lang.sql;

import static com.cosyan.db.lang.expr.SyntaxTree.assertType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.expr.SyntaxTree.Node;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.model.AggrTables;
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.AggrTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.AggrTables.NotAggrTableException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DerivedTables.AliasedTableMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.DistinctTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.IndexFilteredTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.SortedTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.JoinTables.JoinTableMeta;
import com.cosyan.db.model.JoinTables.JoinType;
import com.cosyan.db.model.SeekableTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.base.Joiner;
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
    private final boolean distinct;

    private ExposedTableMeta tableMeta;

    public ExposedTableMeta compileTable(TableProvider tableProvider) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(tableProvider);
      ExposedTableMeta filteredTable = filteredTable(sourceTable, where);
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

      ExposedTableMeta finalTable;
      if (orderBy.isPresent()) {
        ImmutableList<OrderColumn> orderColumns = orderColumns(distinctTable, orderBy.get());
        finalTable = new SortedTableMeta(distinctTable, orderColumns);
      } else {
        finalTable = distinctTable;
      }
      return finalTable;
    }

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = compileTable(metaRepo);
      return tableMeta.readResources();
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      List<Object[]> valuess = new ArrayList<>();
      IterableTableReader reader = tableMeta.reader(resources);
      Object[] values = null;
      while ((values = reader.next()) != null) {
        valuess.add(values);
      }
      reader.close();
      return new QueryResult(tableMeta.columnNames(), valuess);
    }

    @Override
    public void cancel() {

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
          if (!(sourceTable instanceof ExposedTableMeta)) {
            throw new ModelException("Asterisk expression is not allowed here.", expr);
          }
          for (String columnName : ((ExposedTableMeta) sourceTable).columnNames()) {
            Expression columnExpr = FuncCallExpression.of(new Ident(columnName, expr.loc()));
            addColumn(columnName, columnExpr.compileColumn(sourceTable), columnExpr, tableColumns);
          }
        } else {
          CompiledObject obj = expr.compile(sourceTable);
          if (obj instanceof ColumnMeta) {
            addColumn(expr.getName("_c" + (i++)), (ColumnMeta) obj, expr, tableColumns);
          } else {
            throw new ModelException("Expected column.", expr);
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

    private ExposedTableMeta filteredTable(
        ExposedTableMeta sourceTable, Optional<Expression> where)
        throws ModelException {
      if (where.isPresent()) {
        ColumnMeta whereColumn = where.get().compileColumn(sourceTable);
        assertType(DataTypes.BoolType, whereColumn.getType(), where.get().loc());
        if (sourceTable instanceof SeekableTableMeta) {
          SeekableTableMeta tableMeta = (SeekableTableMeta) sourceTable;
          VariableEquals clause = PredicateHelper.getBestClause(tableMeta, where.get());
          if (clause != null) {
            return new IndexFilteredTableMeta(tableMeta, whereColumn, clause);
          } else {
            return new FilteredTableMeta(sourceTable, whereColumn);
          }
        } else {
          return new FilteredTableMeta(sourceTable, whereColumn);
        }
      } else {
        return sourceTable;
      }
    }

    private ColumnMeta havingExpression(
        TableMeta sourceTable,
        Optional<Expression> having) throws ModelException {
      if (having.isPresent()) {
        ColumnMeta havingColumn = having.get().compileColumn(sourceTable);
        assertType(DataTypes.BoolType, havingColumn.getType(), having.get().loc());
        return havingColumn;
      } else {
        return ColumnMeta.TRUE_COLUMN;
      }
    }

    private KeyValueTableMeta keyValueTable(
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
      ImmutableMap<String, ColumnMeta> keyColumns = keyColumnsBuilder.build();
      return new KeyValueTableMeta(
          sourceTable,
          keyColumns);
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
    public abstract ExposedTableMeta compile(TableProvider tableProvider) throws ModelException;

    public abstract String print();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    @Override
    public ExposedTableMeta compile(TableProvider tableProvider) throws ModelException {
      return tableProvider.tableMeta(ident);
    }

    @Override
    public String print() {
      return ident.getString();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public ExposedTableMeta compile(TableProvider tableProvider) throws ModelException {
      return select.compileTable(tableProvider);
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

    public ExposedTableMeta compile(TableProvider tableProvider) throws ModelException {
      ExposedTableMeta leftTable = left.compile(tableProvider);
      ExposedTableMeta rightTable = right.compile(tableProvider);
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
    public ExposedTableMeta compile(TableProvider tableProvider) throws ModelException {
      return new AliasedTableMeta(ident, table.compile(tableProvider));
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

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName(String def) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String print() {
      return "*";
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }
}
