package com.cosyan.db.lang.sql;

import static com.cosyan.db.lang.sql.SyntaxTree.assertType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.Expression.ExtraInfoCollector;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.sql.Result.QueryResult;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.logic.PredicateHelper;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DerivedTables.AliasedTableMeta;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.DistinctTableMeta;
import com.cosyan.db.model.DerivedTables.FilteredTableMeta;
import com.cosyan.db.model.DerivedTables.GlobalAggrTableMeta;
import com.cosyan.db.model.DerivedTables.IndexFilteredTableMeta;
import com.cosyan.db.model.DerivedTables.JoinTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.SortedTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;

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

    public ExposedTableMeta compileTable(MetaRepo metaRepo) throws ModelException {
      ExposedTableMeta sourceTable = table.compile(metaRepo);
      ExtraInfoCollector collector = new ExtraInfoCollector();
      ExposedTableMeta filteredTable = filteredTable(metaRepo, sourceTable, where, collector);
      DerivedTableMeta fullTable;
      if (groupBy.isPresent()) {
        KeyValueTableMeta intermediateTable = keyValueTable(metaRepo, filteredTable, groupBy.get());
        ImmutableMap<String, ColumnMeta> tableColumns = tableColumns(metaRepo, intermediateTable, columns, collector);
        ColumnMeta havingColumn = havingExpression(metaRepo, intermediateTable, having, collector);
        fullTable = new DerivedTableMeta(new KeyValueAggrTableMeta(
            intermediateTable,
            collector.aggrColumns(),
            havingColumn), tableColumns);
      } else {
        ImmutableMap<String, ColumnMeta> tableColumns = tableColumns(metaRepo, filteredTable, columns, collector);
        if (collector.aggrColumns().isEmpty()) {
          fullTable = new DerivedTableMeta(filteredTable, tableColumns);
        } else {
          KeyValueTableMeta keyValueTableMeta = new KeyValueTableMeta(
              filteredTable,
              TableMeta.wholeTableKeys);
          // Recompile with keyValueTableMeta to catch consistency issues.
          tableColumns(metaRepo, keyValueTableMeta, columns, new ExtraInfoCollector());
          fullTable = new DerivedTableMeta(new GlobalAggrTableMeta(
              keyValueTableMeta,
              collector.aggrColumns(),
              ColumnMeta.TRUE_COLUMN), tableColumns);  
        }
      }

      ExposedTableMeta distinctTable;
      if (distinct) {
        distinctTable = new DistinctTableMeta(fullTable);
      } else {
        distinctTable = fullTable;
      }

      ExposedTableMeta finalTable;
      if (orderBy.isPresent()) {
        ImmutableList<OrderColumn> orderColumns = orderColumns(metaRepo, distinctTable, orderBy.get());
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
      ExposedTableReader reader = tableMeta.reader(resources);
      List<ImmutableList<Object>> values = new ArrayList<>();
      SourceValues row = reader.read();
      while (!row.isEmpty()) {
        values.add(row.toList());
        row = reader.read();
      }
      reader.close();
      return new QueryResult(reader.getColumns().keySet().asList(), values);
    }

    @Override
    public void cancel() {

    }

    private ImmutableMap<String, ColumnMeta> tableColumns(
        MetaRepo metaRepo,
        TableMeta sourceTable,
        ImmutableList<Expression> columns,
        ExtraInfoCollector collector) throws ModelException {
      LinkedListMultimap<String, ColumnMeta> tableColumns = LinkedListMultimap.create();
      int i = 0;
      for (Expression expr : columns) {
        if (expr instanceof AsteriskExpression) {
          if (!(sourceTable instanceof ExposedTableMeta)) {
            throw new ModelException("Asterisk experssion is not allowed here.");
          }
          for (String columnName : ((ExposedTableMeta) sourceTable).columnNames()) {
            tableColumns.put(columnName,
                FuncCallExpression.of(new Ident(columnName)).compileColumn(sourceTable));
          }
        } else {
          CompiledObject obj = expr.compile(sourceTable, collector);
          if (obj instanceof ColumnMeta) {
            tableColumns.put(expr.getName("_c" + (i++)), (ColumnMeta) obj);
          } else if (obj instanceof DerivedTableMeta) {
            DerivedTableMeta tableMeta = (DerivedTableMeta) obj;
            for (String name : tableMeta.columnNames()) {
              tableColumns.put(name, tableMeta.getColumns().get(name));
            }
          } else {
            throw new ModelException("Expected table or column.");
          }
        }
      }
      return deduplicateColumns(tableColumns);
    }

    private ImmutableMap<String, ColumnMeta> deduplicateColumns(LinkedListMultimap<String, ColumnMeta> tableColumns)
        throws ModelException {
      ImmutableMap.Builder<String, ColumnMeta> builder = ImmutableMap.builder();
      for (Map.Entry<String, Collection<ColumnMeta>> column : tableColumns.asMap().entrySet()) {
        if (column.getValue().size() > 1) {
          throw new ModelException(String.format("Duplicate column name '%s' in expression.", column.getKey()));
        }
        builder.put(column.getKey(), Iterables.getOnlyElement(column.getValue()));
      }
      return builder.build();
    }

    private ExposedTableMeta filteredTable(
        MetaRepo metaRepo, ExposedTableMeta sourceTable, Optional<Expression> where, ExtraInfoCollector collector)
        throws ModelException {
      if (where.isPresent()) {
        ColumnMeta whereColumn = where.get().compileColumn(sourceTable, collector);
        assertType(DataTypes.BoolType, whereColumn.getType());
        if (sourceTable instanceof MaterializedTableMeta) {
          ImmutableList<VariableEquals> clauses = PredicateHelper.extractClauses(where.get());
          MaterializedTableMeta materializedTableMeta = (MaterializedTableMeta) sourceTable;
          VariableEquals clause = null;
          for (VariableEquals clauseCandidate : clauses) {
            BasicColumn column = materializedTableMeta.column(clauseCandidate.getIdent()).getMeta();
            if ((clause == null && column.isIndexed()) || column.isUnique()) {
              clause = clauseCandidate;
            }
          }
          if (clause != null) {
            return new IndexFilteredTableMeta(materializedTableMeta, whereColumn, clause);
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

    private ColumnMeta havingExpression(MetaRepo metaRepo, TableMeta sourceTable,
        Optional<Expression> having, ExtraInfoCollector collector) throws ModelException {
      if (having.isPresent()) {
        ColumnMeta havingColumn = having.get().compileColumn(sourceTable, collector);
        assertType(DataTypes.BoolType, havingColumn.getType());
        return havingColumn;
      } else {
        return ColumnMeta.TRUE_COLUMN;
      }
    }

    private KeyValueTableMeta keyValueTable(
        MetaRepo metaRepo,
        ExposedTableMeta sourceTable,
        ImmutableList<Expression> groupBy) throws ModelException {
      ImmutableMap.Builder<String, ColumnMeta> keyColumnsBuilder = ImmutableMap.builder();
      for (Expression expr : groupBy) {
        ColumnMeta keyColumn = expr.compileColumn(sourceTable);
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
    }

    private ImmutableList<OrderColumn> orderColumns(MetaRepo metaRepo, ExposedTableMeta sourceTable,
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class Table extends Node {
    public abstract ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableRef extends Table {
    private final Ident ident;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return metaRepo.table(ident);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class TableExpr extends Table {
    private final Select select;

    public ExposedTableMeta compile(MetaRepo metaRepo) throws ModelException {
      return select.compileTable(metaRepo);
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
        leftJoinColumns.add(expr.getLeft().compileColumn(leftTable));
        rightJoinColumns.add(expr.getRight().compileColumn(rightTable));
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsExpression extends Expression {
    private final Ident ident;
    private final Expression expr;

    @Override
    public ColumnMeta compile(
        TableMeta sourceTable, ExtraInfoCollector collector) throws ModelException {
      return expr.compileColumn(sourceTable, collector);
    }

    @Override
    public String getName(String def) {
      return ident.getString();
    }

    @Override
    public String print() {
      return expr.print() + " as " + ident.getString();
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AsteriskExpression extends Expression {

    @Override
    public DerivedColumn compile(TableMeta sourceTable, ExtraInfoCollector collector) {
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
  }
}
