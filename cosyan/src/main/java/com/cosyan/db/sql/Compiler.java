package com.cosyan.db.sql;

import static com.cosyan.db.sql.SyntaxTree.assertType;

import java.util.List;
import java.util.Optional;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.DerivedTableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.FilteredTableMeta;
import com.cosyan.db.model.TableMeta.KeyValueTableMeta;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.sql.SyntaxTree.AsteriskExpression;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.sql.SyntaxTree.IdentExpression;
import com.cosyan.db.sql.SyntaxTree.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class Compiler {

  private final MetaRepo metaRepo;

  public ExposedTableMeta query(SyntaxTree tree) throws ModelException, ConfigException, ParserException {
    if (!tree.isSelect()) {
      throw new ModelException("Expected select.");
    }
    return ((Select) tree.getRoot()).compile(metaRepo);
  }

  public static ImmutableMap<String, ColumnMeta> tableColumns(
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

  public static ImmutableMap<String, ColumnMeta> tableColumns(
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

  public static ExposedTableMeta filteredTable(
      MetaRepo metaRepo, ExposedTableMeta sourceTable, Optional<Expression> where) throws ModelException {
    if (where.isPresent()) {
      DerivedColumn whereColumn = where.get().compile(sourceTable, metaRepo);
      assertType(DataTypes.BoolType, whereColumn.getType());
      return new FilteredTableMeta(sourceTable, whereColumn);
    } else {
      return sourceTable;
    }
  }

  public static DerivedColumn havingExpression(MetaRepo metaRepo, TableMeta sourceTable,
      Optional<Expression> having, List<AggrColumn> aggrColumns) throws ModelException {
    if (having.isPresent()) {
      DerivedColumn havingColumn = having.get().compile(sourceTable, metaRepo, aggrColumns);
      assertType(DataTypes.BoolType, havingColumn.getType());
      return havingColumn;
    } else {
      return ColumnMeta.TRUE_COLUMN;
    }
  }

  public static boolean isAggregation(ImmutableList<Expression> columns) {
    return columns.stream().anyMatch(column -> column.isAggregation() == AggregationExpression.YES);
  }

  public static KeyValueTableMeta keyValueTable(
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

  public static ImmutableList<OrderColumn> orderColumns(MetaRepo metaRepo, DerivedTableMeta sourceTable,
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
