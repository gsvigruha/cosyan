package com.cosyan.db.sql;

import static com.cosyan.db.sql.SyntaxTree.assertType;

import java.util.List;
import java.util.Optional;

import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.FilteredTableMeta;
import com.cosyan.db.model.TableMeta.GroupByTableMeta;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.sql.SyntaxTree.AsteriskExpression;
import com.cosyan.db.sql.SyntaxTree.Expression;
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
      if (expr instanceof AsteriskExpression) {
        tableColumns.putAll(sourceTable.columns());
      } else {
        if (expr.isAggregation() == AggregationExpression.YES) {
          tableColumns.put(expr.getName("_c" + (i++)), expr.compile(sourceTable, metaRepo, aggrColumns));
        } else {
          tableColumns.put(expr.getName("_c" + (i++)), expr.compile(sourceTable, metaRepo, aggrColumns));
        }
      }
    }
    return tableColumns.build();
  }

  public static TableMeta filteredTable(
      MetaRepo metaRepo, ExposedTableMeta sourceTable, Optional<Expression> where) throws ModelException {
    if (where.isPresent()) {
      DerivedColumn whereColumn = where.get().compile(sourceTable, metaRepo);
      assertType(DataTypes.BoolType, whereColumn.getType());
      return new FilteredTableMeta(sourceTable, whereColumn);
    } else {
      return sourceTable;
    }
  }

  public static boolean hasAggregateColumns(ImmutableList<Expression> columns) throws ModelException {
    for (Expression expr : columns) {
      if (expr.isAggregation() == AggregationExpression.YES) {
        return true;
      }
    }
    return false;
  }

  public static GroupByTableMeta groupByTable(MetaRepo metaRepo, TableMeta sourceTable,
      Optional<ImmutableList<Expression>> groupBy) throws ModelException {
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
    return new GroupByTableMeta(
        sourceTable,
        keyColumns,
        sourceTable.columns());
  }
}
