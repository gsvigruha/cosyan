package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.SelectStatement;
import com.cosyan.db.lang.sql.SelectStatement.Select.TableColumns;
import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.AggrTables;
import com.cosyan.db.model.AggrTables.NotAggrTableException;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.CompiledObject;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.DerivedTables.DerivedTableMeta;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.DerivedTables.ReferencedDerivedTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.References.ReferencedAggrTableMeta;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FuncCallExpression extends Expression {
  private final Ident ident;
  @Nullable
  private final Expression object;
  private final ImmutableList<Expression> args;

  public FuncCallExpression(Ident ident, @Nullable Expression object, ImmutableList<Expression> args)
      throws ParserException {
    this.ident = ident;
    this.object = object;
    this.args = args;
  }

  public FuncCallExpression(Ident ident) {
    this.ident = ident;
    this.object = null;
    this.args = ImmutableList.of();
  }

  public static FuncCallExpression of(Ident ident) {
    return new FuncCallExpression(ident);
  }

  private boolean isAggr() {
    return BuiltinFunctions.AGGREGATION_NAMES.contains(ident.getString());
  }

  private DerivedColumnWithDeps simpleFunction(TableMeta sourceTable, @Nullable ColumnMeta objColumn)
      throws ModelException {
    SimpleFunction<?> function = BuiltinFunctions.simpleFunction(ident.getString());
    ImmutableList.Builder<ColumnMeta> argColumnsBuilder = ImmutableList.builder();
    if (objColumn != null) {
      argColumnsBuilder.add(objColumn);
    }
    TableDependencies tableDependencies = new TableDependencies();
    for (int i = 0; i < args.size(); i++) {
      ColumnMeta col = args.get(i).compileColumn(sourceTable);
      argColumnsBuilder.add(col);
      // tableDependencies.add(col.tableDependencies());
    }

    MetaResources resources = MetaResources.empty();
    ImmutableSet.Builder<TableMeta> tables = ImmutableSet.builder();
    ImmutableList<ColumnMeta> argColumns = argColumnsBuilder.build();
    for (int i = 0; i < function.getArgTypes().size(); i++) {
      SyntaxTree.assertType(function.getArgTypes().get(i), argColumns.get(i).getType());
      resources = resources.merge(argColumns.get(i).readResources());
      tables.addAll(argColumns.get(i).tables());
    }
    return new DerivedColumnWithDeps(function.getReturnType(), tableDependencies, resources, tables.build()) {

      @Override
      public Object getValue(Object[] values, Resources resources) throws IOException {
        ImmutableList.Builder<Object> paramsBuilder = ImmutableList.builder();
        for (ColumnMeta column : argColumns) {
          paramsBuilder.add(column.getValue(values, resources));
        }
        ImmutableList<Object> params = paramsBuilder.build();
        for (Object param : params) {
          if (param == DataTypes.NULL) {
            return DataTypes.NULL;
          }
        }
        return function.call(params);
      }
    };
  }

  private AggrColumn aggrFunction(TableMeta sourceTable, Expression arg, ExtraInfoCollector collector)
      throws ModelException {
    if (!(sourceTable instanceof AggrTables)) {
      throw new NotAggrTableException();
    }
    AggrTables aggrTable = (AggrTables) sourceTable;
    KeyValueTableMeta keyValueTableMeta = aggrTable.sourceTable();
    int shift = keyValueTableMeta.getKeyColumns().size();
    ColumnMeta argColumn = arg.compileColumn(keyValueTableMeta.getSourceTable());
    final TypedAggrFunction<?> function = BuiltinFunctions.aggrFunction(ident.getString(), argColumn.getType());
    AggrColumn aggrColumn = new AggrColumn(
        aggrTable,
        function.getReturnType(),
        argColumn,
        shift + collector.numAggrColumns(),
        function);
    collector.addAggrColumn(aggrColumn);
    aggrTable.addAggrColumn(aggrColumn);
    return aggrColumn;
  }

  private TableMeta tableFunction(ReferencedMultiTableMeta tableMeta) throws ModelException {
    if (ident.getString().equals("select")) {
      ExtraInfoCollector collector = new ExtraInfoCollector();
      try {
        TableColumns tableColumns = SelectStatement.Select.tableColumns(tableMeta, args, collector);
        return new DerivedTableMeta(tableMeta, tableColumns.getColumns());
      } catch (NotAggrTableException e) {
        ReferencedAggrTableMeta aggrTable = new ReferencedAggrTableMeta(
            new KeyValueTableMeta(
                tableMeta,
                TableMeta.wholeTableKeys), tableMeta.getReverseForeignKey());
        // Columns have aggregations, recompile with an AggrTable.
        TableColumns tableColumns = SelectStatement.Select.tableColumns(aggrTable, args, new ExtraInfoCollector());
        return new ReferencedDerivedTableMeta(aggrTable, tableColumns.getColumns());
      }
    } else {
      throw new ModelException("Wrong func");
    }
  }

  @Override
  public CompiledObject compile(TableMeta sourceTable, ExtraInfoCollector collector)
      throws ModelException {
    if (object == null) {
      if (args.isEmpty()) {
        if (sourceTable.hasTable(ident)) {
          return sourceTable.table(ident);
        } else {
          return sourceTable.column(ident);
        }
      } else {
        if (isAggr()) {
          if (args.size() != 1) {
            throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
          }
          return aggrFunction(sourceTable, Iterables.getOnlyElement(args), collector);
        } else {
          return simpleFunction(sourceTable, null);
        }
      }
    } else {
      if (isAggr()) {
        if (args.size() > 0) {
          throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
        }
        return aggrFunction(sourceTable, object, collector);
      } else { // Not aggregator.
        CompiledObject obj = object.compile(sourceTable);
        if (obj instanceof TableMeta) {
          TableMeta tableMeta = (TableMeta) obj;
          if (args.isEmpty()) {
            if (tableMeta.hasTable(ident)) {
              return tableMeta.table(ident);
            } else {
              return tableMeta.column(ident);
            }
          } else {
            if (tableMeta instanceof ReferencedMultiTableMeta) {
              return tableFunction((ReferencedMultiTableMeta) tableMeta);
            } else {
              throw new ModelException("Cannot call function on table.");
            }
          }
        } else if (obj instanceof ColumnMeta) {
          ColumnMeta columnMeta = (ColumnMeta) obj;
          return simpleFunction(sourceTable, columnMeta);
        }
      }
    }
    throw new ModelException(String.format("Invalid identifier '%s'.", ident.getString()));
  }

  @Override
  public String getName(String def) {
    if (args.size() == 0 && !isAggr()) {
      return ident.getString();
    } else {
      return super.getName(def);
    }
  }

  @Override
  public String print() {
    String argsStr = args.isEmpty() ? ""
        : "(" + args.stream().map(arg -> arg.print()).collect(Collectors.joining(", ")) + ")";
    if (object == null) {
      return ident.getString() + argsStr;
    } else {
      return object.print() + "." + ident.getString() + argsStr;
    }
  }
}
