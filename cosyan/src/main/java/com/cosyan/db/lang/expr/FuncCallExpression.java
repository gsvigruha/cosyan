package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.stream.Collectors;

import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.SyntaxTree;
import com.cosyan.db.lang.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumnWithDeps;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FuncCallExpression extends Expression {
  private final Ident ident;
  private final ImmutableList<Expression> args;
  private final AggregationExpression aggregation;

  public FuncCallExpression(Ident ident, ImmutableList<Expression> args) throws ParserException {
    this.ident = ident;
    this.args = args;
    if (isAggr()) {
      aggregation = AggregationExpression.YES;
    } else {
      long yes = args.stream().filter(arg -> arg.isAggregation() == AggregationExpression.YES).count();
      long no = args.stream().filter(arg -> arg.isAggregation() == AggregationExpression.NO).count();
      if (no == 0 && yes == 0) {
        aggregation = AggregationExpression.EITHER;
      } else if (no == 0) {
        aggregation = AggregationExpression.YES;
      } else if (yes == 0) {
        aggregation = AggregationExpression.NO;
      } else {
        throw new ParserException("Inconsistent parameter.");
      }
    }
  }

  private boolean isAggr() {
    return BuiltinFunctions.AGGREGATION_NAMES.contains(ident.last());
  }

  @Override
  public DerivedColumn compile(TableMeta sourceTable, ExtraInfoCollector collector)
      throws ModelException {
    if (isAggr()) {
      Expression arg;
      if (ident.isSimple()) {
        if (args.size() == 1) {
          arg = Iterables.getOnlyElement(args);
        } else {
          throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
        }
      } else {
        arg = new IdentExpression(ident.body());
      }
      if (!(sourceTable instanceof KeyValueTableMeta)) {
        throw new ModelException("Aggregators are not allowed here.");
      }
      KeyValueTableMeta outerTable = (KeyValueTableMeta) sourceTable;
      ColumnMeta argColumn = arg.compile(outerTable.getSourceTable());
      final TypedAggrFunction<?> function = BuiltinFunctions.aggrFunction(ident.last(), argColumn.getType());

      AggrColumn aggrColumn = new AggrColumn(
          function.getReturnType(),
          argColumn,
          outerTable.getKeyColumns().size() + collector.numAggrColumns(),
          function);
      collector.addAggrColumn(aggrColumn);
      return aggrColumn;
    } else {
      SimpleFunction<?> function;
      ImmutableList<Expression> allArgs;
      if (ident.isSimple()) {
        function = BuiltinFunctions.simpleFunction(ident.head());
        allArgs = args;
      } else {
        function = BuiltinFunctions.simpleFunction(ident.last());
        Expression objExpr = new IdentExpression(new Ident(ident.body().getString()));
        allArgs = ImmutableList.<Expression>builder().add(objExpr).addAll(args).build();
      }

      ImmutableList.Builder<ColumnMeta> argColumnsBuilder = ImmutableList.builder();
      TableDependencies tableDependencies = new TableDependencies();
      for (int i = 0; i < function.getArgTypes().size(); i++) {
        ColumnMeta col = allArgs.get(i).compile(sourceTable);
        SyntaxTree.assertType(function.getArgTypes().get(i), col.getType());
        argColumnsBuilder.add(col);
        tableDependencies.add(col.tableDependencies());
      }
      ImmutableList<ColumnMeta> argColumns = argColumnsBuilder.build();

      return new DerivedColumnWithDeps(function.getReturnType(), tableDependencies) {

        @Override
        public Object getValue(SourceValues values) throws IOException {
          ImmutableList.Builder<Object> paramsBuilder = ImmutableList.builder();
          for (ColumnMeta column : argColumns) {
            paramsBuilder.add(column.getValue(values));
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
  }

  @Override
  public AggregationExpression isAggregation() {
    return aggregation;
  }

  @Override
  public String print() {
    return ident.getString()
        + "("
        + args.stream().map(arg -> arg.print()).collect(Collectors.joining(", "))
        + ")";
  }

  @Override
  public MetaResources readResources(MaterializedTableMeta tableMeta) throws ModelException {
    MetaResources readResources = MetaResources.empty();
    for (Expression arg : args) {
      readResources = readResources.merge(arg.readResources(tableMeta));
    }
    return readResources;
  }
}
