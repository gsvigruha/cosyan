package com.cosyan.db.lang.expr;

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
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DerivedTables.KeyValueTableMeta;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableDependencies;
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
    return BuiltinFunctions.AGGREGATION_NAMES.contains(ident.getString());
  }

  @Override
  public DerivedColumn compile(TableMeta sourceTable, TableDependencies deps)
      throws ModelException {
    if (isAggr()) {
      if (args.size() != 1) {
        throw new ModelException("Invalid number of arguments for aggregator: " + args.size() + ".");
      }
      if (!(sourceTable instanceof KeyValueTableMeta)) {
        throw new ModelException("Aggregators are not allowed here.");
      }
      KeyValueTableMeta outerTable = (KeyValueTableMeta) sourceTable;
      ColumnMeta argColumn = Iterables.getOnlyElement(args).compile(outerTable.getSourceTable());
      final TypedAggrFunction<?> function = BuiltinFunctions.aggrFunction(ident.getString(), argColumn.getType());

      AggrColumn aggrColumn = new AggrColumn(
          function.getReturnType(),
          argColumn,
          outerTable.getKeyColumns().size() + deps.numAggrColumns(),
          function);
      deps.addAggrColumn(aggrColumn);
      return aggrColumn;
    } else {
      ImmutableList.Builder<ColumnMeta> argColumnsBuilder = ImmutableList.builder();
      SimpleFunction<?> function;
      ImmutableList<Expression> allArgs;
      if (ident.isSimple()) {
        function = BuiltinFunctions.simpleFunction(ident.head());
        allArgs = args;
      } else {
        function = BuiltinFunctions.simpleFunction(ident.tail().getString());
        Expression objExpr = new IdentExpression(new Ident(ident.head()));
        allArgs = ImmutableList.<Expression>builder().add(objExpr).addAll(args).build();
      }
      for (int i = 0; i < function.getArgTypes().size(); i++) {
        ColumnMeta col = allArgs.get(i).compile(sourceTable);
        SyntaxTree.assertType(function.getArgTypes().get(i), col.getType());
        argColumnsBuilder.add(col);
      }
      ImmutableList<ColumnMeta> argColumns = argColumnsBuilder.build();

      return new DerivedColumn(function.getReturnType()) {

        @Override
        public Object getValue(SourceValues values) {
          ImmutableList<Object> params = ImmutableList
              .copyOf(argColumns.stream().map(col -> col.getValue(values)).iterator());
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
