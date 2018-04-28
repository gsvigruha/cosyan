package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class CaseExpression extends Expression {

  private final ImmutableList<Expression> conditions;
  private final ImmutableList<Expression> values;
  private final Expression elseValue;

  @Override
  public DerivedColumn compile(TableMeta sourceTable) throws ModelException {

    final TableDependencies deps = new TableDependencies();
    MetaResources resources = MetaResources.empty();
    ImmutableList.Builder<ColumnMeta> conditionColsBuilder = ImmutableList.builder();
    ImmutableList.Builder<ColumnMeta> valueColsBuilder = ImmutableList.builder();
    DataType<?> type = null;
    for (int i = 0; i < conditions.size(); i++) {
      ColumnMeta condition = conditions.get(i).compileColumn(sourceTable);
      if (condition.getType() != DataTypes.BoolType) {
        throw new ModelException("Expected boolean type but got " + condition.getType() + ".");
      }
      conditionColsBuilder.add(condition);
      deps.addToThis(condition.tableDependencies());
      resources = resources.merge(condition.readResources());
      ColumnMeta value = values.get(i).compileColumn(sourceTable);
      if (type == null) {
        type = value.getType();
      } else if (type != value.getType()) {
        throw new ModelException("Inconsistent type " + type + " vs " + value.getType() + ".");
      }
      valueColsBuilder.add(value);
      deps.addToThis(value.tableDependencies());
      resources = resources.merge(value.readResources());
    }
    ColumnMeta elseCol = elseValue.compileColumn(sourceTable);
    deps.addToThis(elseCol.tableDependencies());
    final MetaResources metaResources = resources.merge(elseCol.readResources());
    final ImmutableList<ColumnMeta> conditionCols = conditionColsBuilder.build();
    final ImmutableList<ColumnMeta> valueCols = valueColsBuilder.build();

    return new DerivedColumn(type) {

      @Override
      public Object value(Object[] values, Resources resources) throws IOException {
        for (int i = 0; i < conditionCols.size(); i++) {
          Object condition = conditionCols.get(i).value(values, resources);
          if (condition != null && ((boolean) condition)) {
            return valueCols.get(i).value(values, resources);
          }
        }
        return elseCol.value(values, resources);
      }

      @Override
      public String print(Object[] values, Resources resources) throws IOException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public TableDependencies tableDependencies() {
        return deps;
      }

      @Override
      public MetaResources readResources() {
        return metaResources;
      }
    };
  }

  @Override
  public String print() {
    // TODO Auto-generated method stub
    return null;
  }
}
