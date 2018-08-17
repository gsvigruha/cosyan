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
package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableContext;
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
  private final Loc loc;

  @Override
  public DerivedColumn compile(TableMeta sourceTable) throws ModelException {

    final TableDependencies deps = new TableDependencies();
    MetaResources resources = MetaResources.empty();
    ImmutableList.Builder<ColumnMeta> conditionColsBuilder = ImmutableList.builder();
    ImmutableList.Builder<ColumnMeta> valueColsBuilder = ImmutableList.builder();
    DataType<?> type = null;
    for (int i = 0; i < conditions.size(); i++) {
      Expression conditionExpr = conditions.get(i);
      ColumnMeta condition = conditionExpr.compileColumn(sourceTable);
      if (condition.getType() != DataTypes.BoolType) {
        throw new ModelException(String.format(
            "Expected 'boolean' type but got '%s'.", condition.getType()), conditionExpr);
      }
      conditionColsBuilder.add(condition);
      deps.addToThis(condition.tableDependencies());
      resources = resources.merge(condition.readResources());
      Expression valueExpr = values.get(i);
      ColumnMeta value = valueExpr.compileColumn(sourceTable);
      if (type == null) {
        type = value.getType();
      } else if (type != value.getType()) {
        throw new ModelException(String.format(
            "Inconsistent types for case expression '%s' and '%s'.", type, value.getType()), valueExpr);
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
      public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
        for (int i = 0; i < conditionCols.size(); i++) {
          Object condition = conditionCols.get(i).value(values, resources, context);
          if (condition != null && ((boolean) condition)) {
            return valueCols.get(i).value(values, resources, context);
          }
        }
        return elseCol.value(values, resources, context);
      }

      @Override
      public String print(Object[] values, Resources resources, TableContext context) throws IOException {
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
    StringBuilder sb = new StringBuilder();
    sb.append("case ");
    for (int i = 0; i < conditions.size(); i++) {
      sb.append("when ")
          .append(conditions.get(i).print())
          .append(" then ")
          .append(values.get(i).print());
    }
    sb.append(" else ").append(elseValue.print());
    sb.append(" end");
    return sb.toString();
  }

  @Override
  public Loc loc() {
    return loc;
  }
}
