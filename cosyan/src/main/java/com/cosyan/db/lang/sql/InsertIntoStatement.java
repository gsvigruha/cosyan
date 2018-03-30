package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import com.cosyan.db.io.TableWriter;
import com.cosyan.db.lang.expr.Literals.Literal;
import com.cosyan.db.lang.sql.Result.StatementResult;
import com.cosyan.db.lang.sql.SyntaxTree.Node;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class InsertIntoStatement {

  public static Object check(DataType<?> dataType, Literal literal) throws RuleException {
    if (literal.getValue() != DataTypes.NULL && literal.getType() != dataType.inputType()) {
      throw new RuleException(String.format("Expected '%s' but got '%s'.", dataType.inputType(), literal.getType()));
    }
    return dataType.convert(literal.getValue());
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class InsertInto extends Node implements Statement {
    private final Ident table;
    private final Optional<ImmutableList<Ident>> columns;
    private final ImmutableList<ImmutableList<Literal>> valuess;

    private MaterializedTableMeta tableMeta;
    private ImmutableMap<Ident, Integer> indexes;

    @Override
    public MetaResources compile(MetaRepo metaRepo) throws ModelException {
      tableMeta = metaRepo.table(table);
      ImmutableMap.Builder<Ident, Integer> indexesBuilder = ImmutableMap.builder();
      if (columns.isPresent()) {
        for (int i = 0; i < columns.get().size(); i++) {
          Ident ident = columns.get().get(i);
          indexesBuilder.put(ident, tableMeta.column(ident).getIndex());
        }
      }
      indexes = indexesBuilder.build();
      return MetaResources
          .insertIntoTable(tableMeta)
          .merge(tableMeta.ruleDependenciesReadResources())
          .merge(tableMeta.reverseRuleDependenciesReadResources());
    }

    @Override
    public Result execute(Resources resources) throws RuleException, IOException {
      ImmutableList<BasicColumn> cols = ImmutableList.copyOf(tableMeta.columns().values());
      Object[] fullValues = new Object[cols.size()];

      // The rules must be evaluated for new records. This requires dependent table
      // readers.
      TableWriter writer = resources.writer(tableMeta.tableName());
      for (ImmutableList<Literal> values : valuess) {
        if (columns.isPresent()) {
          Arrays.fill(fullValues, DataTypes.NULL);
          for (int i = 0; i < columns.get().size(); i++) {
            int idx = indexes.get(columns.get().get(i));
            fullValues[idx] = check(cols.get(idx).getType(), values.get(i));
          }
        } else {
          if (values.size() != fullValues.length) {
            throw new RuleException("Expected '" + fullValues.length + "' values but got '" + values.size() + "'.");
          }
          for (int i = 0; i < values.size(); i++) {
            fullValues[i] = check(cols.get(i).getType(), values.get(i));
          }
        }
        writer.insert(resources, fullValues, /* checkReferencingRules= */true);
      }
      return new StatementResult(valuess.size());
    }

    @Override
    public void cancel() {

    }
  }
}
