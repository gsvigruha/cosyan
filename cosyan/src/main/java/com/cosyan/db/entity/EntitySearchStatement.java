package com.cosyan.db.entity;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.entity.EntityFields.ValueField;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.SelectStatement;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class EntitySearchStatement extends Statement {

  private final String table;
  private final ImmutableList<ValueField> fields;
  private final AtomicBoolean cancelled = new AtomicBoolean(false);

  private ExposedTableMeta filteredTable;
  private ImmutableList<BasicColumn> header;

  @Override
  public MetaResources compile(MetaRepo metaRepo) throws ModelException {
    MaterializedTable tableMeta = metaRepo.table(new Ident(table, new Loc(0, 0)));
    header = tableMeta.columns().values().asList();
    ExposedTableMeta sourceTable = tableMeta.reader();
    if (fields.isEmpty()) {
      filteredTable = sourceTable;
    } else {
      Expression expr = fields.get(0).toFilterExpr();
      for (int i = 1; i < fields.size(); i++) {
        expr = new BinaryExpression(new Token(String.valueOf(Tokens.AND), new Loc(0, 0)), expr,
            fields.get(i).toFilterExpr());
      }
      filteredTable = SelectStatement.Select.filteredTable(sourceTable, expr);
    }
    return filteredTable.readResources();
  }

  @Override
  public EntityList execute(Resources resources) throws RuleException, IOException {
    IterableTableReader reader = filteredTable.reader(resources);
    ImmutableList.Builder<Object[]> valuess = ImmutableList.builder();
    try {
      Object[] values = null;
      while ((values = reader.next()) != null && !cancelled.get()) {
        valuess.add(values);
      }
    } finally {
      reader.close();
    }
    return new EntityList(table, header, valuess.build());
  }

  @Override
  public void cancel() {
    cancelled.set(true);
  }
}
