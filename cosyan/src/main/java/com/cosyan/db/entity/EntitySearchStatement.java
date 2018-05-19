package com.cosyan.db.entity;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.lang.expr.BinaryExpression;
import com.cosyan.db.lang.expr.Expression;
import com.cosyan.db.lang.expr.FuncCallExpression;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.sql.SelectStatement;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class EntitySearchStatement extends Statement {

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class Constant extends Expression {

    private final DataType<?> type;
    private final Object value;

    @Override
    public ColumnMeta compile(TableMeta sourceTable) throws ModelException {
      return new ColumnMeta(type) {

        @Override
        public Object value(Object[] values, Resources resources) throws IOException {
          return value;
        }

        @Override
        public String print(Object[] values, Resources resources) throws IOException {
          return null;
        }

        @Override
        public TableDependencies tableDependencies() {
          return new TableDependencies();
        }

        @Override
        public MetaResources readResources() {
          return MetaResources.empty();
        }
      };
    }

    @Override
    public String print() {
      return null;
    }

    @Override
    public Loc loc() {
      return null;
    }
  }

  @Data
  public static class SearchField {
    private final String name;
    private final String value;

    public Expression toExpr(MaterializedTable tableMeta) throws ModelException {
      try {
        Ident ident = new Ident(name, new Loc(0, 0));
        BasicColumn column = tableMeta.column(ident);
        Object value = column.getType().fromString(this.value);
        return new BinaryExpression(
            new Token(String.valueOf(Tokens.EQ), new Loc(0, 0)),
            FuncCallExpression.of(ident),
            new Constant(column.getType(), value));
      } catch (RuleException e) {
        throw new ModelException(e.getMessage(), new Loc(0, 0));
      }
    }
  }

  private final String table;
  private final ImmutableList<SearchField> fields;
  private final AtomicBoolean cancelled = new AtomicBoolean(false);

  private ExposedTableMeta filteredTable;
  private ImmutableList<BasicColumn> header;

  @Override
  public MetaResources compile(MetaRepo metaRepo) throws ModelException {
    MaterializedTable tableMeta = metaRepo.table(new Ident(table, new Loc(0, 0)));
    header = tableMeta.columns().values().asList();
    Expression expr = fields.get(0).toExpr(tableMeta);
    for (int i = 1; i < fields.size(); i++) {
      expr = new BinaryExpression(new Token(String.valueOf(Tokens.AND), new Loc(0, 0)), expr,
          fields.get(i).toExpr(tableMeta));
    }
    ExposedTableMeta sourceTable = tableMeta.reader();
    filteredTable = SelectStatement.Select.filteredTable(sourceTable, expr);
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
