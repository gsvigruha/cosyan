package com.cosyan.db.lang.expr;

import java.util.Optional;

import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.SeekableTableMeta;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class TableDefinition {
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ColumnDefinition extends Node {
    private final Ident name;
    private final DataType<?> type;
    private final boolean nullable;
    private final boolean unique;
    private final boolean immutable;
  }

  public interface ConstraintDefinition {
    public Ident getName();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class RuleDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Expression expr;
    private final boolean nullIsTrue;

    @Override
    public String toString() {
      return name + " [" + expr.print() + "]";
    }

    public Rule compile(MaterializedTable tableMeta) throws ModelException {
      SeekableTableMeta table = tableMeta.reader();
      ColumnMeta column = expr.compileColumn(table);
      return new Rule(name.getString(), table, column, expr, nullIsTrue, column.tableDependencies());
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class PrimaryKeyDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Ident keyColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ForeignKeyDefinition extends Node implements ConstraintDefinition {
    private final Ident name;
    private final Ident revName;
    private final Ident keyColumn;
    private final Ident refTable;
    private final Optional<Ident> refColumn;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AggRefDefinition extends Node {
    private final Ident name;
    private final Select select;
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FlatRefDefinition extends Node {
    private final Ident name;
    private final ImmutableList<Expression> exprs;
  }
}
