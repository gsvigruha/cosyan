package com.cosyan.db.lang.expr;

import com.cosyan.db.model.ColumnMeta.DerivedColumn;

import java.util.Set;

import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableSet;
import com.cosyan.db.model.TableMeta;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Literals {

  public static interface Literal {
    public Object getValue();
  }

  private static class LiteralColumn extends DerivedColumn {

    private Object value;

    public LiteralColumn(DataType<?> type, Object value) {
      super(type);
      this.value = value;
    }

    @Override
    public Object getValue(Object[] values, Resources resources) {
      return value;
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.empty();
    }

    @Override
    public Set<TableMeta> tables() {
      return ImmutableSet.of();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression implements Literal {
    private final String value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(DataTypes.StringType, value);
    }

    @Override
    public String print() {
      return "'" + value + "'";
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression implements Literal {
    private final Long value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(DataTypes.LongType, value);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression implements Literal {
    private final Double value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(DataTypes.DoubleType, value);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }
  }
}