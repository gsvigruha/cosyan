package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.Date;

import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Literals {

  public static interface Literal {
    public Object getValue();

    public String print();

    public DataType<?> getType();
  }

  private static class LiteralColumn extends DerivedColumn {

    private Literal literal;

    public LiteralColumn(Literal literal) {
      super(literal.getType());
      this.literal = literal;
    }

    @Override
    public Object value(Object[] values, Resources resources) {
      return literal.getValue();
    }

    @Override
    public String print(Object[] values, Resources resources) throws IOException {
      return literal.print();
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.empty();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression implements Literal {
    private final String value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return "'" + value + "'";
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.StringType;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression implements Literal {
    private final Long value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.LongType;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression implements Literal {
    private final Double value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.DoubleType;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BooleanLiteral extends Expression implements Literal {
    private final Boolean value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return "'" + value + "'";
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.BoolType;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DateLiteral extends Expression implements Literal {
    private final Date value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.DateType;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class NullLiteral extends Expression implements Literal {

    @Override
    public DerivedColumn compile(TableMeta sourceTable) {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return "null";
    }

    @Override
    public Object getValue() {
      return DataTypes.NULL;
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.NULL;
    }
  }
}
