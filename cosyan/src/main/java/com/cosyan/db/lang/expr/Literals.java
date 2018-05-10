package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.Date;

import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DateFunctions;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Literals {

  public static interface Literal {
    public Object getValue();

    public String print();

    public DataType<?> getType() throws ModelException;
  }

  private static class LiteralColumn extends DerivedColumn {

    private Literal literal;

    public LiteralColumn(Literal literal) throws ModelException {
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
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
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

    @Override
    public Loc loc() {
      return loc;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression implements Literal {
    private final Long value;
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
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

    @Override
    public Loc loc() {
      return loc;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression implements Literal {
    private final Double value;
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
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

    @Override
    public Loc loc() {
      return loc;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BooleanLiteral extends Expression implements Literal {
    private final Boolean value;
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.BoolType;
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DateLiteral extends Expression implements Literal {
    private final Date value;
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return "dt " + "'" + DateFunctions.sdf1.format(value) + "'";
    }

    @Override
    public DataType<?> getType() {
      return DataTypes.DateType;
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class NullLiteral extends Expression implements Literal {
    private final Loc loc;

    @Override
    public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
      return new LiteralColumn(this);
    }

    @Override
    public String print() {
      return "null";
    }

    @Override
    public Object getValue() {
      return null;
    }

    @Override
    public DataType<?> getType() throws ModelException {
      return DataTypes.NullType;
    }

    @Override
    public Loc loc() {
      return loc;
    }
  }
}
