package com.cosyan.db.lang.expr;

import java.util.List;

import com.cosyan.db.lang.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class Literals {

  public static interface Literal {
    public Object getValue();
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class StringLiteral extends Expression implements Literal {
    private final String value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.StringType) {

        @Override
        public Object getValue(SourceValues values) {
          return value;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }

    @Override
    public String print() {
      return "'" + value + "'";
    }

    @Override
    public MetaResources readResources(MaterializedTableMeta tableMeta) {
      return MetaResources.empty();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LongLiteral extends Expression implements Literal {
    private final Long value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.LongType) {

        @Override
        public Object getValue(SourceValues values) {
          return value;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public MetaResources readResources(MaterializedTableMeta tableMeta) {
      return MetaResources.empty();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DoubleLiteral extends Expression implements Literal {
    private final Double value;

    @Override
    public DerivedColumn compile(TableMeta sourceTable, List<AggrColumn> aggrColumns) {
      return new DerivedColumn(DataTypes.DoubleType) {

        @Override
        public Object getValue(SourceValues values) {
          return value;
        }
      };
    }

    @Override
    public AggregationExpression isAggregation() {
      return AggregationExpression.EITHER;
    }

    @Override
    public String print() {
      return String.valueOf(value);
    }

    @Override
    public MetaResources readResources(MaterializedTableMeta tableMeta) {
      return MetaResources.empty();
    }
  }
}
