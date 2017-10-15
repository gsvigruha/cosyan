package com.cosyan.db.lang.sql;

import static com.cosyan.db.lang.sql.SyntaxTree.assertType;

import java.util.Date;
import java.util.List;

import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.SyntaxTree.AggregationExpression;
import com.cosyan.db.lang.sql.SyntaxTree.Expression;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class BinaryExpression extends Expression {

  private final Token token;
  private final Expression left;
  private final Expression right;
  private final AggregationExpression aggregation;

  public BinaryExpression(
      Token token,
      Expression left,
      Expression right) throws ParserException {
    this.token = token;
    this.left = left;
    this.right = right;
    AggregationExpression leftAggr = left.isAggregation();
    AggregationExpression rightAggr = right.isAggregation();

    if (leftAggr == rightAggr) {
      this.aggregation = leftAggr;
    } else if (leftAggr == AggregationExpression.EITHER) {
      this.aggregation = rightAggr;
    } else if (rightAggr == AggregationExpression.EITHER) {
      this.aggregation = leftAggr;
    } else {
      throw new ParserException("Incompatible left and right expression for " + token.getString() + ".");
    }
  }

  protected abstract class BinaryColumn extends DerivedColumn {
    private final DerivedColumn leftColumn;
    private final DerivedColumn rightColumn;

    public BinaryColumn(DataType<?> type, DerivedColumn leftColumn, DerivedColumn rightColumn) {
      super(type);
      this.leftColumn = leftColumn;
      this.rightColumn = rightColumn;
    }

    @Override
    public Object getValue(Object[] sourceValues) {
      Object l = leftColumn.getValue(sourceValues);
      Object r = rightColumn.getValue(sourceValues);
      if (l == DataTypes.NULL || r == DataTypes.NULL) {
        return DataTypes.NULL;
      } else {
        return getValueImpl(l, r);
      }
    }

    protected abstract Object getValueImpl(Object left, Object right);
  }

  @Override
  public DerivedColumn compile(TableMeta sourceTable, List<AggrColumn> aggrColumns)
      throws ModelException {
    final DerivedColumn leftColumn = left.compile(sourceTable, aggrColumns);
    final DerivedColumn rightColumn = right.compile(sourceTable, aggrColumns);

    if (token.is(Tokens.AND)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l && (Boolean) r;
        }
      };
    } else if (token.is(Tokens.OR)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l || (Boolean) r;
        }
      };
    } else if (token.is(Tokens.XOR)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l ^ (Boolean) r;
        }
      };
    } else if (token.is(Tokens.IMPL)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return !(Boolean) l || (Boolean) r;
        }
      };
    } else if (token.is(Tokens.ASTERISK)) {
      return asteriskExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.PLUS)) {
      return plusExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.MINUS)) {
      return minusExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.DIV)) {
      return divExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.MOD)) {
      return modExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.EQ)) {
      return eqExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.LESS)) {
      return lessExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.GREATER)) {
      return greaterExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.LEQ)) {
      return leqExpression(leftColumn, rightColumn);
    } else if (token.is(Tokens.GEQ)) {
      return geqExpression(leftColumn, rightColumn);
    } else {
      throw new ModelException("Unsupported binary expression '" + token.getString() + "'.");
    }
  }

  private DerivedColumn asteriskExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l * (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l * (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l * (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l * (Double) r;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn plusExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l + (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l + (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l + (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l + (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.StringType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (String) l + (String) r;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn minusExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l - (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l - (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l - (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l - (Double) r;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn divExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l / (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l / (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l / (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l / (Double) r;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn modExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l % (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l % (Double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l % (Long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l % (Double) r;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn eqExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l == (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l == (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l == (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l == (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).equals((String) r);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).equals((Date) r);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn lessExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l < (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l < (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l < (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l < (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) < 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) < 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn greaterExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l > (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l > (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l > (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l > (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) > 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) > 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn leqExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l <= (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l <= (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l <= (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l <= (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) <= 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) <= 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn geqExpression(DerivedColumn leftColumn, DerivedColumn rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l >= (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l >= (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l >= (long) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l >= (double) r;
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) >= 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) >= 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + token.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  @Override
  public AggregationExpression isAggregation() {
    return aggregation;
  }

  @Override
  public String print() {
    return "(" + left.print() + " " + token.getString() + " " + right.print() + ")";
  }
}
