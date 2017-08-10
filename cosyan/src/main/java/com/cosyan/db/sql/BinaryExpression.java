package com.cosyan.db.sql;

import static com.cosyan.db.sql.SyntaxTree.assertType;

import java.util.Date;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.sql.SyntaxTree.Expression;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class BinaryExpression extends Expression {
  private final Ident ident;
  private final Expression left;
  private final Expression right;

  @Override
  public DerivedColumn compile(TableMeta sourceTable, MetaRepo metaRepo) throws ModelException {
    final DerivedColumn leftColumn = left.compile(sourceTable, metaRepo);
    final DerivedColumn rightColumn = right.compile(sourceTable, metaRepo);

    if (ident.is(Tokens.AND)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new DerivedColumn(DataTypes.BoolType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Boolean) leftColumn.getValue(sourceValues) && (Boolean) rightColumn.getValue(sourceValues);
        }
      };
    } else if (ident.is(Tokens.OR)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new DerivedColumn(DataTypes.BoolType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Boolean) leftColumn.getValue(sourceValues) || (Boolean) rightColumn.getValue(sourceValues);
        }
      };
    } else if (ident.is(Tokens.XOR)) {
      assertType(DataTypes.BoolType, leftColumn.getType());
      assertType(DataTypes.BoolType, rightColumn.getType());
      return new DerivedColumn(DataTypes.BoolType) {

        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Boolean) leftColumn.getValue(sourceValues) ^ (Boolean) rightColumn.getValue(sourceValues);
        }
      };
    } else if (ident.is(Tokens.ASTERISK)) {
      return asteriskExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.PLUS)) {
      return plusExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.MINUS)) {
      return minusExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.DIV)) {
      return divExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.MOD)) {
      return modExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.EQ)) {
      return eqExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.LESS)) {
      return lessExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.GREATER)) {
      return greaterExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.LEQ)) {
      return leqExpression(leftColumn, rightColumn);
    } else if (ident.is(Tokens.GEQ)) {
      return geqExpression(leftColumn, rightColumn);
    } else {
      throw new ModelException("Unsupported binary expression '" + ident.getString() + "'.");
    }
  }

  private DerivedColumn asteriskExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.LongType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) * (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) * (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) * (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) * (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn plusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.LongType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) + (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) + (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) + (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) + (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.StringType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (String) leftColumn.getValue(sourceValues) + (String) rightColumn.getValue(sourceValues);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn minusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.LongType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) - (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) - (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) - (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) - (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn divExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.LongType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) / (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) / (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) / (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) / (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn modExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.LongType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) % (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) % (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Double) leftColumn.getValue(sourceValues) % (Long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.DoubleType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (Long) leftColumn.getValue(sourceValues) % (Double) rightColumn.getValue(sourceValues);
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn eqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) == (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) == (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) == (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) == (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((String) leftColumn.getValue(sourceValues)).equals((String) rightColumn.getValue(sourceValues));
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((Date) leftColumn.getValue(sourceValues)).equals((Date) rightColumn.getValue(sourceValues));
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn lessExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) < (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) < (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) < (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) < (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((String) leftColumn.getValue(sourceValues))
              .compareTo((String) rightColumn.getValue(sourceValues)) < 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((Date) leftColumn.getValue(sourceValues)).compareTo((Date) rightColumn.getValue(sourceValues)) < 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn greaterExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) > (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) > (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) > (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) > (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((String) leftColumn.getValue(sourceValues))
              .compareTo((String) rightColumn.getValue(sourceValues)) > 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((Date) leftColumn.getValue(sourceValues)).compareTo((Date) rightColumn.getValue(sourceValues)) > 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn leqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) <= (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) <= (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) <= (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) <= (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((String) leftColumn.getValue(sourceValues))
              .compareTo((String) rightColumn.getValue(sourceValues)) <= 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((Date) leftColumn.getValue(sourceValues)).compareTo((Date) rightColumn.getValue(sourceValues)) <= 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }

  private DerivedColumn geqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) >= (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) >= (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.DoubleType && rightColumn.getType() == DataTypes.LongType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (double) leftColumn.getValue(sourceValues) >= (long) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.LongType && rightColumn.getType() == DataTypes.DoubleType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return (long) leftColumn.getValue(sourceValues) >= (double) rightColumn.getValue(sourceValues);
        }
      };
    } else if (leftColumn.getType() == DataTypes.StringType && rightColumn.getType() == DataTypes.StringType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((String) leftColumn.getValue(sourceValues))
              .compareTo((String) rightColumn.getValue(sourceValues)) >= 0;
        }
      };
    } else if (leftColumn.getType() == DataTypes.DateType && rightColumn.getType() == DataTypes.DateType) {
      return new DerivedColumn(DataTypes.BoolType) {
        @Override
        public Object getValue(ImmutableMap<String, Object> sourceValues) {
          return ((Date) leftColumn.getValue(sourceValues)).compareTo((Date) rightColumn.getValue(sourceValues)) >= 0;
        }
      };
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() +
          " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
    }
  }
}
