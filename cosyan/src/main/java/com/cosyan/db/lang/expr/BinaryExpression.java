package com.cosyan.db.lang.expr;

import static com.cosyan.db.lang.sql.SyntaxTree.assertType;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.Sets;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class BinaryExpression extends Expression {

  private final Token token;
  private final Expression left;
  private final Expression right;

  public BinaryExpression(
      Token token,
      Expression left,
      Expression right) throws ParserException {
    this.token = token;
    this.left = left;
    this.right = right;
  }

  protected abstract class BinaryColumn extends DerivedColumn {
    private final ColumnMeta leftColumn;
    private final ColumnMeta rightColumn;

    public BinaryColumn(DataType<?> type, ColumnMeta leftColumn, ColumnMeta rightColumn) {
      super(type);
      this.leftColumn = leftColumn;
      this.rightColumn = rightColumn;
    }

    @Override
    public Object getValue(Object[] values, Resources resources) throws IOException {
      Object l = leftColumn.getValue(values, resources);
      Object r = rightColumn.getValue(values, resources);
      if (l == DataTypes.NULL || r == DataTypes.NULL) {
        return DataTypes.NULL;
      } else {
        return getValueImpl(l, r);
      }
    }

    @Override
    public MetaResources readResources() {
      return leftColumn.readResources().merge(rightColumn.readResources());
    }

    @Override
    public TableDependencies tableDependencies() {
      // Left and rightColumn is not used elsewhere so mutability is ok.
      return leftColumn.tableDependencies().add(rightColumn.tableDependencies());
    }

    @Override
    public Set<TableMeta> tables() {
      return Sets.union(leftColumn.tables(), rightColumn.tables());
    }
    
    protected abstract Object getValueImpl(Object left, Object right);
  }

  @Override
  public DerivedColumn compile(TableMeta sourceTable, ExtraInfoCollector collector)
      throws ModelException {
    final ColumnMeta leftColumn = left.compileColumn(sourceTable, collector);
    final ColumnMeta rightColumn = right.compileColumn(sourceTable, collector);

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

  private DerivedColumn asteriskExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn plusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn minusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn divExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn modExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn eqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn lessExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn greaterExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn leqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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

  private DerivedColumn geqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
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
  public String print() {
    return "(" + left.print() + " " + token.getString() + " " + right.print() + ")";
  }
}
