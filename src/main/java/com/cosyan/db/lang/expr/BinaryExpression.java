/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.lang.expr;

import java.io.IOException;
import java.util.Date;

import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.lang.sql.Tokens.Token;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableContext;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

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
      Expression right) {
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
    public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
      Object l = leftColumn.value(values, resources, context);
      Object r = rightColumn.value(values, resources, context);
      if (l == null || r == null) {
        return null;
      } else {
        return getValueImpl(l, r);
      }
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      String l = leftColumn.print(values, resources, context);
      String r = rightColumn.print(values, resources, context);
      return "(" + l + " " + token + " " + r + ")";
    }

    @Override
    public MetaResources readResources() {
      return leftColumn.readResources().merge(rightColumn.readResources());
    }

    @Override
    public TableDependencies tableDependencies() {
      TableDependencies deps = new TableDependencies();
      deps.addToThis(leftColumn.tableDependencies());
      deps.addToThis(rightColumn.tableDependencies());
      return deps;
    }

    protected abstract Object getValueImpl(Object left, Object right);
  }

  @Override
  public DerivedColumn compile(TableMeta sourceTable) throws ModelException {
    final ColumnMeta leftColumn = left.compileColumn(sourceTable);
    final ColumnMeta rightColumn = right.compileColumn(sourceTable);

    if (token.is(Tokens.AND)) {
      assertType(DataTypes.BoolType, leftColumn.getType(), token.getLoc());
      assertType(DataTypes.BoolType, rightColumn.getType(), token.getLoc());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l && (Boolean) r;
        }
      };
    } else if (token.is(Tokens.OR)) {
      assertType(DataTypes.BoolType, leftColumn.getType(), token.getLoc());
      assertType(DataTypes.BoolType, rightColumn.getType(), token.getLoc());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l || (Boolean) r;
        }
      };
    } else if (token.is(Tokens.XOR)) {
      assertType(DataTypes.BoolType, leftColumn.getType(), token.getLoc());
      assertType(DataTypes.BoolType, rightColumn.getType(), token.getLoc());
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {

        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Boolean) l ^ (Boolean) r;
        }
      };
    } else if (token.is(Tokens.IMPL)) {
      assertType(DataTypes.BoolType, leftColumn.getType(), token.getLoc());
      assertType(DataTypes.BoolType, rightColumn.getType(), token.getLoc());
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
      throw new ModelException(String.format("Unsupported binary expression '%s'.", token.getString()), token);
    }
  }

  private DerivedColumn asteriskExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l * (Long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l * (Double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l * (Long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l * (Double) r;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn plusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l + (Long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l + (Double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l + (Long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l + (Double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.StringType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (String) l + (String) r;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn minusExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l - (Long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l - (Double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l - (Long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l - (Double) r;
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (((Date) l).getTime() - ((Date) r).getTime()) / 1000;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn divExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l / (Long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l / (Double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l / (Long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l / (Double) r;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn modExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.LongType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l % (Long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l % (Double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Double) l % (Long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.DoubleType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (Long) l % (Double) r;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn eqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l == (long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l == (double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l == (long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l == (double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).equals((String) r);
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).equals((Date) r);
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn lessExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l < (long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l < (double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l < (long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l < (double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) < 0;
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) < 0;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn greaterExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l > (long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l > (double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l > (long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l > (double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) > 0;
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) > 0;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn leqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l <= (long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l <= (double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l <= (long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l <= (double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) <= 0;
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) <= 0;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  private DerivedColumn geqExpression(ColumnMeta leftColumn, ColumnMeta rightColumn) throws ModelException {
    if (leftColumn.getType().isLong() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l >= (long) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l >= (double) r;
        }
      };
    } else if (leftColumn.getType().isDouble() && rightColumn.getType().isLong()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (double) l >= (long) r;
        }
      };
    } else if (leftColumn.getType().isLong() && rightColumn.getType().isDouble()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return (long) l >= (double) r;
        }
      };
    } else if (leftColumn.getType().isString() && rightColumn.getType().isString()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((String) l).compareTo((String) r) >= 0;
        }
      };
    } else if (leftColumn.getType().isDate() && rightColumn.getType().isDate()) {
      return new BinaryColumn(DataTypes.BoolType, leftColumn, rightColumn) {
        @Override
        public Object getValueImpl(Object l, Object r) {
          return ((Date) l).compareTo((Date) r) >= 0;
        }
      };
    } else {
      throw new ModelException(String.format("Unsupported binary expression '%s' for types '%s' and '%s'.",
          token.getString(), leftColumn.getType(), rightColumn.getType()), token);
    }
  }

  @Override
  public String print() {
    return "(" + left.print() + " " + token.getString() + " " + right.print() + ")";
  }

  @Override
  public Loc loc() {
    return token.getLoc();
  }
}
