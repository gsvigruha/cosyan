package com.cosyan.db.sql;

import static com.cosyan.db.sql.SyntaxTree.assertType;

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
      } else {
        throw new ModelException("Unsupported binary expression " + ident.getString() +
            " for types " + leftColumn.getType() + " and " + rightColumn.getType() + ".");
      }
    } else {
      throw new ModelException("Unsupported binary expression " + ident.getString() + ".");
    }
  }

  @Override
  public String getName() {
    return left.getName() + "_" + ident.getString() + "_" + right.getName();
  }
}
