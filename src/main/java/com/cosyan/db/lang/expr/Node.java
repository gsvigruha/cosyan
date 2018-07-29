package com.cosyan.db.lang.expr;

import com.cosyan.db.lang.sql.Tokens.Loc;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DataTypes.DataType;

import lombok.Data;

@Data
public class Node {

  public static void assertType(DataType<?> expectedType, DataType<?> dataType, Loc loc) throws ModelException {
    if (!expectedType.javaClass().equals(dataType.javaClass())) {
      throw new ModelException(
          "Data type " + dataType + " did not match expected type " + expectedType + ".", loc);
    }
  }
}
