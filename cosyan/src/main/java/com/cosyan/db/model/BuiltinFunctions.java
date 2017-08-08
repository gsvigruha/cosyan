package com.cosyan.db.model;

import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class BuiltinFunctions {

  public static interface Function {
    
    public boolean isAggregation();
    
    public DataType<?> returnType();
    
    public ImmutableList<DataType<?>> argTypes();

    public Object call(ImmutableList<Object> argValues);
  }
}
