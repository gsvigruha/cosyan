package com.cosyan.db.doc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.StringJoiner;

import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableCollection;

public class FunctionDocumentation {
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE })
  public static @interface Func {
    public String doc();
  }

  public String documentation(SimpleFunction<?> function) {
    Func ann = function.getClass().getAnnotation(Func.class);
    if (ann == null) {
      throw new RuntimeException(String.format("Missing annotation for '%s'.", function.getIdent()));
    }
    ImmutableCollection<DataType<?>> args = function.getArgTypes().values();
    StringBuilder sb = new StringBuilder();
    StringJoiner sj = new StringJoiner(", ");
    sb.append(function.getIdent()).append("(");
    for (DataType<?> paramType : args) {
      sj.add(paramType.getName());
    }
    sb.append(sj.toString());
    sb.append("): ").append(function.getReturnType().getName()).append("\n");
    sb.append(ann.doc());
    return sb.toString();
  }
}
