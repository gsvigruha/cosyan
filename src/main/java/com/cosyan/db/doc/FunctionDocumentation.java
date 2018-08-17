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
  public static @interface FuncCat {
    public String name();
    public String doc();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.TYPE })
  public static @interface Func {
    public String doc();
  }

  public String documentation(SimpleFunction<?> function) {
    Func ann = function.getClass().getAnnotation(Func.class);
    if (ann == null) {
      throw new RuntimeException(String.format("Missing annotation for '%s'.", function.getName()));
    }
    ImmutableCollection<DataType<?>> args = function.getArgTypes().values();
    StringBuilder sb = new StringBuilder();
    StringJoiner sj = new StringJoiner(", ");
    sb.append(function.getName()).append("(");
    for (DataType<?> paramType : args) {
      sj.add(paramType.getName());
    }
    sb.append(sj.toString());
    sb.append("): ").append(function.getReturnType().getName()).append("\n");
    sb.append(ann.doc());
    return sb.toString();
  }
}
