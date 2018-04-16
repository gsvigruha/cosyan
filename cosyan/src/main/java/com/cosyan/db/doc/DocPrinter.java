package com.cosyan.db.doc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringJoiner;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class DocPrinter {

  public static void main(String[] args) throws IOException {
    PrintWriter pw = new PrintWriter(
        args[0] + File.separator +
            "resources" + File.separator +
            "doc" + File.separator +
            "func" + File.separator +
            "simple_functions.md");
    try {
      for (SimpleFunction<?> function : BuiltinFunctions.SIMPLE) {
        Func ann = function.getClass().getAnnotation(Func.class);
        ImmutableList<DataType<?>> funcArgs = function.getArgTypes();
        StringBuilder sb = new StringBuilder();
        StringJoiner sj = new StringJoiner(", ");
        sb.append(" - `").append(function.getIdent()).append("(");
        for (DataType<?> paramType : funcArgs) {
          sj.add(paramType.getName());
        }
        sb.append(sj.toString());
        sb.append("): ").append(function.getReturnType().getName()).append("`\n");
        pw.print(sb.toString());
        pw.append("  " + ann.doc() + "\n");

      }
    } finally {
      pw.close();
    }
  }
}
