package com.cosyan.db.doc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.StringJoiner;

import com.cosyan.db.doc.FunctionDocumentation.Func;
import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableMap;

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
        ImmutableMap<String, DataType<?>> funcArgs = function.getArgTypes();
        StringBuilder sb = new StringBuilder();
        StringJoiner sj = new StringJoiner(", ");
        sb.append(" * `").append(function.getIdent()).append("(");
        for (Entry<String, DataType<?>> param : funcArgs.entrySet()) {
          sj.add(param.getKey() + ": " + param.getValue().getName());
        }
        sb.append(sj.toString());
        sb.append("): ").append(function.getReturnType().getName()).append("`\n");
        pw.print(sb.toString());
        String doc = ann.doc();
        for (String param : funcArgs.keySet()) {
          doc = doc.replaceAll("([ ,.])" + param + "([ ,.])", "$1`" + param + "`$2");
        }
        pw.append("   " + doc + "\n");
      }
    } finally {
      pw.close();
    }
  }
}
