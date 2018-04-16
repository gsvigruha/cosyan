package com.cosyan.db.doc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.cosyan.db.model.BuiltinFunctions;
import com.cosyan.db.model.BuiltinFunctions.SimpleFunction;

public class DocPrinter {

  public static void main(String[] args) throws IOException {
    PrintWriter pw = new PrintWriter(
        args[0] + File.separator +
            "resources" + File.separator +
            "doc" + File.separator +
            "func" + File.separator +
            "simple_functions.md");
    FunctionDocumentation documentation = new FunctionDocumentation();
    try {
      for (SimpleFunction<?> func : BuiltinFunctions.SIMPLE) {
        pw.write(documentation.documentation(func) + "\n");
      }
    } finally {
      pw.close();
    }
  }
}
