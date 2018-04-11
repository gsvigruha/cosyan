package com.cosyan.db.doc;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.model.MathFunctions.Power;

public class FunctionDocumentationTest {

  FunctionDocumentation doc = new FunctionDocumentation();

  @Test
  public void testMathFunctionDocs() {
    assertEquals("pow(float, float): float\nReturns the first parameter on the power of the second.", doc.documentation(new Power()));
  }
}
