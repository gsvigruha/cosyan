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

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.model.MathFunctions.Power;

public class FunctionDocumentationTest {

  FunctionDocumentation doc = new FunctionDocumentation();

  @Test
  public void testMathFunctionDocs() {
    assertEquals("pow(float, float): float\nReturns self raised to the power of x.", doc.documentation(new Power()));
  }
}
