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
