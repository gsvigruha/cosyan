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
package com.cosyan.db.model;

import com.cosyan.db.meta.MaterializedTable;

import lombok.Data;

public class Keys {

  @Data
  public static class PrimaryKey {
    private final Ident name;
    private final BasicColumn column;
  }

  public static interface Ref {

    String getName();

    String getRevName();

    MaterializedTable getTable();

    BasicColumn getColumn();

    MaterializedTable getRefTable();

    BasicColumn getRefColumn();

    Ref getReverse();
  }

  @Data
  public static class ForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + column.getName() + " -> " + refTable.fullName() + "." + refColumn.getName() + "]";
    }

    public ReverseForeignKey createReverse() {
      return new ReverseForeignKey(revName, name, refTable, refColumn, table, column);
    }

    public ReverseForeignKey getReverse() {
      return refTable.reverseForeignKeys().get(revName);
    }
  }

  @Data
  public static class ReverseForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + refTable.fullName() + "." + refColumn.getName() + " -> " + column.getName() + "]";
    }

    public ForeignKey getReverse() {
      return refTable.foreignKeys().get(revName);
    }
  }
}
