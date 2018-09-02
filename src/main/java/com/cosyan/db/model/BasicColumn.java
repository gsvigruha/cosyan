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

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.DataTypes.DataType;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode()
@ToString
public class BasicColumn {

  private final String name;
  private final DataType<?> type;
  private final int index;
  private boolean nullable;
  private boolean unique;
  private boolean indexed;
  private boolean deleted;
  private boolean immutable;

  public BasicColumn(
      int index,
      Ident ident,
      DataType<?> type,
      boolean nullable,
      boolean unique,
      boolean immutable) throws ModelException {
    assert immutable || type != DataTypes.IDType;
    assert unique || type != DataTypes.IDType;
    assert !nullable || type != DataTypes.IDType;
    this.type = type;
    this.index = index;
    this.name = ident.getString();
    this.nullable = nullable;
    this.unique = unique;
    this.indexed = unique;
    this.immutable = immutable;
    this.deleted = false;
    if (unique) {
      checkIndexType(ident);
    }
  }

  public String getName() {
    return name;
  }

  public DataType<?> getType() {
    return type;
  }

  public int getIndex() {
    return index;
  }

  public boolean isNullable() {
    return nullable;
  }

  public boolean isUnique() {
    return unique;
  }

  public boolean isIndexed() {
    return indexed;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public boolean isImmutable() {
    return immutable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public void setImmutable(boolean immutable) {
    this.immutable = immutable;
  }

  public void checkIndexType(Ident ident) throws ModelException {
    if (type != DataTypes.StringType && type != DataTypes.LongType && type != DataTypes.DoubleType && type != DataTypes.IDType) {
      throw new ModelException("Unique indexes are only supported for " + DataTypes.StringType +
          ", " + DataTypes.LongType + " and " + DataTypes.DoubleType + "and" + DataTypes.IDType + " types, not " + getType() + ".", ident);
    }
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }
}