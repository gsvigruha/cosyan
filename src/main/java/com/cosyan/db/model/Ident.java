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

import java.util.function.Function;

import javax.annotation.Nullable;

import com.cosyan.db.lang.sql.Tokens.Loc;
import com.google.common.base.Preconditions;

import lombok.Data;

@Data
public class Ident {
  private final String string;
  @Nullable
  private final Loc loc;

  public Ident(String string, Loc loc) {
    this.string = Preconditions.checkNotNull(string);
    this.loc = Preconditions.checkNotNull(loc);
  }

  public Ident(String string) {
    this.string = string;
    this.loc = null;
  }

  public boolean is(String str) {
    return string.equals(str);
  }

  public boolean is(char c) {
    return string.equals(String.valueOf(c));
  }

  @Override
  public String toString() {
    return string;
  }

  public Ident map(Function<String, String> f) {
    return new Ident(f.apply(string), loc);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Ident other = (Ident) obj;
    if (string == null) {
      if (other.string != null)
        return false;
    } else if (!string.equals(other.string))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((string == null) ? 0 : string.hashCode());
    return result;
  }
}