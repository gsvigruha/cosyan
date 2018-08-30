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
package com.cosyan.db.auth;

import javax.annotation.Nullable;

public interface AuthToken {

  public void close();

  public String username();

  public boolean isAdmin();

  public String token();

  public static class AdminToken implements AuthToken {

    private final @Nullable String token;

    public AdminToken(@Nullable String token) {
      this.token = token;
    }

    @Override
    public void close() {
    }

    @Override
    public String username() {
      return "admin";
    }

    @Override
    public boolean isAdmin() {
      return true;
    }

    @Override
    public @Nullable String token() {
      return token;
    }
  }

  public static AdminToken adminToken(@Nullable String token) {
    return new AdminToken(token);
  }
}
