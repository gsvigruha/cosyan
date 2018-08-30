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
package com.cosyan.ui;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableSet;

public abstract class ParamServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Repeatable(Params.class)
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD })
  public static @interface Param {
    String name();

    boolean mandatory() default false;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD })
  public @interface Params {
    Param[] value();
  }

  private ImmutableSet<String> mandatoryParams;
  private ImmutableSet<String> optionalParams;

  public ParamServlet() {
    try {
      Param[] params = getClass()
          .getDeclaredMethod("doGetImpl", HttpServletRequest.class, HttpServletResponse.class)
          .getAnnotationsByType(Param.class);
      mandatoryParams = Arrays.stream(params).filter(p -> p.mandatory()).map(p -> p.name()).collect(ImmutableSet.toImmutableSet());
      optionalParams = Arrays.stream(params).filter(p -> !p.mandatory()).map(p -> p.name()).collect(ImmutableSet.toImmutableSet());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    for (String p : mandatoryParams) {
      if (!req.getParameterMap().keySet().contains(p)) {
        throw new ServletException(String.format("Missing parameter '%s'.", p));
      }
    }
    for (String p : req.getParameterMap().keySet()) {
      if (!mandatoryParams.contains(p) && !optionalParams.contains(p)) {
        throw new ServletException(String.format("Unexpected parameter '%s'.", p));
      }
    }
    doGetImpl(req, resp);
  }

  protected abstract void doGetImpl(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException;
}
