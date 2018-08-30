package com.cosyan.ui;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableSet;

public abstract class ParamServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.METHOD })
  public static @interface Params {
    String[] mandatory() default {};

    String[] optional() default {};
  }

  private ImmutableSet<String> mandatoryParams;
  private ImmutableSet<String> optionalParams;

  public ParamServlet() {
    try {
      Params params = getClass()
          .getDeclaredMethod("doGetImpl", HttpServletRequest.class, HttpServletResponse.class)
          .getAnnotation(Params.class);
      mandatoryParams = ImmutableSet.copyOf(params.mandatory());
      optionalParams = ImmutableSet.copyOf(params.optional());
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
