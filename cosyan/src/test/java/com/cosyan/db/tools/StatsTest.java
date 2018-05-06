package com.cosyan.db.tools;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

public class StatsTest {

  @Ignore
  @Test
  public void testStat() {
    Random rand = new Random();
    Map<Integer, Integer> s = new HashMap<>();
    int hits = 0;
    for (int i = 0; i < 100000; i++) {
      int n = (int) Math.pow(rand.nextDouble(), 1.0 / (-2.0 + 1.0));
      if (s.containsKey(n)) {
        hits++;
        
      } else {
        s.put(n, 0);
      }
      s.put(n, s.get(n) + 1);
      if (i % 1000 == 0) {
        double sum = 0;
        double max = 0;
        for (Integer v : s.values()) {
          sum += v;
          max = Math.max(max, v);
        }
        System.out.println(i + "\t" + hits / 1000.0 + "\t" + sum / s.size() + "\t" + max);
        hits = 0;
      }
    }
  }
}
