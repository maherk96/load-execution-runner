package com.mk.fx.qa.load.execution.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class ReservoirTest {

  @Test
  void percentile_onSmallDataset_returnsExpectedQuantiles() {
    Reservoir r = new Reservoir(100);
    LongStream.rangeClosed(1, 100).forEach(r::add);
    assertEquals(95L, r.percentile(95).orElseThrow());
    assertEquals(99L, r.percentile(99).orElseThrow());
    assertEquals(100L, r.percentile(100).orElseThrow());
    assertEquals(1L, r.percentile(0).orElseThrow());
  }

  @Test
  void percentile_onEmpty_returnsEmpty() {
    Reservoir r = new Reservoir(10);
    assertTrue(r.percentile(95).isEmpty());
  }
}
