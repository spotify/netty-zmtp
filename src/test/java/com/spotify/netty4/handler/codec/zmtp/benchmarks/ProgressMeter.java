/*
 * Copyright (c) 2012-2013 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.netty4.handler.codec.zmtp.benchmarks;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;

class ProgressMeter {

  static class Delta {

    Delta(final long ops, final long time) {
      this.ops = ops;
      this.time = time;
    }

    public final long ops;
    public final long time;
  }

  private long lastRows = 0;
  private long lastTime = System.nanoTime();
  private long lastLatency = 0;
  private final long interval = 1000;

  final private String unit;

  final private AtomicLong latency = new AtomicLong();
  final private AtomicLong operations = new AtomicLong();

  final private ArrayDeque<Delta> deltas = new ArrayDeque<Delta>();

  private volatile boolean run = true;

  private final Thread worker;

  public ProgressMeter() {
    this("ops");
  }

  public ProgressMeter(final String unit) {
    this.unit = unit;
    worker = new Thread(new Runnable() {
      public void run() {
        while (run) {
          try {
            Thread.sleep(interval);
          } catch (InterruptedException e) {
            continue;
          }
          progress();
        }
      }
    });
    worker.start();
  }

  private void progress() {
    final long count = this.operations.get();
    final long time = System.nanoTime();
    final long latency = this.latency.get();

    final long delta = count - lastRows;
    final long deltaTime = time - lastTime;
    final long deltaLatency = latency - lastLatency;

    deltas.add(new Delta(delta, deltaTime));

    if (deltas.size() > 10) {
      deltas.pop();
    }

    long opSum = 0;
    long timeSum = 0;

    for (final Delta d : deltas) {
      timeSum += d.time;
      opSum += d.ops;
    }

    final long operations = deltaTime == 0 ? 0 : 1000000000 * delta / deltaTime;
    final long averagedOperations = timeSum == 0 ? 0 : 1000000000 * opSum / timeSum;
    final float averageLatency = opSum == 0 ? 0 : deltaLatency / (1000000.f * opSum);

    System.out.printf("%,10d (%,10d) %s/s. %,10.3f ms average latency. %,10d %s total.\n",
                      operations, averagedOperations, unit, averageLatency, count, unit);
    System.out.flush();

    lastRows = count;
    lastTime = time;
    lastLatency = latency;
  }

  public void finish() {
    run = false;
    worker.interrupt();
    try {
      worker.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    progress();
  }

  public void inc(final long ops, final long latency) {
    this.operations.addAndGet(ops);
    this.latency.addAndGet(latency);
  }
}
