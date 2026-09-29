/*
 * Copyright (C) 2026 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.GwtIncompatible;
import com.google.common.annotations.J2ktIncompatible;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter.SleepingStopwatch;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;
import org.jspecify.annotations.NullUnmarked;

/**
 * Tests for the first-in, first-out behavior of {@link RateLimiter} when several threads share
 * one limiter: a thread that reserved its permits earlier must return from {@code acquire} (or a
 * successful {@code tryAcquire}) before threads that reserved later, no matter how the threads
 * happen to be scheduled.
 *
 * <p>These tests use a {@link ControllableStopwatch} so that they can hold a thread inside the
 * rate limiter at an exact point and deterministically simulate scheduling jitter.
 */
@NullUnmarked
@GwtIncompatible
@J2ktIncompatible
public class RateLimiterFairnessTest extends TestCase {
  private static final double EPSILON = 1e-8;

  /** One permit every 100ms for all the limiters in these tests. */
  private static final double PERMITS_PER_SECOND = 10.0;

  public void testAcquiresReturnInFifoOrder() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0); // the first permit is free

    int threadCount = 4;
    Queue<Double> returnValuesInReturnOrder = new ConcurrentLinkedQueue<>();
    List<Thread> threads = startAcquirers(limiter, threadCount, returnValuesInReturnOrder);

    // The clock is frozen, so the four threads reserve waits of 0.1, 0.2, 0.3 and 0.4 seconds,
    // in whatever order they happened to grab the limiter's lock. Wait until all of them have
    // reserved (a blocked thread has necessarily reserved already).
    awaitAllBlocked(threads);
    stopwatch.awaitSleeperCount(1); // the head of the queue is sleeping; the rest are queued

    // Let the threads complete one at a time, releasing the most recently registered sleeper
    // first. No matter when each thread actually gets to run, they must return in reservation
    // order, which the frozen clock turns into return values of 0.1, 0.2, 0.3, 0.4 seconds.
    for (int i = 1; i < threadCount; i++) {
      stopwatch.releaseLastSleeper();
      stopwatch.awaitSleeperCount(1);
    }
    stopwatch.releaseLastSleeper();
    joinAll(threads);

    assertThat(new ArrayList<>(returnValuesInReturnOrder))
        .containsExactly(0.1, 0.2, 0.3, 0.4)
        .inOrder();
  }

  public void testAcquiresReturnInFifoOrderWhenAlreadyOverdue() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    int threadCount = 4;
    Queue<Double> returnValuesInReturnOrder = new ConcurrentLinkedQueue<>();
    List<Thread> threads = startAcquirers(limiter, threadCount, returnValuesInReturnOrder);

    awaitAllBlocked(threads);
    stopwatch.awaitSleeperCount(1);

    // Every reservation becomes overdue while the threads are still queued. Even so, a later
    // ticket may not overtake an earlier one.
    stopwatch.advanceMillis(SECONDS.toMillis(1));
    stopwatch.gateAllSleeps(); // gate even the zero-length sleeps of the overdue threads
    for (int i = 1; i < threadCount; i++) {
      stopwatch.releaseNextSleeper();
      stopwatch.awaitSleeperCount(1);
    }
    stopwatch.releaseNextSleeper();
    joinAll(threads);

    assertThat(new ArrayList<>(returnValuesInReturnOrder))
        .containsExactly(0.1, 0.2, 0.3, 0.4)
        .inOrder();
  }

  public void testAcquireReturnsComputedWaitNotActualWait() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    Queue<Double> returnValues = new ConcurrentLinkedQueue<>();
    List<Thread> threads = startAcquirers(limiter, 2, returnValues);
    awaitAllBlocked(threads);
    stopwatch.awaitSleeperCount(1);

    // The second ticket's deadline passes long before the first thread is released, so the
    // second thread actually waits much longer than the wait it was charged for. The return
    // value is still the computed wait, not the actual time spent waiting.
    stopwatch.advanceMillis(SECONDS.toMillis(10));
    stopwatch.gateAllSleeps(); // gate even the zero-length sleep of the overdue thread
    stopwatch.releaseNextSleeper();
    stopwatch.awaitSleeperCount(1);
    stopwatch.releaseNextSleeper();
    joinAll(threads);

    assertThat(new ArrayList<>(returnValues)).containsExactly(0.1, 0.2).inOrder();
  }

  public void testTryAcquireGrantsIdlePermitDespiteQueue() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    // One thread takes the next ticket and gets stuck inside its sleep, staying in the queue.
    Thread sleeper = new Thread(limiter::acquire);
    sleeper.start();
    stopwatch.awaitSleeperCount(1);

    // While the permit is not yet available, tryAcquire() refuses as usual.
    assertThat(limiter.tryAcquire()).isFalse();

    // The permit becomes idle while the queued thread is still stuck. A tryAcquire() must be
    // granted on the spot instead of being sent to the back of the queue.
    stopwatch.advanceMillis(500);
    AtomicBoolean acquired = new AtomicBoolean();
    Thread tryAcquirer = new Thread(() -> acquired.set(limiter.tryAcquire()));
    tryAcquirer.start();
    join(tryAcquirer);
    assertThat(acquired.get()).isTrue();

    stopwatch.releaseNextSleeper();
    join(sleeper);
  }

  public void testTryAcquireWaitsForItsTurn() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    Queue<String> returnOrder = new ConcurrentLinkedQueue<>();
    Thread sleeper =
        new Thread(
            () -> {
              limiter.acquire();
              returnOrder.add("sleeper");
            });
    sleeper.start();
    stopwatch.awaitSleeperCount(1);

    AtomicBoolean acquired = new AtomicBoolean();
    Thread tryAcquirer =
        new Thread(
            () -> {
              acquired.set(limiter.tryAcquire(1, 10, SECONDS));
              returnOrder.add("tryAcquirer");
            });
    tryAcquirer.start();
    awaitAllBlocked(ImmutableList.of(tryAcquirer));

    stopwatch.releaseNextSleeper(); // the sleeper returns, then tryAcquirer's turn starts
    stopwatch.awaitSleeperCount(1);
    stopwatch.releaseNextSleeper();
    join(sleeper);
    join(tryAcquirer);

    assertThat(acquired.get()).isTrue();
    assertThat(new ArrayList<>(returnOrder)).containsExactly("sleeper", "tryAcquirer").inOrder();
  }

  public void testTryAcquireTimeoutLeavesQueueCleanly() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    // The head of the queue gets stuck inside its sleep.
    Thread sleeper = new Thread(limiter::acquire);
    sleeper.start();
    stopwatch.awaitSleeperCount(1);

    // A tryAcquire behind it reserves a ticket, but its (real-time) timeout expires while it
    // waits for the stuck head, so it gives up and leaves the queue.
    AtomicBoolean acquired = new AtomicBoolean(true);
    Thread tryAcquirer = new Thread(() -> acquired.set(limiter.tryAcquire(1, 200, MILLISECONDS)));
    tryAcquirer.start();
    join(tryAcquirer);
    assertThat(acquired.get()).isFalse();

    // The threads behind the timed-out thread must not wait for a signal that would never come.
    stopwatch.releaseNextSleeper();
    join(sleeper);

    AtomicReference<Double> nextWait = new AtomicReference<>();
    Thread next = new Thread(() -> nextWait.set(limiter.acquire()));
    next.start();
    stopwatch.awaitSleeperCount(1);
    stopwatch.releaseNextSleeper();
    join(next);
    assertThat(nextWait.get()).isWithin(EPSILON).of(0.3);
  }

  public void testInterruptedThreadLeavesQueueCleanly() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    AtomicBoolean interrupted = new AtomicBoolean();
    AtomicReference<Double> interruptedWait = new AtomicReference<>();
    Thread head = new Thread(limiter::acquire);
    Thread middle =
        new Thread(
            () -> {
              interruptedWait.set(limiter.acquire());
              interrupted.set(Thread.currentThread().isInterrupted());
            });
    AtomicReference<Double> tailWait = new AtomicReference<>();
    Thread tail = new Thread(() -> tailWait.set(limiter.acquire()));
    head.start();
    stopwatch.awaitSleeperCount(1);
    middle.start();
    tail.start();
    awaitAllBlocked(ImmutableList.of(middle, tail));

    // Interrupt the queued thread: it leaves the queue, keeps its reservation, and still
    // returns the computed wait with its interrupt status restored.
    middle.interrupt();
    stopwatch.awaitSleeperCount(2); // the interrupted thread now sleeps until its deadline

    stopwatch.releaseNextSleeper(); // the head returns and signals the tail, not the middle
    stopwatch.awaitSleeperCount(2); // the tail is now sleeping, behind no one else
    stopwatch.releaseNextSleeper(); // the interrupted thread's sleep (already abandoned)
    stopwatch.releaseNextSleeper(); // the tail returns
    join(head);
    join(middle);
    join(tail);

    assertThat(interrupted.get()).isTrue();
    assertThat(interruptedWait.get()).isWithin(EPSILON).of(0.2);
    assertThat(tailWait.get()).isWithin(EPSILON).of(0.3);
  }

  public void testSetRateDoesNotDeadlockWhileThreadsAreQueued() throws Exception {
    ControllableStopwatch stopwatch = new ControllableStopwatch();
    RateLimiter limiter = RateLimiter.create(PERMITS_PER_SECOND, stopwatch);
    assertThat(limiter.acquire()).isWithin(EPSILON).of(0.0);

    Queue<Double> returnValues = new ConcurrentLinkedQueue<>();
    List<Thread> threads = startAcquirers(limiter, 2, returnValues);
    awaitAllBlocked(threads);
    stopwatch.awaitSleeperCount(1);

    // setRate must not block behind the queued threads, and tickets that are already reserved
    // keep the wait they were charged for under the old rate.
    Thread rateChanger = new Thread(() -> limiter.setRate(20.0));
    rateChanger.start();
    join(rateChanger);
    assertThat(limiter.getRate()).isEqualTo(20.0);

    stopwatch.releaseNextSleeper();
    stopwatch.awaitSleeperCount(1);
    stopwatch.releaseNextSleeper();
    joinAll(threads);

    assertThat(new ArrayList<>(returnValues)).containsExactly(0.1, 0.2).inOrder();
  }

  /**
   * Starts {@code count} threads that each acquire one permit and record the returned wait (and
   * thereby their return order) in {@code returnValues}.
   */
  private static List<Thread> startAcquirers(
      RateLimiter limiter, int count, Queue<Double> returnValues) {
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Thread thread = new Thread(() -> returnValues.add(limiter.acquire()));
      thread.start();
      threads.add(thread);
    }
    return threads;
  }

  /** Waits until every thread is blocked, which implies it has already reserved its ticket. */
  private static void awaitAllBlocked(List<Thread> threads) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + SECONDS.toNanos(10);
    for (Thread thread : threads) {
      while (thread.getState() != Thread.State.WAITING
          && thread.getState() != Thread.State.TIMED_WAITING) {
        if (System.nanoTime() >= deadlineNanos) {
          fail("thread did not start waiting: " + thread.getState());
        }
        Thread.sleep(10);
      }
    }
  }

  private static void join(Thread thread) throws InterruptedException {
    thread.join(SECONDS.toMillis(10));
    assertThat(thread.isAlive()).isFalse();
  }

  private static void joinAll(List<Thread> threads) throws InterruptedException {
    for (Thread thread : threads) {
      join(thread);
    }
  }

  /**
   * A stopwatch whose clock only advances when the test advances it, and whose sleeps block until
   * the test releases them individually. This lets tests hold a thread inside the rate limiter at
   * an exact point and resolve the race between threads in any order they choose.
   */
  private static final class ControllableStopwatch extends SleepingStopwatch {
    private long nowMicros;
    private final ArrayDeque<CountDownLatch> sleepers = new ArrayDeque<>(); // guarded by this
    private boolean gateAllSleeps;

    @Override
    protected synchronized long readMicros() {
      return nowMicros;
    }

    synchronized void advanceMillis(long millis) {
      nowMicros += MILLISECONDS.toMicros(millis);
    }

    /** Starts gating even zero-length sleeps, so that overdue threads complete one at a time. */
    synchronized void gateAllSleeps() {
      gateAllSleeps = true;
    }

    @Override
    protected void sleepMicrosUninterruptibly(long micros) {
      CountDownLatch latch = new CountDownLatch(1);
      synchronized (this) {
        if (micros <= 0 && !gateAllSleeps) {
          return;
        }
        sleepers.add(latch);
        notifyAll();
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        // Like the system stopwatch: restore the interrupt status and keep going.
        Thread.currentThread().interrupt();
      }
    }

    /** Waits until {@code count} sleeps are blocked inside the rate limiter. */
    synchronized void awaitSleeperCount(int count) throws InterruptedException {
      long deadlineNanos = System.nanoTime() + SECONDS.toNanos(10);
      while (sleepers.size() < count) {
        if (System.nanoTime() >= deadlineNanos) {
          fail("expected " + count + " sleepers, got " + sleepers.size());
        }
        wait(10);
      }
    }

    /** Releases the oldest blocked sleeper, letting it return from its sleep. */
    synchronized void releaseNextSleeper() {
      CountDownLatch sleeper = sleepers.poll();
      assertThat(sleeper).isNotNull();
      sleeper.countDown();
    }

    /** Releases the most recently registered sleeper, letting it return from its sleep. */
    synchronized void releaseLastSleeper() {
      CountDownLatch sleeper = sleepers.pollLast();
      assertThat(sleeper).isNotNull();
      sleeper.countDown();
    }
  }
}
