/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.log;

import c5db.C5CommonTestUtil;
import c5db.interfaces.log.SequentialEntryCodec;
import c5db.util.KeySerializingExecutor;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static c5db.log.ReplicatorLogGenericTestUtil.term;
import static com.google.common.math.IntMath.pow;
import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Provides for measurement of QuorumDelegatingLog's throughput as a function of log entry size,
 * number of simultaneous logging quorums, and number of IO threads handling requests.
 * <p>
 * This class logs several messages of various sizes and measures the total time it takes to complete
 * the log workload. Each quorum uses the same "script" of log entries, and entries are logged
 * one-at-a-time, cycling round robin through the different quorums.
 */
public class QuorumDelegatingLogPerformanceMeasurement {

  /**
   * QuorumDelegatingLog will use a fixed thread pool ExecutorService with this many threads.
   */
  private static final int THREAD_POOL_SIZE = 5;

  /**
   * The message (payload of the log entry) ranges over various powers of 2. These constants specify the
   * least and greatest powers of two of the messages that will be used. The details of the actual
   * sequence construction are below in {@link #constructLogSequence(int, int)}
   */
  private static final int SMALLEST_MESSAGE_SIZE_LOG_2 = 6;
  private static final int LARGEST_MESSAGE_SIZE_LOG_2 = 14;

  /**
   * Set this constant to true for detailed, human-readable output; false for just the throughput data
   */
  private static final boolean DETAILED_OUTPUT = false;

  /**
   * If this constant is true, the timed run will use dynamic throttling, attempting to log entries
   * roughly as fast as they are completed. A batch of entries will be logged, and then the run will
   * Thread.sleep(). After the sleep, if the last entry logged is still being processed, the next sleep
   * duration will be longer. Otherwise it will try for a shorter sleep duration next time.
   * <p>
   * If this constant is false, the timed run will use a fixed throttling rate determined dynamically
   * by the process just described, during the preceding (untimed) warm-up run.
   */
  private static final boolean DYNAMIC_THROTTLING_DURING_TIMED_RUN = true;

  /**
   * Execute a script of several timed runs, each with its own warmup.
   */
  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    List<Integer> logSequence = constructLogSequence(SMALLEST_MESSAGE_SIZE_LOG_2, LARGEST_MESSAGE_SIZE_LOG_2);

    for (int numQuorums = 50; numQuorums >= 5; numQuorums -= 5) {
      QuorumDelegatingLogPerformanceMeasurement fixture =
          new QuorumDelegatingLogPerformanceMeasurement(numQuorums, THREAD_POOL_SIZE, logSequence);
      fixture.outputTotalSizeThatWillBeWritten();
      fixture.outputMessageSequenceStatistics();
      fixture.doWarmUp();
      fixture.doTimedRun();
    }
  }

  private final List<Integer> logSequence;
  private final long totalMessageSizeB;
  private final long numberOfLogCallsBetweenSleeps;
  private final int numThreads;
  private final int numQuorums;
  private final List<String> quorumList;
  private final Path testDir = (new C5CommonTestUtil()).getDataTestDir("benchmark");

  private long dynamicSleepIntervalMillis = 50;

  public QuorumDelegatingLogPerformanceMeasurement(int numQuorums, int numThreads, List<Integer> logSequence) {
    this.numQuorums = numQuorums;
    this.numThreads = numThreads;
    this.logSequence = logSequence;
    quorumList = getQuorumIds(numQuorums);
    totalMessageSizeB = computeTotalMessageSizeInBytes();
    numberOfLogCallsBetweenSleeps = calculateNumberOfLogCallsBetweenSleeps();
  }

  private void doWarmUp()
      throws IOException, InterruptedException, ExecutionException {
    detailPrintln("Beginning untimed warmup");
    doLogRun(true);
    detailPrintln(
        "Finished warmup; final message injection rate = "
            + formatDouble((double) numberOfLogCallsBetweenSleeps / dynamicSleepIntervalMillis * 1000.)
            + " messages per second (alternating "
            + numberOfLogCallsBetweenSleeps + " messages with "
            + dynamicSleepIntervalMillis + " ms sleeps)");
    detailPrintln("---");
  }

  private void doTimedRun()
      throws IOException, InterruptedException, ExecutionException {
    detailPrintln("Beginning timed run");
    long elapsedNanoseconds = doLogRun(DYNAMIC_THROTTLING_DURING_TIMED_RUN);
    detailPrintln("Finished timed run");
    detailPrintln("---");

    outputSummaryData(elapsedNanoseconds);
    outputTimedRunResults(elapsedNanoseconds);
  }

  private long doLogRun(boolean dynamicSleepInterval)
      throws IOException, InterruptedException, ExecutionException {
    final LogFileService logFileService = new LogFileService(testDir);

    detailPrintln("Logging to " + testDir.toString());

    long sleepCountdown = numberOfLogCallsBetweenSleeps;
    long startTime;

    try (OLog log = getLog(logFileService)) {
      for (String quorumId : quorumList) {
        log.openAsync(quorumId).get();
      }

      startTime = System.nanoTime();

      for (int messageSizeLog2 : logSequence) {
        for (String quorumId : quorumList) {
          List<OLogEntry> entryList = Lists.newArrayList(constructLogEntry(messageSizeLog2, quorumId));
          ListenableFuture<Boolean> lastFuture = log.logEntries(entryList, quorumId);

          sleepCountdown--;

          if (sleepCountdown == 0) {
            if (dynamicSleepInterval) {
              sleepForDynamicDuration(lastFuture);
            } else {
              Thread.sleep(dynamicSleepIntervalMillis);
            }

            sleepCountdown = numberOfLogCallsBetweenSleeps;
          }
        }
      }
    } finally {
      logFileService.clearAllLogs();
    }

    return System.nanoTime() - startTime;
  }

  private void sleepForDynamicDuration(ListenableFuture<Boolean> lastFuture) throws InterruptedException {
    Thread.sleep(dynamicSleepIntervalMillis);
    if (lastFuture.isDone() && dynamicSleepIntervalMillis > 0) {
      dynamicSleepIntervalMillis--;
    } else {
      dynamicSleepIntervalMillis++;
    }
  }

  private void outputTotalSizeThatWillBeWritten() {
    detailPrintln(
        formatDouble((double) totalMessageSizeB / pow(2, 30))
            + " GiB will be written to disk ("
            + formatDouble((double) totalMessageSizeB / pow(2, 30) / numQuorums)
            + " GiB per quorum), not including entry headers or CRCs");
    detailPrintln("---");
  }

  private void outputMessageSequenceStatistics() {
    int messagesPerQuorum = logSequence.size();
    long totalNumberOfMessages = messagesPerQuorum * numQuorums;
    double averageMessageSizeB = (double) totalMessageSizeB / totalNumberOfMessages;
    double stdDevMessageSizeB = computeStdDevMessageSizeInBytes(averageMessageSizeB);
    double totalMessageSizeGiB = (double) totalMessageSizeB / pow(2, 30);

    detailPrintln("Number of quorums: " + numQuorums);
    detailPrintln("Number of threads: " + numThreads);
    detailPrintln("Total number of messages to log: " + totalNumberOfMessages);
    detailPrintln("Minimum message size (B): " + computeMinMessageSizeInBytes());
    detailPrintln("Average message size (B): " + formatDouble(averageMessageSizeB));
    detailPrintln("Standard deviation message size (B): " + formatDouble(stdDevMessageSizeB));
    detailPrintln("Total size of messages to log (GiB): " + formatDouble(totalMessageSizeGiB));
    detailPrintln("---");
  }

  private void outputTimedRunResults(long elapsedNanoseconds) {
    double elapsedSeconds = (double) elapsedNanoseconds / 1000000000.;
    double throughputMiBps = computeThroughputInMiBps(elapsedNanoseconds);

    detailPrintln("Elapsed time (seconds): " + formatDouble(elapsedSeconds));
    detailPrintln("Throughput (MiB per second): " + formatDouble(throughputMiBps));
    detailPrintln("--------------------------\n");
  }

  private void outputSummaryData(long elapsedNanoseconds) {
    System.out.println(numQuorums + "\t" + numThreads + "\t" + computeThroughputInMiBps(elapsedNanoseconds));
  }

  private void detailPrintln(String string) {
    if (DETAILED_OUTPUT) {
      System.out.println(string);
    }
  }

  private long calculateNumberOfLogCallsBetweenSleeps() {
    return (long) Math.ceil(((double) logSequence.size() * numQuorums / 500));
  }

  private double computeThroughputInMiBps(long elapsedNanoseconds) {
    double elapsedSeconds = (double) elapsedNanoseconds / 1000000000.;
    return totalMessageSizeB / pow(2, 20) / elapsedSeconds;
  }

  private long computeMinMessageSizeInBytes() {
    return pow(2, Ordering.<Integer>natural().min(logSequence));
  }

  private long computeTotalMessageSizeInBytes() {
    long sum = 0;
    for (int m : logSequence) {
      sum += pow(2, m);
    }
    return sum * numQuorums;
  }

  private double computeStdDevMessageSizeInBytes(double averageMessageSizeB) {
    double sum = 0;
    int sequenceSize = logSequence.size();

    for (int m : logSequence) {
      double differenceFromAverage = pow(2, m) - averageMessageSizeB;
      sum += (differenceFromAverage * differenceFromAverage) / sequenceSize;
    }
    return Math.sqrt(sum);
  }

  private OLog getLog(LogFileService logFileService) {
    KeySerializingExecutor executor = new WrappingKeySerializingExecutor(newFixedThreadPool(numThreads));
    return new QuorumDelegatingLog(logFileService,
        executor,
        (OLogEntryOracle.OLogEntryOracleFactory) new OLogEntryOracle.OLogEntryOracleFactory() {
          @Override
          public OLogEntryOracle create() {
            return new NavigableMapOLogEntryOracle();
          }
        },
        new LogPersistenceService.PersistenceNavigatorFactory() {
          @Override
          public LogPersistenceService.PersistenceNavigator create(LogPersistenceService.BytePersistence persistence, SequentialEntryCodec<?> encoding, long offset) {
            return new InMemoryPersistenceNavigator<>(persistence, encoding, offset);
          }
        });
  }

  /**
   * Static members
   */

  private static final Random rand = new Random();
  private static final byte[] bytes = new byte[pow(2, LARGEST_MESSAGE_SIZE_LOG_2)];

  static {
    rand.nextBytes(bytes);
  }

  private static OLogEntry constructLogEntry(int messageSizeLog2, String quorumId) {
    int size = pow(2, messageSizeLog2);
    List<ByteBuffer> data = Lists.newArrayList(ByteBuffer.wrap(bytes, 0, size));
    return new OLogEntry(quorumNextSeqNum(quorumId), term(1), new OLogRawDataContent(data));
  }

  private static final Map<String, Long> quorumSeqNums = new HashMap<>();

  private static long quorumNextSeqNum(String quorumId) {
    if (!quorumSeqNums.containsKey(quorumId)) {
      quorumSeqNums.put(quorumId, 1L);
      return 1;
    } else {
      long nextSeqNum = quorumSeqNums.get(quorumId) + 1;
      quorumSeqNums.put(quorumId, nextSeqNum);
      return nextSeqNum;
    }
  }

  private static List<String> getQuorumIds(int numQuorums) {
    List<String> quorums = new ArrayList<>();
    for (int i = 1; i <= numQuorums; i++) {
      quorums.add(quorumId(i));
    }
    return quorums;
  }

  private static String quorumId(int quorumNum) {
    return String.format("%05d", quorumNum);
  }

  private static String formatDouble(double d) {
    return String.format("%.2f", d);
  }

  private static List<Integer> constructLogSequence(int minMessageSizeLog2, int maxMessageSizeLog2) {
    final Map<Integer, List<Integer>> logSequences = new HashMap<>();

    logSequences.put(minMessageSizeLog2, Lists.newArrayList(minMessageSizeLog2));
    return constructLogSequence0(maxMessageSizeLog2, logSequences);
  }

  /**
   * Recursively construct sequence of messages. All messages sizes are stored log 2, so a 128 byte message
   * is represented by 7. The sequence is built recursively, each sequence containing three copies of the
   * previous sequence:
   * <p>
   * First sequence:     7
   * Second sequence:    7, 8, 7, 7, 8
   * Third sequence:    (7, 8, 7, 7, 8), 9, (7, 8, 7, 7, 8), (7, 8, 7, 7, 8), 9
   * Fourth sequence:  ((7, 8, 7, 7, 8), 9, (7, 8, 7, 7, 8), (7, 8, 7, 7, 8), 9), 10, ((7, 8, 7, ...
   */
  private static List<Integer> constructLogSequence0(int k, Map<Integer, List<Integer>> logSequences) {
    if (logSequences.containsKey(k)) {
      return logSequences.get(k);

    } else {
      List<Integer> seqK = new ArrayList<>();
      List<Integer> seqKMinus1 = constructLogSequence0(k - 1, logSequences);

      seqK.addAll(seqKMinus1);
      seqK.add(k);
      seqK.addAll(seqKMinus1);
      seqK.addAll(seqKMinus1);
      seqK.add(k);

      logSequences.put(k, seqK);
      return seqK;
    }
  }
}
