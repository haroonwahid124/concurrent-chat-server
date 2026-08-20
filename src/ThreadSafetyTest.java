import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadSafetyTest {

    static final int THREAD_COUNT = 200;
    static final int ITERATIONS = 50;


    static void testSequenceCounter_SAFE() throws Exception {
        System.out.println("\n FEATURE 1: Sequence Counter (SAFE — AtomicLong) ");

        AtomicLong safeSeq = new AtomicLong(0);
        Set<Long> allSeqs = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                for (int j = 0; j < ITERATIONS; j++) {
                    long seq = safeSeq.incrementAndGet();
                    if (!allSeqs.add(seq)) {
                        duplicates.incrementAndGet();
                    }
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();   // all threads fire at once
        done.await();

        long expected = (long) THREAD_COUNT * ITERATIONS;
        System.out.println("  Expected unique seqs: " + expected);
        System.out.println("  Actual unique seqs:   " + allSeqs.size());
        System.out.println("  Duplicates found:     " + duplicates.get());
        System.out.println("  RESULT: " + (duplicates.get() == 0 ? "PASS ✓" : "FAIL ✗"));
    }

    static void testSequenceCounter_UNSAFE() throws Exception {
        System.out.println("\n FEATURE 1: Sequence Counter (UNSAFE — plain long)");

        // Simulates UnsafeServer.RoomState.nextSeq (a plain long)
        final long[] unsafeSeq = {0};
        Set<Long> allSeqs = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                for (int j = 0; j < ITERATIONS; j++) {
                    // UNSAFE: read-then-write, exactly like UnsafeServer
                    long seq = unsafeSeq[0] + 1;
                    unsafeSeq[0] = seq;
                    if (!allSeqs.add(seq)) {
                        duplicates.incrementAndGet();
                    }
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        long expected = (long) THREAD_COUNT * ITERATIONS;
        System.out.println("  Expected unique seqs: " + expected);
        System.out.println("  Actual unique seqs:   " + allSeqs.size());
        System.out.println("  Duplicates found:     " + duplicates.get());
        System.out.println("  RESULT: " + (duplicates.get() > 0 ? "FAIL ✗ (race condition detected!)" : "PASS ✓ (no race — try increasing threads)"));
    }


    //  FEATURE 2: Mailbox (add messages for offline users)
    //  Safe   = Collections.synchronizedList
    //  Unsafe = plain ArrayList


    static void testMailbox_SAFE() throws Exception {
        System.out.println("\n FEATURE 2: Mailbox (SAFE — synchronizedList)");

        List<String> inbox = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    for (int j = 0; j < ITERATIONS; j++) {
                        inbox.add("msg-" + id + "-" + j);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        int expected = THREAD_COUNT * ITERATIONS;
        System.out.println("  Expected messages: " + expected);
        System.out.println("  Actual messages:   " + inbox.size());
        System.out.println("  Exceptions:        " + errors.get());
        System.out.println("  RESULT: " + (inbox.size() == expected && errors.get() == 0 ? "PASS ✓" : "FAIL ✗"));
    }

    static void testMailbox_UNSAFE() throws Exception {
        System.out.println("\n FEATURE 2: Mailbox (UNSAFE — plain ArrayList)");

        // Plain ArrayList — exactly like UnsafeServer
        List<String> inbox = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    for (int j = 0; j < ITERATIONS; j++) {
                        inbox.add("msg-" + id + "-" + j);
                    }
                } catch (Exception e) {
                    // ArrayIndexOutOfBoundsException or other internal failures
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        int expected = THREAD_COUNT * ITERATIONS;
        System.out.println("  Expected messages: " + expected);
        System.out.println("  Actual messages:   " + inbox.size());
        System.out.println("  Exceptions:        " + errors.get());
        boolean failed = inbox.size() != expected || errors.get() > 0;
        System.out.println("  RESULT: " + (failed
                ? "FAIL ✗ (lost messages or exceptions!)"
                : "PASS ✓ (no race — try increasing threads)"));
    }


    //  FEATURE 3: Banned Words Filter
    //  Safe   = ConcurrentHashMap-backed set
    //  Unsafe = plain HashSet
    //  Race: one thread adds words while others iterate for filtering


    static void testBannedWords_SAFE() throws Exception {
        System.out.println("\nFEATURE 3: Banned Words Filter (SAFE — ConcurrentHashMap set)");

        Set<String> bannedWords = Collections.newSetFromMap(new ConcurrentHashMap<>());
        bannedWords.add("spam");
        bannedWords.add("curse");

        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT + 1);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT + 1);

        // Writer thread: keeps adding new banned words
        new Thread(() -> {
            ready.countDown();
            try { go.await(); } catch (InterruptedException e) { return; }

            try {
                for (int j = 0; j < ITERATIONS * 10; j++) {
                    bannedWords.add("word" + j);
                    // Also remove some to cause more churn
                    if (j > 5) bannedWords.remove("word" + (j - 5));
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            }
            done.countDown();
        }).start();

        // Reader threads: filter messages by iterating the set
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    for (int j = 0; j < ITERATIONS; j++) {
                        String msg = "This is a spam message with curse words";
                        String result = msg;
                        for (String word : bannedWords) {
                            if (result.toLowerCase().contains(word.toLowerCase())) {
                                result = result.replaceAll(
                                        "(?i)" + java.util.regex.Pattern.quote(word), "***");
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        System.out.println("  Exceptions during concurrent read/write: " + errors.get());
        System.out.println("  RESULT: " + (errors.get() == 0 ? "PASS ✓" : "FAIL ✗"));
    }

    static void testBannedWords_UNSAFE() throws Exception {
        System.out.println("\n FEATURE 3: Banned Words Filter (UNSAFE — plain HashSet) ");

        // Plain HashSet — exactly like UnsafeServer
        Set<String> bannedWords = new HashSet<>();
        bannedWords.add("spam");
        bannedWords.add("curse");

        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT + 1);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT + 1);

        // Writer thread: keeps adding/removing banned words
        new Thread(() -> {
            ready.countDown();
            try { go.await(); } catch (InterruptedException e) { return; }

            try {
                for (int j = 0; j < ITERATIONS * 10; j++) {
                    bannedWords.add("word" + j);
                    if (j > 5) bannedWords.remove("word" + (j - 5));
                }
            } catch (Exception e) {
                errors.incrementAndGet();
            }
            done.countDown();
        }).start();

        // Reader threads: iterate the set (causes ConcurrentModificationException)
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    for (int j = 0; j < ITERATIONS; j++) {
                        String msg = "This is a spam message with curse words";
                        String result = msg;
                        // This iteration WILL throw ConcurrentModificationException
                        for (String word : bannedWords) {
                            if (result.toLowerCase().contains(word.toLowerCase())) {
                                result = result.replaceAll(
                                        "(?i)" + java.util.regex.Pattern.quote(word), "***");
                            }
                        }
                    }
                } catch (ConcurrentModificationException e) {
                    errors.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        System.out.println("  ConcurrentModificationExceptions caught: " + errors.get());
        System.out.println("  RESULT: " + (errors.get() > 0
                ? "FAIL ✗ (ConcurrentModificationException detected!)"
                : "PASS ✓ (no race — try increasing threads)"));
    }


    //  FEATURE 4: Room Membership (join/leave while broadcasting)
    //  Safe   = ConcurrentHashMap.newKeySet()
    //  Unsafe = plain HashSet


    static void testRoomMembership_SAFE() throws Exception {
        System.out.println("\n FEATURE 4: Room Membership (SAFE — ConcurrentHashMap.newKeySet())");

        Set<String> members = ConcurrentHashMap.newKeySet();
        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        // Half the threads join/leave, half iterate (broadcast)
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    if (id % 2 == 0) {
                        // Joiner/leaver
                        for (int j = 0; j < ITERATIONS; j++) {
                            String name = "user-" + id + "-" + j;
                            members.add(name);
                            Thread.yield();
                            members.remove(name);
                        }
                    } else {
                        // Broadcaster (iterates the set)
                        for (int j = 0; j < ITERATIONS; j++) {
                            int count = 0;
                            for (String m : members) {
                                count++; // simulates sending a message
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        System.out.println("  Exceptions during concurrent join/leave/broadcast: " + errors.get());
        System.out.println("  RESULT: " + (errors.get() == 0 ? "PASS ✓" : "FAIL ✗"));
    }

    static void testRoomMembership_UNSAFE() throws Exception {
        System.out.println("\n FEATURE 4: Room Membership (UNSAFE — plain HashSet)");

        // Plain HashSet — exactly like UnsafeServer
        Set<String> members = new HashSet<>();
        AtomicInteger errors = new AtomicInteger(0);

        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int id = i;
            new Thread(() -> {
                ready.countDown();
                try { go.await(); } catch (InterruptedException e) { return; }

                try {
                    if (id % 2 == 0) {
                        // Joiner/leaver
                        for (int j = 0; j < ITERATIONS; j++) {
                            String name = "user-" + id + "-" + j;
                            members.add(name);
                            Thread.yield();
                            members.remove(name);
                        }
                    } else {
                        // Broadcaster — iterating a plain HashSet
                        // WILL throw ConcurrentModificationException
                        for (int j = 0; j < ITERATIONS; j++) {
                            int count = 0;
                            for (String m : members) {
                                count++;
                            }
                        }
                    }
                } catch (ConcurrentModificationException e) {
                    errors.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                done.countDown();
            }).start();
        }

        ready.await();
        go.countDown();
        done.await();

        System.out.println("  ConcurrentModificationExceptions caught: " + errors.get());
        System.out.println("  RESULT: " + (errors.get() > 0
                ? "FAIL ✗ (ConcurrentModificationException detected!)"
                : "PASS ✓ (no race — try increasing threads)"));
    }


    //  MAIN — runs all 8 tests


    public static void main(String[] args) throws Exception {
        System.out.println("     Thread Safety Test          ");
        System.out.println("    Threads: " + THREAD_COUNT + "  |  Iterations: " + ITERATIONS + "          ");

        // --- Feature 1: Sequence Counter ---
        testSequenceCounter_SAFE();
        testSequenceCounter_UNSAFE();

        // --- Feature 2: Mailbox ---
        testMailbox_SAFE();
        testMailbox_UNSAFE();

        // --- Feature 3: Banned Words ---
        testBannedWords_SAFE();
        testBannedWords_UNSAFE();

        // --- Feature 4: Room Membership ---
        testRoomMembership_SAFE();
        testRoomMembership_UNSAFE();


        System.out.println("All tests complete.");
        System.out.println("SAFE tests should PASS. UNSAFE tests should FAIL.");
    }
}