package com.microsoft.azure.data;

import com.azure.core.util.logging.ClientLogger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    private static final ClientLogger LOGGER = new ClientLogger(Main.class);

    private static long beginMilli = 0;

    private static String offsetFromBegin() {
        long offset = (TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS) - beginMilli);
        return String.format("%8.3f", offset / 1E3);
    }

    public static void main(String args[]) throws Exception {
        run();

//        System.exit(0);
    }

    private static void run() throws Exception {
        Sender sender = new Sender();

        LOGGER.info("begin");
        beginMilli = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

        sender.enqueue(600, "0").block();
        sleep(Duration.ofSeconds(1));
        sender.enqueue(300, "1").block();
        sleep(Duration.ofSeconds(1));
        sender.enqueue(200, "2").block();
        sleep(Duration.ofSeconds(18));
        // wait for auto flush

        sender.enqueue(2100, "0").block();
        // auto flush

        sleep(Duration.ofSeconds(3));

        sender.enqueue(700, "1").block();
        sender.enqueue(800, "2").block();
        LOGGER.info("{} flush", offsetFromBegin());
        sender.flush().toFuture();      // flush
        LOGGER.info("{} flush", offsetFromBegin());
        sender.flush().toFuture();      // flush but NOOP
        sleep(Duration.ofSeconds(3));
        LOGGER.info("{} flush", offsetFromBegin());
        sender.flush().toFuture();      // flush but NOOP

        sleep(Duration.ofSeconds(3));

        sender.enqueue(600, "0").block();
        sender.enqueue(200, "2").block();
        LOGGER.info("{} close", offsetFromBegin());
        sender.close();
        sleep(Duration.ofSeconds(5));
    }

    private static void sleep(Duration d) throws InterruptedException {
        Thread.sleep(d.toMillis());
    }

    public static class Sender implements AutoCloseable {
        private final ConcurrentMap<String, Channel> partitionChannels = new ConcurrentHashMap<>();

        public Mono<Void> enqueue(int count, String partitionId) {
            Channel channel = partitionChannels.computeIfAbsent(partitionId, Channel::new);
            return channel.enqueue(count);
        }

        public Mono<Void> flush() {
            return Flux.merge(partitionChannels.values().stream()
                            .map(Channel::flush)
                            .collect(Collectors.toList()))
                    .then();
        }

        @Override
        public void close() throws Exception {
            Flux.merge(partitionChannels.values().stream()
                            .map(Channel::close)
                            .collect(Collectors.toList()))
                    .then().block();
        }
    }

    public static class Channel {

        private final String partitionId;
        private final Duration maxWaitTime = Duration.ofSeconds(10);
        private final int maxPendingSize = 1000;

        private final ConcurrentLinkedDeque<OffsetDateTime> queue = new ConcurrentLinkedDeque<>();
        private final ConcurrentMap<Integer, CompletableFuture<Void>> poller = new ConcurrentHashMap<>(); // could not find an at-most-once in atomic
        private volatile boolean closed = false;
        private volatile boolean flushing = false;
        private volatile Mono<Void> cachedFlushing;

        public Channel(String partitionId) {
            this.partitionId = partitionId;
        }

        public Mono<Void> enqueue(int count) {
            if (closed) {
                return Mono.empty();
            }

            int previousCount = this.queue.size();
            this.queue.addAll(Collections.nCopies(count, OffsetDateTime.now()));
            if (previousCount == 0 && !poller.containsKey(0)) {
                // start loop
                poller.computeIfAbsent(0, ignored -> startLoop().then().toFuture());
            }
            return Mono.empty();    // seems no need to wait?
        }

        public Mono<Void> flush() {
            if (queue.size() == 0) {
                return Mono.empty();
            }

            if (!flushing) {
                flushing = true;
                cachedFlushing = flushAll().then().cache();
            }
            return cachedFlushing;
        }

        private Flux<Batch> flushAll() {
            if (queue.size() == 0) {
                return Flux.empty();
            }
            return Flux.defer(() -> {
                        flushing = true;
                        return createBatch().repeat(() -> queue.size() > 0);
                    })
                    .map(batch -> {
                        while (true) {
                            OffsetDateTime data = queue.peek();
                            if (data != null) {
                                boolean added = batch.tryAdd(1);
                                if (added) {
                                    queue.pop();
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        return batch;
                    })
                    .flatMap(this::send)    // do we need to send maintaining sequence?
                    .doOnComplete(() -> {
                        LOGGER.info("{} P{} finish sendAll", offsetFromBegin(), partitionId);
                        flushing = false;
                    });
        }

        private Flux<Batch> startLoop() {
            return Flux.interval(Duration.ofSeconds(1))
                    .takeUntil(ignored -> closed && queue.size() == 0)
                    .concatMap(ignored -> {
                        if (queue.size() == 0 || flushing) {
                            return Flux.empty();
                        }

                        OffsetDateTime data = queue.peek();
                        if (data != null && data.plus(maxWaitTime).isBefore(OffsetDateTime.now())) {
                            return flushAll();
                        } else if (queue.size() > maxPendingSize) {
                            return flushAll();
                        } else {
                            return Flux.empty();
                        }
                    });
        }

        private Mono<Batch> createBatch() {
            return Mono.defer(() -> Mono.just(new Batch()))
                    .map(b -> {
                        LOGGER.info("{} P{} begin create batch", offsetFromBegin(), partitionId);
                        return b;
                    })
                    .delayElement(Duration.ofMillis(100))
                    .map(b -> {
                        LOGGER.info("{} P{} end create batch", offsetFromBegin(), partitionId);
                        return b;
                    });
        }

        private Mono<Batch> send(Batch batch) {
            return Mono.just(batch)
                    .map(b -> {
                        LOGGER.info("{} P{} begin send batch, size {}", offsetFromBegin(), partitionId, b.count);
                        return b;
                    })
                    .delayElement(Duration.ofMillis(200))
                    .map(b -> {
                        LOGGER.info("{} P{} end send batch, size {}", offsetFromBegin(), partitionId, b.count);
                        return b;
                    });
        }

        public Mono<Void> close() {
            return Mono.defer(() -> {
                closed = true;
                return flush();
            });
        }
    }

    public static class Batch {

        private int count = 0;

        synchronized public boolean tryAdd(int count) {
            if (this.count + count > 500) {
                return false;
            } else {
                this.count += count;
                return true;
            }
        }

        public boolean isEmpty() {
            return count == 0;
        }
    }
}
