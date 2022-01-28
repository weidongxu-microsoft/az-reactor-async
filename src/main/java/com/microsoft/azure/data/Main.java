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
import java.util.stream.Collectors;

public class Main {

    private static final ClientLogger LOGGER = new ClientLogger(Main.class);

    public static void main(String args[]) throws Exception {
        run();

//        System.exit(0);
    }

    private static void run() throws Exception {
        Sender sender = new Sender();

        LOGGER.info("{} begin", OffsetDateTime.now());

        sender.enqueue(600, "0");
        sleep(Duration.ofSeconds(1));
        sender.enqueue(300, "1");
        sleep(Duration.ofSeconds(1));
        sender.enqueue(200, "2");
        sleep(Duration.ofSeconds(20));

        sender.enqueue(2100, "0");
        sleep(Duration.ofSeconds(5));

        sender.enqueue(700, "1");
        sender.enqueue(800, "2");
        LOGGER.info("{} flush", OffsetDateTime.now());
        sender.flush().toFuture();      // flush and subscribe
        sleep(Duration.ofSeconds(5));

        sender.enqueue(600, "0");
        LOGGER.info("{} close", OffsetDateTime.now());
        sender.close();
        sleep(Duration.ofSeconds(5));
    }

    private static void sleep(Duration d) throws InterruptedException {
        Thread.sleep(d.toMillis());
    }

    public static class Sender implements AutoCloseable {
        private final ConcurrentMap<String, Channel> partitionChannels = new ConcurrentHashMap<>();

        public Mono<Void> enqueue(int count, String partitionId) {
            Channel channel = partitionChannels.computeIfAbsent(partitionId, ignored -> new Channel());
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
            for (Channel channel : partitionChannels.values()) {
                channel.close();    // closeAsync?
            }
        }
    }

    public static class Channel implements AutoCloseable {

        Duration maxWaitTime = Duration.ofSeconds(10);
        int maxPendingSize = 1000;

        private final ConcurrentLinkedDeque<OffsetDateTime> queue = new ConcurrentLinkedDeque<>();
        private final ConcurrentMap<Integer, CompletableFuture<Void>> poller = new ConcurrentHashMap<>(); // could not find an at-most-once in atomic
        private volatile boolean closed = false;
        private volatile boolean flushing = false;
        private volatile Mono<Void> monoFlushing;

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
                monoFlushing = sendAll().then();
            }
            return monoFlushing;
        }

        private Flux<Batch> sendAll() {
            if (queue.size() == 0) {
                return Flux.empty();
            }
            return createBatch().repeat(() -> queue.size() > 0)
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
                        LOGGER.info("{} finish sendAll", OffsetDateTime.now());
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
                            return sendAll();
                        } else if (queue.size() > maxPendingSize) {
                            return sendAll();
                        } else {
                            return Flux.empty();
                        }
                    });
        }

        private Mono<Batch> createBatch() {
            return Mono.defer(() -> Mono.just(new Batch()))
                    .map(b -> {
                        LOGGER.info("{} begin create batch", OffsetDateTime.now());
                        return b;
                    })
                    .delayElement(Duration.ofMillis(100))
                    .map(b -> {
                        LOGGER.info("{} end create batch", OffsetDateTime.now());
                        return b;
                    });
        }

        private Mono<Batch> send(Batch batch) {
            return Mono.just(batch)
                    .map(b -> {
                        LOGGER.info("{} begin send batch, size {}", OffsetDateTime.now(), b.count);
                        return b;
                    })
                    .delayElement(Duration.ofMillis(200))
                    .map(b -> {
                        LOGGER.info("{} end send batch, size {}", OffsetDateTime.now(), b.count);
                        return b;
                    });
        }

        @Override
        public void close() throws Exception {
            closed = true;
            flush().block();
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
