package org.huoyu.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Author: huoyu
 * 数据获取器，可以并发的去获取
 * 用途：在组装一些属性比价多的对象时使用，避免多过的耗时操作造成长时间的串行等待
 **/

public class DataFetcher<D> {

    private final D data;

    private final List<Node<?>> nodes;

    private static final Executor DEFAULT_EXECUTOR;

    private static final int DEFAULT_TIME_WAIT_SECONDS = 10;

    public DataFetcher(D data) {
        this.data = data;
        this.nodes = new ArrayList<>();
    }

    public void addNode(Node<?> node) {
        Objects.requireNonNull(node);
        this.nodes.add(node);
    }

    public <P> void addNode(Supplier<P> fetchFunction, BiConsumer<D, P> setter) {
        this.nodes.add(new Node<>(fetchFunction, setter));
    }

    public void fetch() {
        fetch(DEFAULT_TIME_WAIT_SECONDS, DEFAULT_EXECUTOR);
    }

    public void fetch(int timeWaitSeconds) {
        fetch(timeWaitSeconds, DEFAULT_EXECUTOR);
    }

    public void fetch(int timeWaitSeconds, Executor executor) {
        List<CompletableFuture<?>> futures = nodes.stream()
                .map(node -> CompletableFuture.runAsync(node::fetch, executor))
                .collect(Collectors.toList());

        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(timeWaitSeconds, TimeUnit.SECONDS);
            nodes.forEach(Node::propSetter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch, causes " + e.getMessage(), e);
        }
    }

    public class Node<Prop> {

        private Prop p;

        private final Supplier<Prop> fetchFunction;

        private final BiConsumer<D, Prop> setter;

        public Node(Supplier<Prop> fetchFunction, BiConsumer<D, Prop> setter) {
            this.fetchFunction = fetchFunction;
            this.setter = setter;
        }

        void fetch() {
            p = this.fetchFunction.get();
        }

        void propSetter() {
            this.setter.accept(data, p);
        }
    }

    static {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        DEFAULT_EXECUTOR = new ThreadPoolExecutor(
                availableProcessors * 2 + 1,
                availableProcessors * 3,
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2028),
                new DefaultThreadFactory());
    }


    private static class DefaultThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "dataFetcher-thread-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
