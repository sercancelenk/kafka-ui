package com.trendyol.kafka.stream.api.util;

import com.trendyol.kafka.stream.api.model.Models;

import java.util.Comparator;
import java.util.PriorityQueue;

public class ThreadLocalPriorityQueue<T> {
    private static  final ThreadLocal<PriorityQueue<Models.MessageInfo>> threadLocalPriorityQueue =
            ThreadLocal.withInitial(() -> new PriorityQueue<>(Comparator.comparingLong(Models.MessageInfo::offset)));

    public static PriorityQueue<Models.MessageInfo> getPriorityQueue() {
        return threadLocalPriorityQueue.get();
    }

    public static void clear() {
        threadLocalPriorityQueue.get().clear();
    }
}