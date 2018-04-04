package com.indeed.common.datastruct;

import com.google.common.collect.Ordering;

import javax.annotation.Nonnull;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

// TODO: Move to https://github.com/indeedeng/util/tree/master/util-core/src/main/java/com/indeed/util/core/datastruct

/**
 * @author jwolfe
 */
public class BoundedPriorityQueue<E> extends AbstractQueue<E> {
    private final PriorityQueue<E> pq;
    private final Comparator<? super E> comparator;
    private final int maxCapacity;

    private BoundedPriorityQueue(Comparator<? super E> comparator, int maxCapacity) {
        // TODO: Figure out default capacity
        this.pq = new PriorityQueue<E>(10, comparator);
        this.comparator = comparator;
        this.maxCapacity = maxCapacity;
    }

    public static <E extends Comparable<E>> BoundedPriorityQueue<E> newInstance(int maxCapacity) {
        return new BoundedPriorityQueue<E>(Ordering.<E>natural(), maxCapacity);
    }

    public static <E> BoundedPriorityQueue<E> newInstance(int maxCapacity, Comparator<? super E> comparator) {
        return new BoundedPriorityQueue<E>(comparator, maxCapacity);
    }

    @Nonnull @Override
    public Iterator<E> iterator() {
        return pq.iterator();
    }

    @Override
    public int size() {
        return pq.size();
    }

    @Override
    public boolean offer(E e) {
        if (pq.size() < maxCapacity) {
            pq.add(e);
            return true;
        }
        if (maxCapacity != 0 && comparator.compare(e, pq.peek()) >= 0) {
            pq.add(e);
            pq.poll();
            return true;
        }
        return false;
    }

    @Override
    public E poll() {
        return pq.poll();
    }

    @Override
    public E peek() {
        return pq.peek();
    }

    public boolean isFull() {
        return pq.size() == maxCapacity;
    }

    @Override
    public String toString() {
        return "BoundedPriorityQueue{" +
                "pq=" + pq +
                ", comparator=" + comparator +
                ", maxCapacity=" + maxCapacity +
                '}';
    }
}
