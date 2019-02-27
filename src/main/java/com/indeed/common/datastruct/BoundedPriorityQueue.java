/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.common.datastruct;

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
        this.pq = new PriorityQueue<>(10, comparator);
        this.comparator = comparator;
        this.maxCapacity = maxCapacity;
    }

    public static <E> BoundedPriorityQueue<E> newInstance(int maxCapacity, Comparator<? super E> comparator) {
        return new BoundedPriorityQueue<>(comparator, maxCapacity);
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
