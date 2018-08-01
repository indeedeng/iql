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

package com.indeed.iql2.execution;

import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.AbstractIntSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author jwolfe
 */
public class DenseInt2ObjectMap<T> extends AbstractInt2ObjectMap<T> {
    private static final Logger log = Logger.getLogger(DenseInt2ObjectMap.class);

    private final ArrayList<T> elements = new ArrayList<>();

    private int size = 0;

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (!(key instanceof Integer)) {
            throw new ClassCastException("Keys to DenseInt2ObjectMap must be Integer.");
        } else {
            final int intKey = (Integer)key;
            return elements.size() > intKey && elements.get(intKey) != null;
        }
    }

    @Override
    public boolean containsValue(Object value) {
        for (final T element : elements) {
            if (element != null && (value == element || element.equals(value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public T get(Object key) {
        if (!(key instanceof Integer)) {
            throw new ClassCastException("Keys to DenseInt2ObjectMap must be Integer.");
        } else {
            final int intKey = (Integer)key;
            return get(intKey);
        }
    }

    public T get(int key) {
        return elements.size() > key ? elements.get(key) : null;
    }

    @Override
    public T remove(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(int i) {
        return elements.size() > i && elements.get(i) != null;
    }

    @Override
    public void defaultReturnValue(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T defaultReturnValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T put(Integer key, T value) {
        final int intKey = key;
        return put(intKey, value);
    }

    public T put(int key, T value) {
        if (value == null) {
            throw new NullPointerException("DenseInt2ObjectMap does not support null values.");
        }
        if (elements.size() <= key) {
            while (elements.size() < key) {
                elements.add(null);
            }
            elements.add(value);
            size++;
            return null;
        } else {
            final T oldValue = elements.set(key, value);
            if (oldValue == null) size++;
            return oldValue;
        }
    }


    @Override
    public T remove(Object key) {
        if (!(key instanceof Integer)) {
            throw new ClassCastException("Keys to DenseInt2ObjectMap must be Integer.");
        } else {
            final int intKey = (Integer)key;
            if (elements.size() > intKey) {
                final T oldValue = elements.remove(intKey);
                if (oldValue != null) size--;
                return oldValue;
            } else {
                return null;
            }
        }
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends T> m) {
        for (final Map.Entry<? extends Integer, ? extends T> entry : m.entrySet()) {
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        size = 0;
        elements.clear();
    }

    @Override
    public it.unimi.dsi.fastutil.ints.IntSet keySet() {
        return new AbstractIntSet() {
            @Override
            public IntIterator iterator() {
                return new AbstractIntIterator() {
                    int nextIndex = 0;

                    @Override
                    public boolean hasNext() {
                        while (nextIndex < elements.size() && elements.get(nextIndex) == null) {
                            nextIndex++;
                        }
                        if (nextIndex >= elements.size() || elements.get(nextIndex) == null) {
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public int nextInt() {
                        if (hasNext()) {
                            return nextIndex++;
                        }
                        throw new NoSuchElementException("nextInt() called when hasNext() was false");
                    }
                };
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    @Override
    public it.unimi.dsi.fastutil.objects.ObjectCollection<T> values() {
        return new AbstractObjectCollection<T>() {
            @Override
            public it.unimi.dsi.fastutil.objects.ObjectIterator<T> iterator() {
                return new AbstractObjectIterator<T>() {
                    int nextIndex = 0;

                    @Override
                    public boolean hasNext() {
                        while (nextIndex < elements.size() && elements.get(nextIndex) == null) {
                            nextIndex++;
                        }
                        if (nextIndex >= elements.size() || elements.get(nextIndex) == null) {
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public T next() {
                        if (hasNext()) {
                            return elements.get(nextIndex++);
                        }
                        throw new NoSuchElementException("next() called when hasNext() was false");
                    }
                };
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    @Override
    public ObjectSet<Int2ObjectMap.Entry<T>> int2ObjectEntrySet() {
        return new AbstractObjectSet<Int2ObjectMap.Entry<T>>() {
            @Override
            public it.unimi.dsi.fastutil.objects.ObjectIterator<Int2ObjectMap.Entry<T>> iterator() {
                return new AbstractObjectIterator<Int2ObjectMap.Entry<T>>() {
                    int nextIndex = 0;

                    @Override
                    public boolean hasNext() {
                        while (nextIndex < elements.size() && elements.get(nextIndex) == null) {
                            nextIndex++;
                        }
                        if (nextIndex >= elements.size() || elements.get(nextIndex) == null) {
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public Int2ObjectMap.Entry<T> next() {
                        if (hasNext()) {
                            return new AbstractInt2ObjectMap.BasicEntry<T>(nextIndex, elements.get(nextIndex++));
                        }
                        throw new NoSuchElementException("next() called when hasNext() was false");
                    }
                };
            }

            @Override
            public int size() {
                return size;
            }
        };
    }
}
