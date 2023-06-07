package com.amazon.ion.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class _Private_RecyclingQueue<T> {

    /**
     * Factory for new stack elements.
     * @param <T> the type of element.
     */
    public interface ElementFactory<T> {

        /**
         * @return a new instance.
         */
        T newElement();
    }

    private class ElementIterator implements Iterator<T> {

        int i = 0;

        @Override
        public boolean hasNext() {
            return i <= currentIndex;
        }

        @Override
        public T next() {
            return elements.get(i++);
        }
    }

    private final ElementIterator iterator;
    private final List<T> elements;
    private final ElementFactory<T> elementFactory;
    private int currentIndex;
    private T top;

    /**
     * @param initialCapacity the initial capacity of the underlying collection.
     * @param elementFactory the factory used to create a new element on {@link #push()} when the stack has
     *                       not previously grown to the new depth.
     */
    public _Private_RecyclingQueue(int initialCapacity, ElementFactory<T> elementFactory) {
        elements = new ArrayList<T>(initialCapacity);
        this.elementFactory = elementFactory;
        currentIndex = -1;
        iterator = new ElementIterator();

    }

    public void truncate(int index) {
        currentIndex = index;
    }

    public T get(int index) {
        return elements.get(index);
    }

    /**
     * Pushes an element onto the top of the stack, instantiating a new element only if the stack has not
     * previously grown to the new depth.
     * @return the element at the top of the stack after the push. This element must be initialized by the caller.
     */
    public T push() {
        currentIndex++;
        if (currentIndex >= elements.size()) {
            top = elementFactory.newElement();
            elements.add(top);
        }  else {
            top = elements.get(currentIndex);
        }
        return top;
    }

    public void remove() {
        currentIndex = Math.max(-1, currentIndex - 1);
    }

    public Iterator<T> iterate() {
        iterator.i = 0;
        return iterator;
    }

    public void extend(_Private_RecyclingQueue<T> patches) {
        elements.addAll(patches.elements);
    }

    public boolean isEmpty() {
        return currentIndex < 0;
    }

    public void clear() {
        currentIndex = -1;
    }

    public int size() {
        return currentIndex + 1;
    }

}