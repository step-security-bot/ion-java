/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl._Private_CurriedValueFactory;
import com.amazon.ion.impl._Private_IonValue;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;


abstract class IonSequenceLite
    extends IonContainerLite
    implements IonSequence
{
    /**
     * A zero-length array.
     */
    protected static final IonValueLite[] EMPTY_VALUE_ARRAY = new IonValueLite[0];

    IonSequenceLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonSequenceLite(IonSequenceLite existing, IonContext context) {
        super(existing, context);
    }

    /**
     * Constructs a sequence value <em>not</em> backed by binary.
     *
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    IonSequenceLite(ContainerlessContext context,
                    Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException,
            IllegalArgumentException
    {
        this(context, (elements == null));
        assert _children == null;

        if (elements != null)
        {
            _children = new IonValueLite[elements.size()];
            for (Iterator i = elements.iterator(); i.hasNext();)
            {
                IonValueLite element = (IonValueLite) i.next();
                super.add(element);
            }
        }
    }

    //=========================================================================

    @Override
    public IonSequence clone() {
        return (IonSequence) deepClone(false);
    }

    @Override
    // Increasing visibility
    public boolean add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        // super.add will check for the lock
        return super.add(element);
    }

    public boolean addAll(Collection<? extends IonValue> c)
    {
        checkForLock();

        if (c == null) {
            throw new NullPointerException();
        }

        boolean changed = false;

        for (IonValue v : c)
        {
            changed = add(v) || changed;
        }

        return changed;
    }

    public boolean addAll(int index, Collection<? extends IonValue> c)
    {
        checkForLock();

        if (c == null) {
            throw new NullPointerException();
        }
        if (index < 0 || index > size())
        {
            throw new IndexOutOfBoundsException();
        }

        // TODO optimize to avoid n^2 shifting and renumbering of elements.
        boolean changed = false;

        for (IonValue v : c)
        {
            add(index++, v);
            changed = true;
        }

        if (changed) {
            patch_elements_helper(index);
        }

        return changed;
    }


    public ValueFactory add()
    {
        return new _Private_CurriedValueFactory(this.getSystem())
        {
            @Override
            protected void handle(IonValue newValue)
            {
                add(newValue);
            }
        };
    }


    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        add(index, (IonValueLite) element);
    }

    public ValueFactory add(final int index)
    {
        return new _Private_CurriedValueFactory(getSystem())
        {
            @Override
            protected void handle(IonValue newValue)
            {
                add(index, newValue);
                patch_elements_helper(index + 1);
            }
        };
    }

    public IonValue set(int index, IonValue element)
    {
        checkForLock();
        final IonValueLite concrete = ((IonValueLite) element);

        // NOTE: size calls makeReady() so we don't have to
        if (index < 0 || index >= size())
        {
            throw new IndexOutOfBoundsException("" + index);
        }

        validateNewChild(element);

        assert _children != null; // else index would be out of bounds above.
        concrete._context = getContextForIndex(element, index);
        IonValueLite removed = set_child(index, concrete);
        concrete._elementid(index);

        removed.detachFromContainer();
        // calls setDirty(), UNLESS it hits an IOException

        return removed;
    }

    public IonValue remove(int index)
    {
        checkForLock();

        if (index < 0 || index >= get_child_count()) {
            throw new IndexOutOfBoundsException("" + index);
        }

        IonValueLite v = get_child(index);
        assert(v._elementid() == index);
        remove_child(index);
        patch_elements_helper(index);
        return v;
    }

    public boolean remove(Object o)
    {
        checkForLock();

        int idx = lastIndexOf(o);
        if (idx < 0) {
            return false;
        }
        assert(o instanceof IonValueLite); // since it's in our current array
        assert( ((IonValueLite)o)._elementid() == idx );

        remove_child(idx);
        patch_elements_helper(idx);
        return true;
    }

    public boolean removeAll(Collection<?> c)
    {
        boolean changed = false;

        checkForLock();

        // remove the collection member if it is a
        // member of our child array keep track of
        // the lowest array index for later patching
        for (Object o : c) {
            int idx = lastIndexOf(o);
            if (idx >= 0) {
                assert(o == get_child(idx));
                remove_child(idx);
                patch_elements_helper(idx);
                changed = true;
            }
        }
        return changed;
    }

    public boolean retainAll(Collection<?> c)
    {
        checkForLock();

        if (get_child_count() < 1) return false;

        // TODO this method (and probably several others) needs optimization.
        IdentityHashMap<IonValue, IonValue> keepers =
            new IdentityHashMap<IonValue, IonValue>();

        for (Object o : c)
        {
            IonValue v = (IonValue) o;
            if (this == v.getContainer()) keepers.put(v, v);
        }

        boolean changed = false;
        for (int ii = get_child_count(); ii > 0; )
        {
            ii--;
            IonValue v = get_child(ii);
            if (! keepers.containsKey(v))
            {
                remove(v);
                patch_elements_helper(ii);
                changed = true;
            }
        }

        return changed;
    }

    public boolean contains(Object o)
    {
        if (o == null) {
            throw new NullPointerException();
        }
        if (!(o instanceof IonValue)) {
            throw new ClassCastException();
        }
        return ((IonValue)o).getContainer() == this;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Object o : c)
        {
            if (! contains(o)) return false;
        }
        return true;
    }

    public int indexOf(Object o)
    {
        if (o == null) {
            throw new NullPointerException();
        }
        _Private_IonValue v = (_Private_IonValue) o;
        if (this != v.getContainer()) return -1;
        return v.getElementId();
    }

    public int lastIndexOf(Object o)
    {
        return indexOf(o);
    }

    private static void checkSublistParameters(int size, int fromIndex, int toIndex)
    {
        if(fromIndex < 0)
        {
            throw new IndexOutOfBoundsException("fromIndex is less than zero");
        }
        if(toIndex < fromIndex)
        {
            throw new IllegalArgumentException("toIndex may not be less than fromIndex");
        }
        if(toIndex > size)
        {
            throw new IndexOutOfBoundsException("toIndex exceeds size");
        }
    }

    public List<IonValue> subList(int fromIndex, int toIndex)
    {
        checkSublistParameters(this.size(), fromIndex, toIndex);
        return new SubListView(fromIndex, toIndex);
    }

    public IonValue[] toArray()
    {
        if (get_child_count() < 1) return EMPTY_VALUE_ARRAY;

        IonValue[] array = new IonValue[get_child_count()];
        System.arraycopy(_children, 0, array, 0, get_child_count());
        return array;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a)
    {
        int size = get_child_count();
        if (a.length < size)
        {
            // TODO JDK 1.6 this could use Arrays.copyOf
            Class<?> type = a.getClass().getComponentType();
            // generates unchecked warning
            a = (T[]) Array.newInstance(type, size);
        }
        if (size > 0) {
            System.arraycopy(_children, 0, a, 0, size);
        }
        if (size < a.length) {
            // A surprising bit of spec.
            // this is required even with a 0 entries
            a[size] = null;
        }
        return a;
    }

    @SuppressWarnings("unchecked")
    public <T extends IonValue> T[] extract(Class<T> type)
    {
        checkForLock();

        if (isNullValue()) return null;
        T[] array = (T[]) Array.newInstance(type, size());
        toArray(array);
        clear();
        return array;
    }

    /**
     * SubListView throws a {@link ConcurrentModificationException} if the parent list has any
     * structural modifications, i.e. any operation that cause its size to change. To determine if
     * a parent structural modification happened it keeps track of the structural modification count
     * to compare against the parent
     *
     * Structural modifications from the sublist itself are allowed.
     */
    private class SubListView extends AbstractList<IonValue> {

        /**
         * index in top level IonSequenceLite that marks the start of this sublist view. For nested
         * sublists fromIndex will be in relation to the root parent which must be a IonSequenceLite
         */
        private final int fromIndex;
        private int size;

        private SubListView(final int fromIndex, final int toIndex) {
            this.fromIndex = fromIndex;
            this.size = toIndex - fromIndex;
            super.modCount = IonSequenceLite.this.structuralModificationCount;
        }

        @Override
        public int size() {
            checkForParentModification();
            return size;
        }

        @Override
        public boolean isEmpty() {
            checkForParentModification();
            return size == 0;
        }

        @Override
        public IonValue get(final int index) {
            checkForParentModification();
            rangeCheck(index);

            return IonSequenceLite.this.get(toParentIndex(index));
        }

        @Override
        public IonValue set(final int index, final IonValue element) {
            checkForParentModification();
            rangeCheck(index);

            return IonSequenceLite.this.set(toParentIndex(index), element);
        }

        @Override
        public boolean contains(final Object o) {
            checkForParentModification();
            return super.contains(o);
        }

        @Override
        public boolean containsAll(final Collection<?> collection) {
            checkForParentModification();
            return super.containsAll(collection);
        }

        @Override
        public Object[] toArray() {
            checkForParentModification();
            return super.toArray();
        }

        @Override
        public <T> T[] toArray(T[] ts) {
            checkForParentModification();
            return super.toArray(ts);
        }

        @Override
        public boolean add(final IonValue ionValue) {
            checkForParentModification();

            int parentIndex = toParentIndex(size);

            // adds at end of parent list
            if (parentIndex == IonSequenceLite.this.size()) {
                IonSequenceLite.this.add(ionValue);
            } else {
                IonSequenceLite.this.add(parentIndex, ionValue);
            }

            super.modCount = IonSequenceLite.this.structuralModificationCount;
            size++;

            return true;
        }

        @Override
        public void add(final int index, final IonValue ionValue) {
            checkForParentModification();
            rangeCheck(index);

            IonSequenceLite.this.add(toParentIndex(index), ionValue);

            super.modCount = IonSequenceLite.this.structuralModificationCount;
            size++;
        }


        @Override
        public boolean addAll(int i, Collection<? extends IonValue> collection) {
            checkForParentModification();
            return super.addAll(i, collection);
        }


        @Override
        public boolean addAll(Collection<? extends IonValue> collection) {
            checkForParentModification();
            return super.addAll(collection);
        }

        // retainAll has no functionality that is specific to SubListView but
        // needs to be implemented because the AbstractList<T>.retainAll() throws [UnsupportedOperationException].
        @Override
        public boolean retainAll(final Collection<?> c) {
            checkForParentModification();
            if (size < 1) {
                return false;
            }

            final Map<Object,Object> toRetain = new IdentityHashMap<Object,Object>();
            for (Object o : c) {
                toRetain.put(o, null);
            }

            final List<IonValue> toRemove = new ArrayList<IonValue>(size - c.size());
            for (int i = 0; i < size; i++) {
                final IonValue ionValue = get(i);

                if (!toRetain.containsKey(ionValue)) {
                    toRemove.add(ionValue);
                }
            }

            return removeAll(toRemove);
        }

        @Override
        public void clear() {
            checkForParentModification();

            // remove the first element size times to remove all elements
            int parentIndex = toParentIndex(0);
            for (int i = 0; i < size; i++) {
                IonSequenceLite.this.remove(parentIndex);
            }

            size = 0;
            super.modCount = IonSequenceLite.this.structuralModificationCount;
        }

        @Override
        public IonValue remove(final int index) {
            checkForParentModification();
            rangeCheck(index);

            final IonValue removed = IonSequenceLite.this.remove(toParentIndex(index));

            super.modCount = IonSequenceLite.this.structuralModificationCount;
            size--;

            return removed;
        }

        // remove(Object) contains no functionality that is specific to SubListView but
        // needs to be implemented because the AbstractList<T>.remove(Object) throws [UnsupportedOperationException].
        @Override
        public boolean remove(final Object o) {
            checkForParentModification();
            int index = indexOf(o);
            if (index < 0) {
                return false;
            }

            remove(index);
            return true;
        }

        // removeAll(Collection<?>) contains no functionality that is specific to SubListView but
        // needs to be implemented because the AbstractList<T>.remove(Object) throws [UnsupportedOperationException].
        @Override
        public boolean removeAll(final Collection<?> c) {
            checkForParentModification();
            boolean changed = false;
            for (Object o : c) {
                if (remove(o)) {
                    changed = true;
                }
            }

            return changed;
        }

        @Override
        public int indexOf(final Object o) {
            checkForParentModification();
            return super.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            checkForParentModification();
            return super.lastIndexOf(o);
        }

        @Override
        protected void removeRange(int from, int to) {
            checkForParentModification();
            super.removeRange(from, to);
        }


        @Override
        public Iterator<IonValue> iterator() {
            return listIterator(0);
        }

        @Override
        public ListIterator<IonValue> listIterator() {
            return listIterator(0);
        }

        @Override
        public ListIterator<IonValue> listIterator(final int index) {
            checkForParentModification();

            return new ListIterator<IonValue>() {
                private int lastReturnedIndex = index;
                private int nextIndex = index;

                public boolean hasNext() {
                    return nextIndex < SubListView.this.size();
                }

                public IonValue next() {
                    lastReturnedIndex = nextIndex++;
                    return SubListView.this.get(lastReturnedIndex);
                }

                public boolean hasPrevious() {
                    return nextIndex > 0;
                }

                public IonValue previous() {
                    lastReturnedIndex = --nextIndex;
                    return SubListView.this.get(lastReturnedIndex);
                }

                public int nextIndex() {
                    return nextIndex;
                }

                public int previousIndex() {
                    return nextIndex - 1;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }

                public void set(final IonValue ionValue) {
                    SubListView.this.set(lastReturnedIndex, ionValue);
                }

                public void add(final IonValue ionValue) {
                    SubListView.this.add(lastReturnedIndex, ionValue);
                }
            };
        }

        @Override
        public List<IonValue> subList(final int fromIndex, final int toIndex) {
            checkSublistParameters(this.size(), fromIndex, toIndex);
            checkForParentModification();
            return new SubListView(toParentIndex(fromIndex), toParentIndex(toIndex));
        }

        @Override
        public boolean equals(Object o) {
            checkForParentModification();
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            checkForParentModification();
            return super.hashCode();
        }

        @Override
        public String toString() {
            checkForParentModification();
            return super.toString();
        }

        private void rangeCheck(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException(String.valueOf(index));
            }
        }

        private int toParentIndex(int index) {
            return index + fromIndex;
        }

        private void checkForParentModification() {
            if (super.modCount != IonSequenceLite.this.structuralModificationCount) {
                throw new ConcurrentModificationException();
            }
        }
    }
}
