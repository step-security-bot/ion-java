package com.amazon.ion;

public interface IonReaderReentrantSystem extends IonReaderReentrantCore {

    /**
     * Gets the symbol ID of the field name attached to the current value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return the symbol ID of the field name, if the current value is a
     * field within a struct.
     * If the current value is not a field, or if the symbol ID cannot be
     * determined, this method returns a value <em>less than one</em>.
     *
     */
    @Deprecated
    int getFieldId();
}
