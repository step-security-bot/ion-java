package com.amazon.ion.impl;

/**
 * Interface to be implemented by all incremental IonReaders. See
 * {@link com.amazon.ion.system.IonReaderBuilder#withIncrementalReadingEnabled(boolean)}.
 */
public interface _Private_IncrementalReader { // TODO merge with IonReaderReentrantCore? Or IonCursor?

    /**
     * Requires that the reader not currently be buffering an incomplete value.
     * @throws com.amazon.ion.IonException if the reader is buffering an incomplete value.
     */
    void requireCompleteValue();
}
