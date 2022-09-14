package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.io.InputStream;

import static com.amazon.ion.util.IonStreamUtils.throwAsIonException;

public final class IonReaderBinaryIncrementalTopLevel extends IonReaderBinaryIncrementalArbitraryDepth implements IonReader, _Private_ReaderWriter {

    private final boolean isFixed;
    private boolean isLoadingValue = false;
    private IonType type = null; // TODO see if it's possible to remove this

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, InputStream inputStream) {
        super(
            builder,
            new IonBinaryLexerRefillableFromInputStream(inputStream, builder.getBufferConfiguration())
        );
        isFixed = false;
    }

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(
            builder,
            new IonBinaryLexerFromByteArray(builder.getBufferConfiguration(), data, offset, length)
        );
        isFixed = true;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    private IonCursor.Event nextValueHelper() {
        IonCursor.Event event = null;
        try {
            event = super.nextValue();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    private IonCursor.Event fillValueHelper() {
        IonCursor.Event event = null;
        try {
            event = super.fillValue();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    @Override
    public IonType next() {
        if (isFixed || !isTopLevel()) {
            IonCursor.Event event = nextValueHelper();
            if (event == IonCursor.Event.NEEDS_DATA) {
                type = null;
                return null;
            }
        } else {
            while (true) {
                IonCursor.Event event = isLoadingValue ? fillValueHelper() : nextValueHelper();
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                }
                isLoadingValue = true;
                event = fillValueHelper(); // TODO this is called twice if re-entering with LOAD_VALUE as the next instruction
                if (event == IonCursor.Event.NEEDS_DATA) {
                    type = null;
                    return null;
                } else if (event == IonCursor.Event.NEEDS_INSTRUCTION) {
                    // The value was skipped for being too large. Get the next one.
                    isLoadingValue = false;
                    continue;
                }
                break;
            }
            isLoadingValue = false;
        }
        type = super.getType();
        return type;
    }

    @Override
    public void stepIn() {
        try {
            super.stepIntoContainer();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        type = null;
    }

    @Override
    public void stepOut() {
        try {
            super.stepOutOfContainer();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        type = null;
    }

    @Override
    public IonType getType() {
        return type;
        //return reader.getType();
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null;
    }

    @Override
    public void close() throws IOException {
        requireCompleteValue();
        super.close();
    }
}
