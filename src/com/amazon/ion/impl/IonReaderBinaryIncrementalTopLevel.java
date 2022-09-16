package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static com.amazon.ion.util.IonStreamUtils.throwAsIonException;

public final class IonReaderBinaryIncrementalTopLevel extends IonReaderBinaryIncrementalArbitraryDepth implements IonReader, _Private_ReaderWriter {

    private final boolean isFixed;
    private final boolean isNonReentrant;
    private boolean isLoadingValue = false;
    private IonType type = null; // TODO see if it's possible to remove this

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, InputStream inputStream) {
        super(
            builder,
            new IonBinaryLexerRefillableFromInputStream(inputStream, builder.getBufferConfiguration())
        );
        isFixed = false;
        isNonReentrant = !builder.isIncrementalReadingEnabled();
    }

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(
            builder,
            new IonBinaryLexerBase(builder.getBufferConfiguration(), data, offset, length)
        );
        isFixed = true;
        // TODO could just share the same non-reentrant unexpected EOF behavior since the buffer is fixed.
        isNonReentrant = !builder.isIncrementalReadingEnabled();
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
        if (isFixed || isNonReentrant || !isTopLevel()) {
            IonCursor.Event event = nextValueHelper();
            if (event == IonCursor.Event.NEEDS_DATA) {
                if (isNonReentrant) {
                    requireCompleteValue();
                }
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

    private void prepareScalar() {
        if (isFixed || !isNonReentrant || getCurrentEvent() == IonCursor.Event.VALUE_READY) {
            return;
        }
        if (getCurrentEvent() != IonCursor.Event.START_SCALAR) {
            // Note: existing tests expect IllegalStateException in this case.
            throw new IllegalStateException("Reader is not positioned on a scalar value.");
        }
        IonCursor.Event event = null;
        try {
            event = fillValue();
        } catch (IOException e) {
            throwAsIonException(e);
        }
        if (event != IonCursor.Event.VALUE_READY) {
            throw new IonException("Unexpected EOF.");
        }
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (getType() != IonType.INT) {
            return null;
        }
        prepareScalar();
        return super.getIntegerSize();
    }

    @Override
    public boolean isNullValue() {
        // TODO the type == NULL check should not be necessary and indicates a problem lower down. Fix.
        return type == IonType.NULL || super.isNullValue();
    }

    @Override
    public boolean booleanValue() {
        prepareScalar();
        return super.booleanValue();
    }

    @Override
    public int intValue() {
        prepareScalar();
        return super.intValue();
    }

    @Override
    public long longValue() {
        prepareScalar();
        return super.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        prepareScalar();
        return super.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        prepareScalar();
        return super.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        prepareScalar();
        return super.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        prepareScalar();
        return super.decimalValue();
    }

    @Override
    public Date dateValue() {
        prepareScalar();
        return super.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        prepareScalar();
        return super.timestampValue();
    }

    @Override
    public String stringValue() {
        prepareScalar();
        return super.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        prepareScalar();
        return super.symbolValue();
    }

    @Override
    public int byteSize() {
        prepareScalar(); // TODO is it possible/necessary to try to avoid this?
        return super.byteSize();
    }

    @Override
    public byte[] newBytes() {
        prepareScalar();
        return super.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        prepareScalar();
        return super.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!isNonReentrant) {
            requireCompleteValue();
        }
        super.close();
    }
}
