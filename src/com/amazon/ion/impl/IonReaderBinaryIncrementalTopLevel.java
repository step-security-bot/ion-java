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
    private final boolean isLoadRequired;
    private boolean isLoadingValue = false;
    private IonType type = null; // TODO see if it's possible to remove this

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, InputStream inputStream) {
        super(builder, inputStream);
        isFixed = false;
        isNonReentrant = !builder.isIncrementalReadingEnabled();
        isLoadRequired = isNonReentrant;
    }

    IonReaderBinaryIncrementalTopLevel(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(builder, data, offset, length);
        isFixed = true;
        // TODO could just share the same non-reentrant unexpected EOF behavior since the buffer is fixed.
        isNonReentrant = !builder.isIncrementalReadingEnabled();
        isLoadRequired = false;
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
        if (isFixed || isNonReentrant || parent != null) {
            IonCursor.Event event = Event.NEEDS_DATA;
            try {
                event = super.nextValue();
            } catch (IOException e) {
                throwAsIonException(e);
            }
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
    }

    private void prepareScalar() {
        if (getCurrentEvent() == IonCursor.Event.VALUE_READY) {
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
        if (type != IonType.INT) {
            return null;
        }
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.getIntegerSize();
    }

    @Override
    public boolean booleanValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.booleanValue();
    }

    @Override
    public int intValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.intValue();
    }

    @Override
    public long longValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.decimalValue();
    }

    @Override
    public Date dateValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.timestampValue();
    }

    @Override
    public String stringValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.stringValue();
    }

    @Override
    public SymbolToken symbolValue() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.symbolValue();
    }

    @Override
    public int byteSize() {
        if (isLoadRequired) {
            prepareScalar(); // TODO is it possible/necessary to try to avoid this?
        }
        return super.byteSize();
    }

    @Override
    public byte[] newBytes() {
        if (isLoadRequired) {
            prepareScalar();
        }
        return super.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        if (isLoadRequired) {
            prepareScalar();
        }
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
