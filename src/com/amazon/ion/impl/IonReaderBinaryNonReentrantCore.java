package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonReaderReentrantCore;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.util.IonStreamUtils.throwAsIonException;

// TODO integrate with IonSystem.newSystemReader and tests
class IonReaderBinaryNonReentrantCore implements IonReader {

    protected final IonReaderReentrantCore reader;
    private IonType type = null; // TODO see if this can be removed by fixing type handling logic lower down

    IonReaderBinaryNonReentrantCore(IonReaderReentrantCore reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    private IonCursor.Event next(IonCursor.Instruction instruction) {
        IonCursor.Event event = null;
        try {
            event = reader.next(instruction);
        } catch (IOException e) {
            throwAsIonException(e);
        }
        return event;
    }

    @Override
    public IonType next() {
        if (NEEDS_DATA == next(IonCursor.Instruction.NEXT_VALUE)) {
            reader.requireCompleteValue();
            type = null;
        } else {
            type = reader.getType();
        }
        return type;
    }

    @Override
    public void stepIn() {
        next(IonCursor.Instruction.STEP_IN);
        type = null;
    }

    @Override
    public void stepOut() {
        next(IonCursor.Instruction.STEP_OUT);
        type = null;
    }

    @Override
    public int getDepth() {
        return reader.getDepth();
    }

    @Override
    public IonType getType() {
        return type;
    }

    protected void prepareScalar() {
        if (reader.getCurrentEvent() == IonCursor.Event.VALUE_READY) {
            return;
        }
        if (reader.getCurrentEvent() != IonCursor.Event.START_SCALAR) {
            // Note: existing tests expect IllegalStateException in this case.
            throw new IllegalStateException("Reader is not positioned on a scalar value.");
        }
        if (next(IonCursor.Instruction.LOAD_VALUE) != IonCursor.Event.VALUE_READY) {
            throw new IonException("Unexpected EOF.");
        }
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (getType() != IonType.INT) {
            return null;
        }
        prepareScalar();
        return reader.getIntegerSize();
    }

    @Override
    public boolean isNullValue() {
        // TODO the type == NULL check should not be necessary and indicates a problem lower down. Fix.
        return type == IonType.NULL || reader.isNullValue();
    }

    @Override
    public boolean isInStruct() {
        return reader.isInStruct();
    }

    @Override
    public boolean booleanValue() {
        prepareScalar();
        return reader.booleanValue();
    }

    @Override
    public int intValue() {
        prepareScalar();
        return reader.intValue();
    }

    @Override
    public long longValue() {
        prepareScalar();
        return reader.longValue();
    }

    @Override
    public BigInteger bigIntegerValue() {
        prepareScalar();
        return reader.bigIntegerValue();
    }

    @Override
    public double doubleValue() {
        prepareScalar();
        return reader.doubleValue();
    }

    @Override
    public BigDecimal bigDecimalValue() {
        prepareScalar();
        return reader.bigDecimalValue();
    }

    @Override
    public Decimal decimalValue() {
        prepareScalar();
        return reader.decimalValue();
    }

    @Override
    public Date dateValue() {
        prepareScalar();
        return reader.dateValue();
    }

    @Override
    public Timestamp timestampValue() {
        prepareScalar();
        return reader.timestampValue();
    }

    @Override
    public String stringValue() {
        prepareScalar();
        return reader.stringValue();
    }

    @Override
    public int byteSize() {
        prepareScalar(); // TODO is it possible/necessary to try to avoid this?
        return reader.byteSize();
    }

    @Override
    public byte[] newBytes() {
        prepareScalar();
        return reader.newBytes();
    }

    @Override
    public int getBytes(byte[] buffer, int offset, int len) {
        prepareScalar();
        return reader.getBytes(buffer, offset, len);
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null; // TODO
    }

    @Override
    public SymbolTable getSymbolTable() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public String[] getTypeAnnotations() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public int getFieldId() {
        return reader.getFieldId();
    }

    @Override
    public String getFieldName() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public SymbolToken symbolValue() {
        throw new UnsupportedOperationException("Method requires an application-level reader.");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
