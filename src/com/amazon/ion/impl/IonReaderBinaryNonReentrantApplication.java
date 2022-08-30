package com.amazon.ion.impl;

import com.amazon.ion.IonReaderReentrantApplication;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.InputStream;
import java.util.Iterator;

public class IonReaderBinaryNonReentrantApplication
    extends IonReaderBinaryNonReentrantCore<IonReaderReentrantApplication> {

    IonReaderBinaryNonReentrantApplication(IonReaderBuilder builder, InputStream inputStream) {
        super(
            new IonReaderBinaryIncrementalArbitraryDepth(
                builder,
                new RefillableBufferFromInputStream(inputStream, builder.getBufferConfiguration())
            )
        );
    }

    IonReaderBinaryNonReentrantApplication(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(
            new IonReaderBinaryIncrementalArbitraryDepth(
                builder,
                new FixedBufferFromByteArray(data, offset, length)
            )
        );
    }

    @Override
    public SymbolTable getSymbolTable() {
        return reader.getSymbolTable();
    }

    @Override
    public String[] getTypeAnnotations() {
        return reader.getTypeAnnotations();
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return reader.getTypeAnnotationSymbols();
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        return reader.iterateTypeAnnotations();
    }

    @Override
    public int getFieldId() {
        return reader.getFieldId();
    }

    @Override
    public String getFieldName() {
        return reader.getFieldName();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return reader.getFieldNameSymbol();
    }

    @Override
    public SymbolToken symbolValue() {
        prepareScalar();
        return reader.symbolValue();
    }
}
