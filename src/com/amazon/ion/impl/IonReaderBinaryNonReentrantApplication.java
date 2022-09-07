package com.amazon.ion.impl;

import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.InputStream;
import java.util.Iterator;

final class IonReaderBinaryNonReentrantApplication
    extends IonReaderBinaryNonReentrantCore implements _Private_ReaderWriter {

    private final IonReaderBinaryIncrementalArbitraryDepth application;

    IonReaderBinaryNonReentrantApplication(IonReaderBuilder builder, InputStream inputStream) {
        super(
            new IonReaderBinaryIncrementalArbitraryDepth(
                builder,
                new RefillableBufferFromInputStream(inputStream, builder.getBufferConfiguration())
            )
        );
        application = (IonReaderBinaryIncrementalArbitraryDepth) reader;
    }

    IonReaderBinaryNonReentrantApplication(IonReaderBuilder builder, byte[] data, int offset, int length) {
        super(
            new IonReaderBinaryIncrementalArbitraryDepth(
                builder,
                new FixedBufferFromByteArray(data, offset, length)
            )
        );
        application = (IonReaderBinaryIncrementalArbitraryDepth) reader;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return application.getSymbolTable();
    }

    @Override
    public String[] getTypeAnnotations() {
        return application.getTypeAnnotations();
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        return application.getTypeAnnotationSymbols();
    }

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        return application.iterateTypeAnnotations();
    }

    @Override
    public int getFieldId() {
        return application.getFieldId();
    }

    @Override
    public String getFieldName() {
        return application.getFieldName();
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        return application.getFieldNameSymbol();
    }

    @Override
    public SymbolToken symbolValue() {
        prepareScalar();
        return application.symbolValue();
    }

    @Override
    public SymbolTable pop_passed_symbol_table() {
        return application.pop_passed_symbol_table();
    }
}
