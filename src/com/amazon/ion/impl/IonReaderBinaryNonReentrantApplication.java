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
        super(new IonReaderBinaryIncrementalArbitraryDepth(builder, inputStream));
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
        //throw new UnsupportedOperationException("Method requires an system-level reader.");
        return -1; // TODO
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
