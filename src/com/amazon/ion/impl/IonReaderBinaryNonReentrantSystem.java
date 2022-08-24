package com.amazon.ion.impl;

import com.amazon.ion.IonReaderReentrantSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;

import java.util.Iterator;

public class IonReaderBinaryNonReentrantSystem extends IonReaderBinaryNonReentrantCore<IonReaderReentrantSystem> {

    IonReaderBinaryNonReentrantSystem(IonReaderReentrantSystem reader) {
        super(reader);
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
}
