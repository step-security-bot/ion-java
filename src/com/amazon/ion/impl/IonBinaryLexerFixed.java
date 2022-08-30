package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;

public class IonBinaryLexerFixed extends IonBinaryLexerBase<FixedBufferFromByteArray> {

    IonBinaryLexerFixed(
        BufferConfiguration.DataHandler dataHandler,
        IvmNotificationConsumer ivmConsumer,
        FixedBufferFromByteArray buffer
    ) {
        super(buffer, dataHandler, ivmConsumer);
    }

    @Override
    protected int readByte() {
        if (peekIndex >= buffer.limit) { // TODO any way to avoid?
            return -1;
        }
        return buffer.peek(peekIndex++);
    }
}
