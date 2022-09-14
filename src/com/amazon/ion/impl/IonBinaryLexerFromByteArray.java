package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO try making LexerRefillable extend this? Performance experiment.
class IonBinaryLexerFromByteArray extends IonBinaryLexerBase {

    protected byte[] buffer;

    IonBinaryLexerFromByteArray(BufferConfiguration<?> configuration, byte[] buffer, int offset, int length) {
        super(offset, configuration == null ? null : configuration.getDataHandler());
        this.buffer = buffer;
        this.offset = offset;
        this.limit = offset + length;
        this.capacity = limit;
        byteBuffer = ByteBuffer.wrap(buffer, offset, length);
    }

    @Override
    int peek(long index) {
        return buffer[(int) index] & SINGLE_BYTE_MASK;
    }

    @Override
    void copyBytes(long position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, (int) position, destination, destinationOffset, length);
    }

    @Override
    public void close() throws IOException {
        // Nothing to do.
    }
}
