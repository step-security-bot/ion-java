package com.amazon.ion.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

class FixedBufferFromByteArray extends AbstractBuffer {

    private final byte[] buffer;

    FixedBufferFromByteArray(byte[] buffer, int offset, int length) {
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
    protected boolean carefulFillAt(long index, long numberOfBytes) {
        if (numberOfBytes > availableAt(index)) {
            // TODO? throw or notify user?
            state = State.FILL;
            return false;
        }
        state = State.READY;
        return true;
    }

    @Override
    protected boolean seek(long numberOfBytes) {
        if (numberOfBytes > available()) {
            offset = limit;
            state = State.SEEK;
            return false;
        }
        offset += numberOfBytes;
        state = State.READY;
        return true;
    }

    @Override
    public void close() throws IOException {
        // Nothing to do.
    }
}
