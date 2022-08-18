package com.amazon.ion.impl;

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
    int peek(int index) {
        return buffer[index] & SINGLE_BYTE_MASK;
    }

    @Override
    void copyBytes(int position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, position, destination, destinationOffset, length);
    }

    @Override
    boolean fillAt(int index, int numberOfBytes) {
        if (numberOfBytes > availableAt(index)) {
            // TODO? throw or notify user?
            state = State.FILL;
            return false;
        }
        state = State.READY;
        return true;
    }

    @Override
    boolean seek(int numberOfBytes) {
        if (numberOfBytes < available()) {
            offset = limit;
            state = State.SEEK;
            return false;
        }
        offset += numberOfBytes;
        state = State.READY;
        return true;
    }
}
