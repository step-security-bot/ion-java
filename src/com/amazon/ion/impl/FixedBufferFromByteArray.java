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
    boolean fill(int numberOfBytes) {
        if (numberOfBytes > available()) {
            // TODO? throw or notify user?
            instruction = Instruction.FILL;
            return false;
        }
        instruction = Instruction.READY;
        return true;
    }

    @Override
    boolean seek(int numberOfBytes) {
        if (numberOfBytes < available()) {
            offset = limit;
            instruction = Instruction.SEEK;
            return false;
        }
        offset += numberOfBytes;
        instruction = Instruction.READY;
        return true;
    }
}
