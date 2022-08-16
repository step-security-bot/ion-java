package com.amazon.ion.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class AbstractBuffer {

    protected enum Instruction {
        FILL,
        SEEK,
        READY
    }

    /**
     * Mask to isolate a single byte.
     */
    static final int SINGLE_BYTE_MASK = 0xFF;

    /**
     * The index of the next byte in the buffer that is available to be read. Always less than or equal to `limit`.
     */
    int offset = 0;

    /**
     * The index at which the next byte received will be written. Always greater than or equal to `offset`.
     */
    int limit = 0;

    int capacity;

    ByteBuffer byteBuffer;

    Instruction instruction = Instruction.READY;

    int bytesRequested = 0;

    abstract int peek(int index);

    // TODO abstraction?
    ByteBuffer getByteBuffer(int startIndex, int endIndex) {
        // Setting the limit to the capacity first is required because setting the position will fail if the new
        // position is outside the limit.
        byteBuffer.limit(capacity);
        byteBuffer.position(startIndex);
        byteBuffer.limit(endIndex);
        return byteBuffer;
    }

    int available() {
        return availableAt(offset);
    }

    int availableAt(int index) {
        return limit - index;
    }

    abstract void copyBytes(int position, byte[] destination, int destinationOffset, int length);

    boolean fill(int numberOfBytes) throws Exception {
        return fillAt(offset, numberOfBytes);
    }

    abstract boolean fillAt(int index, int numberOfBytes) throws Exception;

    abstract boolean seek(int numberOfBytes) throws IOException;

    boolean seekTo(int index) throws IOException {
        return seek(index - offset);
    }

    boolean makeReady() throws Exception {
        switch (instruction) {
            case READY:
                return true;
            case SEEK:
                return seek(bytesRequested);
            case FILL:
                return fill(bytesRequested);
            default:
                throw new IllegalStateException();
        }
    }

    int getOffset() {
        return offset;
    }

    boolean isReady() {
        return instruction == Instruction.READY;
    }

    boolean isAwaitingMoreData() {
        // TODO doesn't feel quite right
        return instruction == Instruction.SEEK;
    }

}
