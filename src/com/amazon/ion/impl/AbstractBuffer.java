package com.amazon.ion.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class AbstractBuffer {

    protected enum State {
        FILL,
        SEEK,
        READY,
        TERMINATED
    }

    /**
     * Mask to isolate a single byte.
     */
    static final int SINGLE_BYTE_MASK = 0xFF;

    /**
     * The index of the next byte in the buffer that is available to be read. Always less than or equal to `limit`.
     */
    long offset = 0;

    /**
     * The index at which the next byte received will be written. Always greater than or equal to `offset`.
     */
    long limit = 0;

    long capacity;

    ByteBuffer byteBuffer;

    State state = State.READY;

    long bytesRequested = 0;

    abstract int peek(long index);

    // TODO abstraction?
    ByteBuffer getByteBuffer(long startIndex, long endIndex) {
        // Setting the limit to the capacity first is required because setting the position will fail if the new
        // position is outside the limit.
        byteBuffer.limit((int) capacity);
        byteBuffer.position((int) startIndex);
        byteBuffer.limit((int) endIndex);
        return byteBuffer;
    }

    long available() {
        return availableAt(offset);
    }

    long availableAt(long index) {
        return limit - index;
    }

    abstract void copyBytes(long position, byte[] destination, int destinationOffset, int length);

    boolean fill(long numberOfBytes) throws Exception {
        return fillAt(offset, numberOfBytes);
    }

    abstract boolean fillAt(long index, long numberOfBytes) throws Exception;

    abstract boolean seek(long numberOfBytes) throws IOException;

    boolean seekTo(long index) throws IOException {
        return seek(index - offset);
    }

    boolean makeReady() throws Exception {
        switch (state) {
            case READY:
                return true;
            case SEEK:
                return seek(bytesRequested);
            case FILL:
                return fill(bytesRequested);
            case TERMINATED:
                return false;
            default:
                throw new IllegalStateException();
        }
    }

    long getOffset() {
        return offset;
    }

    boolean isReady() {
        return state == State.READY;
    }

    boolean isAwaitingMoreData() {
        // TODO doesn't feel quite right
        return state == State.SEEK || bytesRequested > 1;
    }

    void terminate() {
        state = State.TERMINATED;
    }

    boolean isTerminated() {
        return state == State.TERMINATED;
    }

}
