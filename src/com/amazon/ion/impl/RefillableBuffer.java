package com.amazon.ion.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class RefillableBuffer extends AbstractBuffer {
    /**
     * Handler of notifications provided by the ResizingPipedInputStream.
     */
    interface NotificationConsumer {

        /**
         * Bytes have been shifted to the start of the buffer in order to make room for additional bytes
         * to be buffered.
         * @param leftShiftAmount the amount of the left shift (also: the pre-shift read index of the first shifted
         *                        byte).
         */
        void bytesConsolidatedToStartOfBuffer(int leftShiftAmount);

        void bufferOverflowDetected() throws Exception; // TODO see about removing 'throws Exception'
    }

    /**
     * A NotificationConsumer that does nothing.
     */
    private static final NotificationConsumer NO_OP_NOTIFICATION_CONSUMER = new NotificationConsumer() {
        @Override
        public void bytesConsolidatedToStartOfBuffer(int leftShiftAmount) {
            // Do nothing.
        }

        @Override
        public void bufferOverflowDetected() {
            // Do nothing.
        }
    };

    /**
     * The initial size of the buffer and the number of bytes by which the size of the buffer will increase
     * each time it grows, unless it must grow by a smaller amount to fit within 'maximumBufferSize'.
     */
    protected final int initialBufferSize;

    /**
     * The maximum size of the buffer. If the user attempts to buffer more bytes than this, an exception will be raised.
     */
    protected final int maximumBufferSize;


    protected NotificationConsumer notificationConsumer;

    protected byte[] buffer;

    RefillableBuffer(final int initialBufferSize, final int maximumBufferSize) {
        if (initialBufferSize < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (maximumBufferSize < initialBufferSize) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }

        buffer = new byte[initialBufferSize];
        byteBuffer = ByteBuffer.wrap(buffer, 0, initialBufferSize);
        this.initialBufferSize = initialBufferSize;
        this.maximumBufferSize = maximumBufferSize;
        this.capacity = initialBufferSize;
    }

    void registerNotificationConsumer(NotificationConsumer notificationConsumer) {
        this.notificationConsumer = notificationConsumer;
    }

    abstract void refill(long numberOfBytesToFill) throws IOException;

    abstract int readByteWithoutBuffering() throws IOException;

    @Override
    int peek(long index) {
        return buffer[(int) index] & SINGLE_BYTE_MASK;
    }

    @Override
    void copyBytes(long position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, (int) position, destination, destinationOffset, length);
    }

    /*
    void truncate(int index, int size) {
        limit = index;
        //available = previousAvailable;
        //boundary = writeIndex;
        //this.size = size;
    }

     */

    /**
     * Moves all bytes starting at 'fromPosition' to 'toPosition', overwriting the bytes in-between. It is the caller's
     * responsibility to ensure that the overwritten bytes are not needed.
     * @param fromPosition the position to move bytes from. Must be less than or equal to 'writeIndex' and 'boundary'.
     * @param toPosition the position to move bytes to. Must be greater than or equal to 'readIndex'.
     */
    /*
    void consolidate(int fromPosition, int toPosition) {
        if (fromPosition > limit  || toPosition < offset) { //|| fromPosition > boundary
            throw new IllegalArgumentException("Tried to consolidate using an index that violates the constraints.");
        }
        int indexShift = fromPosition - toPosition;
        System.arraycopy(buffer, fromPosition, buffer, toPosition, limit - fromPosition);
        //size -= indexShift;
        //available -= indexShift;
        limit -= indexShift;
        //boundary -= indexShift;
        // readIndex does not need to change, because none of the consolidated bytes have been read yet.
    }
    */
    /**
     * Ensures that there is space for at least 'minimumNumberOfBytesRequired' additional bytes in the buffer,
     * growing the buffer if necessary. May consolidate buffered bytes, performing an in-order copy and resetting
     * indices such that the `readIndex` points to the same byte and the `writeIndex` is positioned after the last
     * byte that is available to read.
     * @param minimumNumberOfBytesRequired the minimum number of additional bytes to buffer.
     * @return true if the buffer has sufficient capacity; otherwise, false.
     */
    boolean ensureCapacity(long minimumNumberOfBytesRequired) throws Exception {
        if (freeSpaceAt(offset) >= minimumNumberOfBytesRequired) {
            // No need to shift any bytes or grow the buffer.
            return true;
        }
        if (minimumNumberOfBytesRequired > maximumBufferSize) {
            notificationConsumer.bufferOverflowDetected();
            return false;
        }
        long shortfall = minimumNumberOfBytesRequired - capacity;
        if (shortfall > 0) {
            // TODO consider doubling the size rather than growing in increments of the initial buffer size.
            // TODO ensure alignment to a power of 2?
            int newSize = (int) Math.min(capacity + Math.max(initialBufferSize, shortfall), maximumBufferSize);
            byte[] newBuffer = new byte[newSize];
            moveBytesToStartOfBuffer(newBuffer);
            capacity = newSize;
            buffer = newBuffer;
            byteBuffer = ByteBuffer.wrap(buffer, (int) offset, (int) capacity);
        } else {
            // The current capacity can accommodate the requested size; move the existing bytes to the beginning
            // to make room for the remaining requested bytes to be filled at the end.
            moveBytesToStartOfBuffer(buffer);
        }
        return true;
    }

    @Override
    boolean fillAt(long index, long numberOfBytes) throws Exception {
        long shortfall = numberOfBytes - availableAt(index);
        if (shortfall > 0) {
            bytesRequested = numberOfBytes + (index - offset);
            if (ensureCapacity(bytesRequested)) {
                // Fill all the free space, not just the shortfall; this reduces I/O.
                refill(freeSpaceAt(limit));
                shortfall = bytesRequested - available();
            } else {
                // The request cannot be satisfied, but not because data was unavailable. Return normally; it is the
                // caller's responsibility to recover.
                shortfall = 0;
            }
        }
        if (shortfall <= 0) {
            bytesRequested = 0;
            state = State.READY;
            return true;
        }
        //remainingBytesRequested = shortfall;
        state = State.FILL;
        return false;
    }

    /**
     * Moves all buffered (but not yet read) bytes from 'buffer' to the destination buffer. In total, `size`
     * bytes will be moved.
     * @param destinationBuffer the destination buffer, which may be 'buffer' itself or a new buffer.
     */
    private void moveBytesToStartOfBuffer(byte[] destinationBuffer) {
        long size = available();
        if (size > 0) {
            System.arraycopy(buffer, (int) offset, destinationBuffer, 0, (int) size);
        }
        if (offset > 0) {
            notificationConsumer.bytesConsolidatedToStartOfBuffer((int) offset);
        }
        offset = 0;
        //boundary = available;
        limit = size;
    }

    /**
     * @return the number of bytes that can be written at the end of the buffer.
     */
    private long freeSpaceAt(long index) {
        return capacity - index;
    }
}
