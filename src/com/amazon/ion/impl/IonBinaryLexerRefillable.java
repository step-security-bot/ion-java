package com.amazon.ion.impl;

import static com.amazon.ion.IonCursor.Event;

import com.amazon.ion.BufferConfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class IonBinaryLexerRefillable extends IonBinaryLexerBase {

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

        void bufferOverflowDetected();
    }

    // TODO should this be an IonBufferConfiguration?
    private final BufferConfiguration<?> configuration;

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

    private BufferConfiguration.OversizedValueHandler oversizedValueHandler;

    /**
     * The number of bytes to attempt to buffer each time more bytes are required.
     */
    private final int pageSize;

    private boolean isSkippingCurrentValue = false;

    private int individualBytesSkippedWithoutBuffering = 0;

    IonBinaryLexerRefillable(final BufferConfiguration<?> configuration) {
        super(0, false, configuration.getDataHandler());
        if (configuration.getInitialBufferSize() < 1) {
            throw new IllegalArgumentException("Initial buffer size must be at least 1.");
        }
        if (configuration.getMaximumBufferSize() < configuration.getInitialBufferSize()) {
            throw new IllegalArgumentException("Maximum buffer size cannot be less than the initial buffer size.");
        }
        this.configuration = configuration;
        this.initialBufferSize = configuration.getInitialBufferSize();
        this.maximumBufferSize = configuration.getMaximumBufferSize();
        this.capacity = initialBufferSize;
        buffer = new byte[initialBufferSize];
        byteBuffer = ByteBuffer.wrap(buffer, 0, initialBufferSize);
        notificationConsumer = new NotificationConsumer() {
            @Override
            public void bytesConsolidatedToStartOfBuffer(int leftShiftAmount) {
                // The existing data in the buffer has been shifted to the start. Adjust the saved indexes
                // accordingly. -1 indicates that all indices starting at 0 will be shifted.
                shiftIndicesLeft(-1, leftShiftAmount);
            }

            @Override
            public void bufferOverflowDetected() {
                isSkippingCurrentValue = true;
            }
        };
        pageSize = configuration.getInitialBufferSize();
    }

    abstract void refill(long numberOfBytesToFill) throws IOException;

    abstract int readByteWithoutBuffering() throws IOException;

    // TODO is it possible to just put this in the constructor?
    void registerOversizedValueHandler(BufferConfiguration.OversizedValueHandler oversizedValueHandler) {
        this.oversizedValueHandler = oversizedValueHandler;
    }

    private void handleOversizedValue() throws IOException {
        // The value was oversized.
        oversizedValueHandler.onOversizedValue();
        if (!isTerminated()) { // TODO note: not required
            // TODO reuse setCheckpoint
            seek(valueMarker.endIndex - offset - individualBytesSkippedWithoutBuffering);
            reportConsumedData(valueMarker.endIndex - checkpoint);
            checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;
            peekIndex = offset;
            checkpoint = peekIndex;
            reset();
        }
        isSkippingCurrentValue = false;
        individualBytesSkippedWithoutBuffering = 0;
    }

    @Override
    public Event next() throws IOException {
        while (true) {
            Event event = super.next();
            if (isSkippingCurrentValue) {
                handleOversizedValue();
                // The user has requested a value. Continue to the next one.
                continue;
                // This was a request to fill a value, but the request could not be completed because the value
                // was oversized. Convey that NEXT is now required.
            }
            return event;
        }
    }

    @Override
    public Event fillValue() throws IOException {
        Event event = super.fillValue();
        if (isSkippingCurrentValue) {
            handleOversizedValue();
        }
        return event;
    }

    @Override
    int peek(long index) {
        return buffer[(int) index] & SINGLE_BYTE_MASK;
    }

    @Override
    void copyBytes(long position, byte[] destination, int destinationOffset, int length) {
        System.arraycopy(buffer, (int) position, destination, destinationOffset, length);
    }

    /**
     * Ensures that there is space for at least 'minimumNumberOfBytesRequired' additional bytes in the buffer,
     * growing the buffer if necessary. May consolidate buffered bytes, performing an in-order copy and resetting
     * indices such that the `readIndex` points to the same byte and the `writeIndex` is positioned after the last
     * byte that is available to read.
     * @param minimumNumberOfBytesRequired the minimum number of additional bytes to buffer.
     * @return true if the buffer has sufficient capacity; otherwise, false.
     */
    boolean ensureCapacity(long minimumNumberOfBytesRequired) {
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
    protected boolean carefulFillAt(long index, long numberOfBytes) throws IOException {
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

    private interface ReadByteFunction {
        int readByte() throws IOException;
    }

    private final ReadByteFunction carefulReadByteFunction = new ReadByteFunction() {
        @Override
        public int readByte() throws IOException {
            return carefulReadByte();
        }
    };

    private final ReadByteFunction quickReadByteFunction = new ReadByteFunction() {
        @Override
        public int readByte() throws IOException {
            return peekByte();
        }
    };

    private ReadByteFunction currentReadByteFunction = carefulReadByteFunction;

    @Override
    protected int peekByte() throws IOException {
        int b;
        if (isSkippingCurrentValue) {
            b = readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            b = peek(peekIndex);
            //pipe.extendBoundary(1);
            peekIndex++;
        }
        return b;
    }


    protected int carefulReadByte() throws IOException {
        int b;
        if (isSkippingCurrentValue) {
            // If the value is being skipped, the byte will not have been buffered.
            //b = getInput().read();
            b = readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            if (!fillAt(peekIndex, 1)) {
                return -1;
            }
            // TODO ugly
            b = peekByte();
        }
        return b;
    }

    /**
     * Reads one byte, if possible.
     * @return the byte, or -1 if none was available.
     * @throws IOException if an IOException is thrown by the underlying InputStream.
     */
    @Override
    protected int readByte() throws IOException {
        return currentReadByteFunction.readByte();
    }

    @Override
    protected void setValueMarker(long valueLength, boolean isAnnotated) {
        if (isSkippingCurrentValue) {
            // If the value is being skipped, not all of its bytes will be buffered, so start/end indexes will not
            // align to the expected values. This is fine, because the value will not be accessed.
            return;
        }
        super.setValueMarker(valueLength, isAnnotated);
    }

    @Override
    protected Event handleFill() {
        if (isSkippingCurrentValue) {
            return Event.NEEDS_INSTRUCTION;
        }
        return super.handleFill();
    }

    protected void enterQuickMode() {
        super.enterQuickMode();
        currentReadByteFunction = quickReadByteFunction;
    }

    protected void exitQuickMode() {
        super.exitQuickMode();
        currentReadByteFunction = carefulReadByteFunction;
    }

    private long amountToShift = 0;

    private final _Private_RecyclingStack.Consumer<ContainerInfo> shiftContainerIndex =
        new _Private_RecyclingStack.Consumer<ContainerInfo>() {

        @Override
        public void accept(ContainerInfo element) {
            element.endIndex -= amountToShift;
        }
    };

    private void shiftContainerEnds(long shiftAmount) {
        amountToShift = shiftAmount; // TODO ugly
        containerStack.forEach(shiftContainerIndex);
        amountToShift = 0;
    }

    @Override
    protected void handleSkip() {
        if (peekIndex < valueMarker.endIndex) {
            shiftContainerEnds(valueMarker.endIndex - peekIndex);
        }
    }

    /**
     * Shift all indices after 'afterIndex' left by the given amount. This is used when data is moved in the underlying
     * buffer either due to buffer growth or NOP padding being reclaimed to make room for a value that would otherwise
     * exceed the buffer's maximum size.
     * @param afterIndex all indices after this index will be shifted (-1 indicates that all indices should be shifted).
     * @param shiftAmount the amount to shift left.
     */
    private void shiftIndicesLeft(int afterIndex, int shiftAmount) {
        peekIndex = Math.max(peekIndex - shiftAmount, 0);
        valueMarker.startIndex -= shiftAmount;
        valueMarker.endIndex -= shiftAmount;
        checkpoint -= shiftAmount;
        if (annotationSidsMarker.startIndex > afterIndex) {
            annotationSidsMarker.startIndex -= shiftAmount;
            annotationSidsMarker.endIndex -= shiftAmount;
        }
        shiftContainerEnds(shiftAmount);
    }

    BufferConfiguration<?> getConfiguration() {
        return configuration;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
