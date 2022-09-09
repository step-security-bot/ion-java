package com.amazon.ion.impl;

import static com.amazon.ion.IonCursor.Event;

import com.amazon.ion.BufferConfiguration;

import java.io.IOException;

final class IonBinaryLexerRefillable extends IonBinaryLexerBase {


    private final BufferConfiguration.OversizedValueHandler oversizedValueHandler;

    /**
     * The number of bytes to attempt to buffer each time more bytes are required.
     */
    private final int pageSize;

    private boolean isSkippingCurrentValue = false;

    private int individualBytesSkippedWithoutBuffering = 0;

    IonBinaryLexerRefillable(
        final BufferConfiguration.OversizedValueHandler oversizedValueHandler,
        final IvmNotificationConsumer ivmConsumer,
        final RefillableBuffer buffer
    ) {
        super(buffer, buffer.getConfiguration().getDataHandler(), ivmConsumer);
        buffer.registerNotificationConsumer(
            new RefillableBuffer.NotificationConsumer() {
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
            }
        );
        this.oversizedValueHandler = oversizedValueHandler;
        pageSize = buffer.getConfiguration().getInitialBufferSize();
    }

    private void handleOversizedValue() throws IOException {
        // The value was oversized.
        oversizedValueHandler.onOversizedValue();
        if (!buffer.isTerminated()) { // TODO note: not required
            // TODO reuse setCheckpoint
            buffer.seek(valueMarker.endIndex - buffer.getOffset() - individualBytesSkippedWithoutBuffering);
            reportConsumedData(valueMarker.endIndex - checkpoint);
            checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;
            peekIndex = buffer.getOffset();
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
            b = ((RefillableBuffer) buffer).readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            b = buffer.peek(peekIndex);
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
            b = ((RefillableBuffer) buffer).readByteWithoutBuffering();
            if (b >= 0) {
                individualBytesSkippedWithoutBuffering += 1;
            }
        } else {
            if (!buffer.fillAt(peekIndex, 1)) {
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

    /**
     * Reclaim the NOP padding that occurred before the current value, making space for the value in the buffer.
     */
    /*
    private void reclaimNopPadding() {
        buffer.consolidate(valuePreHeaderIndex, nopPadStartIndex);
        shiftIndicesLeft(nopPadStartIndex, valuePreHeaderIndex - nopPadStartIndex);
        resetNopPadIndex();
    }

     */


    /*
    void truncateToEndOfPreviousValue() throws Exception { // TODO
        //additionalBytesNeeded -= pipe.availableBeyondBoundary();
        dataHandler.onData(pipe.availableBeyondBoundary());
        //peekIndex = valueStartWriteIndex;
        // TODO modify checkpoint
        // TODO truncate pipe
        // TODO mark notification to handler
        //pipe.truncate(valueStartWriteIndex, valueStartAvailable);
        //handlerNeedsToBeNotifiedOfOversizedValue = true;
    }

     */

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
        //valuePreHeaderIndex -= shiftAmount;
        //valuePostHeaderIndex -= shiftAmount;
        //valueEndIndex -= shiftAmount;
        //valueStartWriteIndex -= shiftAmount;

        valueMarker.startIndex -= shiftAmount;
        valueMarker.endIndex -= shiftAmount;

        /*
        for (Marker symbolTableMarker : symbolTableMarkers) {
            if (symbolTableMarker.startIndex > afterIndex) {
                symbolTableMarker.startIndex -= shiftAmount;
                symbolTableMarker.endIndex -= shiftAmount;
            }
        }

         */
        checkpoint -= shiftAmount;
        if (annotationSidsMarker.startIndex > afterIndex) {
            annotationSidsMarker.startIndex -= shiftAmount;
            annotationSidsMarker.endIndex -= shiftAmount;
        }

        shiftContainerEnds(shiftAmount);
        /*
        if (ivmSecondByteIndex > afterIndex) {
            ivmSecondByteIndex -= shiftAmount;
        }

         */
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }
}
