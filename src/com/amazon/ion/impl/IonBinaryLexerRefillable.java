package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;

import java.io.IOException;
import java.io.InputStream;

public class IonBinaryLexerRefillable extends IonBinaryLexerBase<RefillableBuffer> {


    private final BufferConfiguration.OversizedValueHandler oversizedValueHandler;

    /**
     * The number of bytes to attempt to buffer each time more bytes are required.
     */
    private final int pageSize;

    private boolean isSkippingCurrentValue = false;

    IonBinaryLexerRefillable(
        final IonBufferConfiguration configuration,
        final BufferConfiguration.OversizedValueHandler oversizedValueHandler,
        final IvmNotificationConsumer ivmConsumer,
        final InputStream inputStream
    ) {
        //super(configuration, oversizedValueHandler, inputStream);
        super(
            new RefillableBufferFromInputStream(
                inputStream,
                configuration.getInitialBufferSize(),
                configuration.getMaximumBufferSize()
            ),
            configuration.getDataHandler(),
            ivmConsumer
        );
        buffer.registerNotificationConsumer(
            new RefillableBuffer.NotificationConsumer() {
                @Override
                public void bytesConsolidatedToStartOfBuffer(int leftShiftAmount) {
                    // The existing data in the buffer has been shifted to the start. Adjust the saved indexes
                    // accordingly. -1 indicates that all indices starting at 0 will be shifted.
                    shiftIndicesLeft(-1, leftShiftAmount);
                }

                @Override
                public void bufferOverflowDetected() throws Exception {
                    handleOversizedValue();
                }
            }
        );
        this.oversizedValueHandler = oversizedValueHandler;
        pageSize = configuration.getInitialBufferSize();
    }

    @Override
    protected void reset() {
        super.reset();
        isSkippingCurrentValue = false;
    }

    private void handleOversizedValue() throws Exception {
        oversizedValueHandler.onOversizedValue();
        // Reaching this point means the user wishes to skip the oversized value and continue.
        // Skip all bytes that have already been read.
        isSkippingCurrentValue = true;
        buffer.seekTo(peekIndex);
    }

    /**
     * Reads one byte, if possible.
     * @return the byte, or -1 if none was available.
     * @throws IOException if an IOException is thrown by the underlying InputStream.
     */
    @Override
    protected int readByte() throws Exception {
        int b;
        if (isSkippingCurrentValue) {
            // If the value is being skipped, the byte will not have been buffered.
            //b = getInput().read();
            b = buffer.readByteWithoutBuffering();
        } else {
            if (!buffer.fillAt(peekIndex, 1)) {
                return -1;
            }
            b = buffer.peek(peekIndex);
            //pipe.extendBoundary(1);
            peekIndex++;
        }
        return b;
    }

    protected boolean handleHeaderEnd() throws IOException {
        boolean isSkipping = isSkippingCurrentValue;
        if (isSkipping) {
            peekIndex = buffer.limit;
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
            buffer.seek(peekIndex); // TODO check
        }
        return isSkipping;
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
        /*
        if (ivmSecondByteIndex > afterIndex) {
            ivmSecondByteIndex -= shiftAmount;
        }

         */
    }


}
