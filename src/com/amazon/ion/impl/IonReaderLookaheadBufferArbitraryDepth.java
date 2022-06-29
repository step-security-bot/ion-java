package com.amazon.ion.impl;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReaderIncremental;
import com.amazon.ion.IonType;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Monitors an InputStream over binary Ion data to ensure enough data is available to be navigated successfully by a
 * non-incremental IonReader.
 * <p>
 * Error reporting: this wrapper reads the least amount of Ion data possible in order to determine whether a value
 * is complete. As such, it will not raise any errors if invalid data exists anywhere outside the header of a
 * top-level value. Any such invalid data will be detected as normal by the IonReader. In the few cases where this
 * wrapper does detect an error (e.g. upon finding the illegal type 0xF), it will raise {@link IonException}.
 */
public final class IonReaderLookaheadBufferArbitraryDepth extends ReaderLookaheadBufferBase implements IonReaderIncremental {

    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;
    private static final int HIGHEST_BIT_BITMASK = 0x80;
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;
    // Note: because long is a signed type, Long.MAX_VALUE is represented in Long.SIZE - 1 bits.
    private static final int MAXIMUM_SUPPORTED_VAR_UINT_BYTES = (Long.SIZE - 1) / VALUE_BITS_PER_VARUINT_BYTE;
    private static final int IVM_START_BYTE = 0xE0;
    private static final int IVM_FINAL_BYTE = 0xEA;
    private static final int IVM_REMAINING_LENGTH = 3; // Length of the IVM after the first byte.
    private static final int ION_SYMBOL_TABLE_SID = 3;
    // The following is a limitation imposed by this implementation, not the Ion specification.
    private static final long MAXIMUM_VALUE_SIZE = Integer.MAX_VALUE;

    /**
     * Represents a VarUInt that may be read in multiple steps.
     */
    private static final class VarUInt {

        /**
         * The location of the VarUInt in the value header.
         */
        private enum Location {
            /**
             * The length field that is included when the low nibble of a value's type ID is VARIABLE_LENGTH_NIBBLE.
             */
            VALUE_LENGTH,
            /**
             * The length field that is included when the low nibble of an annotation wrapper's type ID is
             * VARIABLE_LENGTH_NIBBLE.
             */
            ANNOTATION_WRAPPER_LENGTH,
            /**
             * The annot_length field that always precedes the SIDs in an annotation wrapper. Indicates the number
             * of total bytes used to represent the SIDs that follow.
             */
            ANNOTATION_WRAPPER_SIDS_LENGTH,
            /**
             * A symbol ID. An annotation wrapper may contain more than one.
             */
            //ANNOTATION_WRAPPER_SID

            FIELD_NAME_SID
        }

        /**
         * The location.
         */
        private Location location;

        /**
         * The value accumulated so far. This will only be the actual value when `isComplete` is true.
         */
        private long value;

        /**
         * The number of bytes in the VarUInt representation that have been read so far. This is only the total
         * number of bytes in the representation when `isComplete` is true.
         */
        private int numberOfBytesRead;

        /**
         * True when the VarUInt is complete; otherwise, false.
         */
        private boolean isComplete;

        /**
         * Constructor.
         */
        private VarUInt() {
            reset(Location.VALUE_LENGTH);
        }

        /**
         * Resets the value to zero.
         * @param nextLocation the location of the next VarUInt to read.
         */
        private void reset(final Location nextLocation) {
            location = nextLocation;
            value = 0;
            numberOfBytesRead = 0;
            isComplete = false;
        }
    }

    /**
     * The state of the wrapper.
     */
    private enum State {
        /**
         * Positioned before the type ID of a value.
         */
        BEFORE_TYPE_ID,

        /**
         * Started reading a value's type ID, but did not finish because the byte was not yet available.
         */
        READING_TYPE_ID,

        /**
         * Reading the value's header, which includes all bytes between the type ID and the first byte of
         * the value representation.
         */
        READING_HEADER,

        BEFORE_NOP_PAD,

        BEFORE_SCALAR,

        BEFORE_CONTAINER,

        BEFORE_FIELD_NAME,

        READING_FIELD_NAME,

        AFTER_CONTAINER,

        // TODO one 'skipping' state with a companion that tells what is being skipped?

        SKIPPING_ANNOTATION_SIDS,

        SKIPPING_PREVIOUS_VALUE,

        /**
         * Skipping over the value representation.
         */
        SKIPPING_BYTES,

        /**
         * There is nothing left to do.
         */
        //DONE
    }

    /**
     * Holds the start and end indices of a slice of the buffer.
     */
    static class Marker {
        /**
         * Index of the first byte in the slice.
         */
        int startIndex;

        /**
         * Index of the first byte after the end of the slice.
         */
        int endIndex;

        /**
         * @param startIndex index of the first byte in the slice.
         * @param length the number of bytes in the slice.
         */
        private Marker(final int startIndex, final int length) {
            this.startIndex = startIndex;
            this.endIndex = startIndex + length;
        }
    }

    interface IvmNotificationConsumer {
        void ivmEncountered(int majorVersion, int minorVersion);
    }

    /**
     * Holds the information that the binary reader must keep track of for containers at any depth.
     */
    private static class ContainerInfo {

        private IonType type;

        /**
         * The container's type.
         */
        private long totalLength;

        /**
         * The byte position of the end of the container.
         */
        private long remainingLength;
    }

    // Constructs ContainerInfo instances.
    private static final _Private_RecyclingStack.ElementFactory<ContainerInfo> CONTAINER_INFO_FACTORY =
        new _Private_RecyclingStack.ElementFactory<ContainerInfo>() {

            @Override
            public ContainerInfo newElement() {
                return new ContainerInfo();
            }
        };

    // Initial capacity of the stack used to hold ContainerInfo. Each additional level of nesting in the data requires
    // a new ContainerInfo. Depths greater than 8 will be rare.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 8;

    /**
     * Stack to hold container info. Stepping into a container results in a push; stepping out results in a pop.
      */
    private final _Private_RecyclingStack<ContainerInfo> containerStack;

    /**
     * The number of bytes to attempt to buffer each time more bytes are required.
     */
    private final int pageSize;

    /**
     * The VarUInt currently in progress.
     */
    private final VarUInt inProgressVarUInt;

    /**
     * Marker for the sequence of annotation symbol IDs on the current value. If there are no annotations on the
     * current value, the startIndex will be negative.
     */
    private final Marker annotationSidsMarker = new Marker(-1, 0);

    private final Marker valueMarker = new Marker(-1, 0);

    private final IvmNotificationConsumer ivmConsumer;

    private Event event = Event.NEEDS_DATA;

    /**
     * The number of additional bytes that must be read from `input` and stored in `pipe` before
     * {@link #moreDataRequired()} can return false.
     */
    private long additionalBytesNeeded;

    /**
     * True if the current value is a system value (IVM, symbol table, or NOP pad), not a user value.
     * `IonReader#next()` consumes any system values before the next user value, so the wrapper
     * must be able to identify system values so that their bytes can be included in `pipe` before
     * {@link #moreDataRequired()} returns false.
     */
    private boolean isIvm;

    /**
     * The number of bytes of annotation SIDs left to read from the value's annotation wrapper.
     */
    private long numberOfAnnotationSidBytesRemaining;

    /**
     * The number of annotations in the annotation wrapper that have been processed so far.
     */
    private long currentNumberOfAnnotations;

    /**
     * The current state of the wrapper.
     */
    private State state = State.BEFORE_TYPE_ID;

    /**
     * The write index of the start of the current value.
     */
    private int valueStartWriteIndex;

    /**
     * The number of bytes available in the buffer if truncated to `valueStartWriteIndex`.
     */
    private int valueStartAvailable;

    /**
     * The read index of the type ID byte of the current value.
     */
    private int valuePreHeaderIndex;

    /**
     * The read index of the first byte of the value representation of the current value (past the type ID and the
     * optional length field).
     */
    private int valuePostHeaderIndex;

    /**
     * The type ID byte of the current value.
     */
    private IonTypeID valueTid;

    /**
     * The index of the first byte after the end of the current value.
     */
    private int valueEndIndex;

    /**
     * The local symbol ID of the field name of the current value, or -1 if not positioned on a value with a field name.
     */
    private int fieldSid = -1;

    /**
     * The index of the first byte of the first no-op pad that precedes the current value. -1 indicates either that
     * the current value was not preceded by no-op padding or that the space occupied by the no-op padding that preceded
     * the current value has already been reclaimed.
     */
    private int nopPadStartIndex = -1;

    /**
     * The index of the second byte of the IVM.
     */
    private int ivmSecondByteIndex = -1;

    /**
     * The index of the next byte to peek from the buffer.
     */
    private int peekIndex = 0;

    /**
     * True if the event handler has not yet been notified if the current value is oversized.
     */
    private boolean handlerNeedsToBeNotifiedOfOversizedValue = true;

    // The major version of the Ion encoding currently being read.
    private int majorVersion = 1;

    // The minor version of the Ion encoding currently being read.
    private int minorVersion = 0;

    private boolean ivmEncountered = false;

    /**
     * Resets the wrapper to the start of a new value.
     */
    private void reset() {
        event = Event.NEEDS_DATA;
        setAdditionalBytesNeeded(0, true);
        isIvm = false;
        numberOfAnnotationSidBytesRemaining = 0;
        currentNumberOfAnnotations = 0;
        valuePreHeaderIndex = -1;
        valuePostHeaderIndex = -1;
        valueTid = null;
        valueEndIndex = -1;
        annotationSidsMarker.startIndex = -1;
        valueMarker.startIndex = -1;
        valueStartAvailable = pipe.available();
        startNewValue();
    }

    /**
     * Constructs a wrapper with the given configuration.
     * @param configuration the configuration for the new instance.
     * @param inputStream an InputStream over binary Ion data.
     */
    public IonReaderLookaheadBufferArbitraryDepth(final IonBufferConfiguration configuration, final IvmNotificationConsumer ivmConsumer, final InputStream inputStream) {
        super(configuration, inputStream);
        this.ivmConsumer = ivmConsumer;
        pipe.registerNotificationConsumer(
            new ResizingPipedInputStream.NotificationConsumer() {
                @Override
                public void bytesConsolidatedToStartOfBuffer(int leftShiftAmount) {
                    // The existing data in the buffer has been shifted to the start. Adjust the saved indexes
                    // accordingly. -1 indicates that all indices starting at 0 will be shifted.
                    shiftIndicesLeft(-1, leftShiftAmount);
                }
            }
        );
        pageSize = configuration.getInitialBufferSize();
        inProgressVarUInt = new VarUInt();
        containerStack = new _Private_RecyclingStack<ContainerInfo>(
            CONTAINER_STACK_INITIAL_CAPACITY,
            CONTAINER_INFO_FACTORY
        );
        reset();
    }

    /**
     * Resets the `inProgressVarUInt`.
     * @param location the VarUInt's location.
     */
    private void initializeVarUInt(final VarUInt.Location location) {
        inProgressVarUInt.reset(location);
        state = State.READING_HEADER;
    }

    /**
     * Reads one byte, if possible.
     * @return the byte, or -1 if none was available.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     */
    private int readByte() throws Exception {
        if (pipe.availableBeyondBoundary() == 0 && fillPage(1) < 1) {
            return -1;
        }
        int b;
        if (isSkippingCurrentValue()) {
            // If the value is being skipped, the byte will not have been buffered.
            b = getInput().read();
        } else {
            b = pipe.peek(peekIndex);
            pipe.extendBoundary(1);
            peekIndex++;
        }
        return b;
    }

    /**
     * Reads a VarUInt. NOTE: the VarUInt must fit in a `long`. This is not a true limitation, as IonJava requires
     * VarUInts to fit in an `int`.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     */
    private void readVarUInt() throws Exception {
        int currentByte;
        while (inProgressVarUInt.numberOfBytesRead < MAXIMUM_SUPPORTED_VAR_UINT_BYTES) {
            currentByte = readByte();
            if (currentByte < 0) {
                return;
            }
            inProgressVarUInt.numberOfBytesRead++;
            inProgressVarUInt.value =
                    (inProgressVarUInt.value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
            if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                inProgressVarUInt.isComplete = true;
                dataHandler.onData(inProgressVarUInt.numberOfBytesRead);
                subtractConsumedBytesFromParent(inProgressVarUInt.numberOfBytesRead); // TODO what if the user steps out before completing the VarUInt?
                return;
            }
        }
        throw new IonException("Found a VarUInt that was too large to fit in a `long`");
    }

    /**
     * Sets `additionalBytesNeeded` if and only if the value is not within an annotation wrapper. When the
     * value is contained in an annotation wrapper, `additionalBytesNeeded` was set when reading the annotation
     * wrapper's length and already includes the value's length.
     * @param value the new value of `additionalBytesNeeded`.
     * @param isUnannotated true if this type ID is not on a value within an annotation wrapper; false if it is.
     */
    private void setAdditionalBytesNeeded(final long value, final boolean isUnannotated) {
        if (isUnannotated) {
            additionalBytesNeeded = value;
        }
    }

    /**
     * Conveys the result of {@link #readTypeID(boolean)}.
     */
    private enum ReadTypeIdResult {
        /**
         * The type ID is for a struct value.
         */
        STRUCT,
        /**
         * The type ID is not for a struct value.
         */
        NOT_STRUCT,
        /**
         * The type ID could not be read because there is no data available. `readTypeID` should be called
         * again when more data is available.
         */
        NO_DATA
    }

    private void setNextStateFromTypeId() {
        // TODO when "buffer entire top level value" is enabled, go directly to SKIPPING_BYTES
        if (valueTid.isNopPad) {
            state = State.SKIPPING_BYTES;
        } else if (IonType.isContainer(valueTid.type)) {
            state = State.BEFORE_CONTAINER;
        } else {
            state = State.BEFORE_SCALAR;
        }
    }

    /**
     * Reads the type ID byte.
     * @param isUnannotated true if this type ID is not on a value within an annotation wrapper; false if it is.
     * @return the result as a {@link ReadTypeIdResult}.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     */
    private ReadTypeIdResult readTypeID(final boolean isUnannotated) throws Exception {
        int header = readByte();
        if (header < 0) {
            return ReadTypeIdResult.NO_DATA;
        }
        valueTid = IonTypeID.TYPE_IDS[header];
        dataHandler.onData(1);
        subtractConsumedBytesFromParent(1);
        if (header == IVM_START_BYTE && containerStack.isEmpty()) {
            if (!isUnannotated) {
                throw new IonException("Invalid annotation header.");
            }
            setAdditionalBytesNeeded(IVM_REMAINING_LENGTH, true);
            isIvm = true;
            // Encountering an IVM resets the symbol table context; no need to parse any previous symbol tables.
            //resetSymbolTableMarkers();
            ivmSecondByteIndex = peekIndex;
            state = State.SKIPPING_BYTES;
        } else if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonType.BOOL) {
            // bool values are always a single byte.
            state = State.BEFORE_TYPE_ID;
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            // Annotation.
            if (!isUnannotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (valueTid.variableLength) {
                initializeVarUInt(VarUInt.Location.ANNOTATION_WRAPPER_LENGTH);
            } else {
                setAdditionalBytesNeeded(valueTid.length, true);
                initializeVarUInt(VarUInt.Location.ANNOTATION_WRAPPER_SIDS_LENGTH);
            }
        } else {
            if (valueTid.isNull) {
                // null values are always a single byte.
                state = State.BEFORE_TYPE_ID;
            } else {
                // Not null
                if (valueTid.variableLength) {
                    initializeVarUInt(VarUInt.Location.VALUE_LENGTH);
                } else {
                    if (valueTid.isNopPad && !isUnannotated) {
                        throw new IonException(
                            "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
                        );
                    }
                    setAdditionalBytesNeeded(valueTid.length, isUnannotated);
                    setNextStateFromTypeId();
                }
            }
        }
        if (valueTid.type == IonType.STRUCT) {
            return ReadTypeIdResult.STRUCT;
        }
        return ReadTypeIdResult.NOT_STRUCT;
    }

    /**
     * Reads the bytes of the value header that occur after the type ID byte and before the first value byte.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     * @return true if the header is for an annotation wrapper; otherwise, false.
     */
    private boolean readHeader() throws Exception {
        if (inProgressVarUInt.location == VarUInt.Location.VALUE_LENGTH) {
            readVarUInt();
            if (inProgressVarUInt.isComplete) {
                setAdditionalBytesNeeded(inProgressVarUInt.value, true);
                setNextStateFromTypeId();
            }
            return false;
        }
        if (inProgressVarUInt.location == VarUInt.Location.ANNOTATION_WRAPPER_LENGTH) {
            readVarUInt();
            if (!inProgressVarUInt.isComplete) {
                return false;
            }
            setAdditionalBytesNeeded(inProgressVarUInt.value, true);
            initializeVarUInt(VarUInt.Location.ANNOTATION_WRAPPER_SIDS_LENGTH);
        }
        if (inProgressVarUInt.location == VarUInt.Location.ANNOTATION_WRAPPER_SIDS_LENGTH) {
            readVarUInt();
            if (!inProgressVarUInt.isComplete) {
                return false;
            }
            additionalBytesNeeded -= inProgressVarUInt.numberOfBytesRead;
            numberOfAnnotationSidBytesRemaining = inProgressVarUInt.value;
            //initializeVarUInt(VarUInt.Location.ANNOTATION_WRAPPER_SID);
            annotationSidsMarker.startIndex = peekIndex;
            annotationSidsMarker.endIndex = annotationSidsMarker.startIndex + (int) numberOfAnnotationSidBytesRemaining;
            if (additionalBytesNeeded <= 0) { // TODO check
                throw new IonException("Annotation wrappers without values are invalid.");
            }
            // TODO when "buffer entire top level value" is enabled, go directly to SKIPPING_BYTES
            state = State.SKIPPING_ANNOTATION_SIDS;
        }
        /*
        if (inProgressVarUInt.location == VarUInt.Location.ANNOTATION_WRAPPER_SID) {
            // Read the first annotation SID, which is all that is required to determine whether the value is a
            // symbol table.
            readVarUInt();
            if (inProgressVarUInt.isComplete) {
                numberOfAnnotationSidBytesRemaining -= inProgressVarUInt.numberOfBytesRead;
                additionalBytesNeeded -= inProgressVarUInt.numberOfBytesRead;
                //if (inProgressVarUInt.value == ION_SYMBOL_TABLE_SID) {
                //    state = State.READING_VALUE_WITH_SYMBOL_TABLE_ANNOTATION;
                //} else {
                //state = State.SKIPPING_VALUE;
                //}

                // TODO read past the embedded header to determine the next state (scalar or container)
                state = State.BEFORE_TYPE_ID;
                return true;
            }
        }*/
        return false;
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
        valuePreHeaderIndex -= shiftAmount;
        valuePostHeaderIndex -= shiftAmount;
        valueStartWriteIndex -= shiftAmount;
        /*
        for (Marker symbolTableMarker : symbolTableMarkers) {
            if (symbolTableMarker.startIndex > afterIndex) {
                symbolTableMarker.startIndex -= shiftAmount;
                symbolTableMarker.endIndex -= shiftAmount;
            }
        }

         */
        if (annotationSidsMarker.startIndex > afterIndex) {
            annotationSidsMarker.startIndex -= shiftAmount;
            annotationSidsMarker.endIndex -= shiftAmount;
        }
        if (ivmSecondByteIndex > afterIndex) {
            ivmSecondByteIndex -= shiftAmount;
        }
    }

    /**
     * Reclaim the NOP padding that occurred before the current value, making space for the value in the buffer.
     */
    private void reclaimNopPadding() {
        pipe.consolidate(valuePreHeaderIndex, nopPadStartIndex);
        shiftIndicesLeft(nopPadStartIndex, valuePreHeaderIndex - nopPadStartIndex);
        resetNopPadIndex();
    }

    /**
     * Skip bytes from the underlying InputStream without ever buffering them.
     * @param numberOfBytesToSkip the number of bytes to attempt to skip.
     * @return the number of bytes actually skipped.
     * @throws IOException if thrown by the underlying InputStream.
     */
    private long skipBytesFromInput(long numberOfBytesToSkip) throws IOException {
        try {
            return getInput().skip(numberOfBytesToSkip);
        } catch (EOFException e) {
            // Certain InputStream implementations (e.g. GZIPInputStream) throw EOFException if more bytes are requested
            // to skip than are currently available (e.g. if a header or trailer is incomplete).
            return 0;
        }
    }

    /**
     * Retrieve and buffer up to {@link #pageSize} bytes from the input.
     * @param numberOfBytesRequested the minimum amount of space that must be available before the buffer reaches
     *                               its configured maximum size.
     * @return the number of bytes buffered by this operation.
     * @throws Exception if thrown by the underlying InputStream.
     */
    private int fillPage(int numberOfBytesRequested) throws Exception {
        int amountToFill = pipe.capacity() - pipe.size();
        if (amountToFill <= 0) {
            // Try to fill the remainder of the existing buffer to avoid growing unnecessarily. If there is no
            // space, that indicates that a single value exceeds the size of a page. In that case, fill another page,
            // growing the buffer only up to the configured maximum size.
            int spaceAvailable = getMaximumBufferSize() - pipe.capacity();
            if (numberOfBytesRequested > spaceAvailable) {
                if (nopPadStartIndex > -1 && valuePreHeaderIndex - nopPadStartIndex >= numberOfBytesRequested) {
                    // Reclaim the NOP pad space if doing so would allow the value to fit.
                    reclaimNopPadding();
                } else {
                    startSkippingValue();
                }
                amountToFill = numberOfBytesRequested;
            } else {
                amountToFill = Math.min(pageSize, spaceAvailable);
            }
        }
        int received;
        if (isSkippingCurrentValue()) {
            if (state == State.SKIPPING_BYTES) {
                // This is a seek operation, meaning that the bytes don't need to be interpreted.
                received = (int) skipBytesFromInput(amountToFill);
            } else {
                // The bytes need to be interpreted, so they cannot be skipped. The caller must retrieve them from
                // the input.
                received = amountToFill;
            }
        } else {
            received = pipe.receive(getInput(), amountToFill);
        }
        return received;
    }

    /**
     * Notifies the event handler that the current value is oversized, if the handler has not already been notified.
     * @throws Exception if thrown by the handler.
     */
    private void notifyHandlerOfOversizedValue() throws Exception {
        if (handlerNeedsToBeNotifiedOfOversizedValue) {
            //if (isSystemValue) {
                // Processing cannot continue after system values (symbol tables) are truncated because subsequent
                // values may be unreadable. Notify the user.
            //    oversizedSymbolTableHandler.onOversizedSymbolTable();
            //} else {
                // An oversized user value has been encountered. Notify the user so they can decide whether to continue
                // or abort.
                oversizedValueHandler.onOversizedValue();
            //}
        }
        handlerNeedsToBeNotifiedOfOversizedValue = false;
    }

    /**
     * Attempt to retrieve at least `additionalBytesNeeded` bytes from the input and either buffer them (if the value
     * is being consumed) or skip them (if the value is being skipped due to being oversize).
     * @return the number of bytes filled or skipped.
     * @throws Exception if thrown by the event handler.
     */
    private long fillOrSkip(boolean shouldExtendBoundary) throws Exception {
        // Clamping at the number of buffered bytes available guarantees that the buffer
        // will never grow beyond its initial size.
        long bytesRequested = additionalBytesNeeded - pipe.availableBeyondBoundary();
        long bytesFilled;
        if (isSkippingCurrentValue()) {
            bytesFilled = skipBytesFromInput(bytesRequested);
        } else {
            if (additionalBytesNeeded > MAXIMUM_VALUE_SIZE) {
                throw new IonException("The size of the value exceeds the limits of the implementation.");
            }
            bytesFilled = fillPage((int) bytesRequested);
        }
        if (bytesFilled < 1) {
            return 0;
        }
        if (isSkippingCurrentValue()) {
            // The user cannot be notified of a size violation until it has been determined whether
            // the value is a symbol table or user value, which is only true in the SKIPPING_VALUE
            // state.
            notifyHandlerOfOversizedValue();
            // Skip all of the bytes skipped from the InputStream as well as all bytes previously
            // buffered.
            bytesFilled = bytesFilled + ((int) additionalBytesNeeded - bytesRequested);
        } else {
            bytesFilled = Math.min(additionalBytesNeeded, bytesFilled);
            if (shouldExtendBoundary) {
                pipe.extendBoundary((int) bytesFilled);
            }
            peekIndex += (int) bytesFilled;
        }
        return bytesFilled;
    }

    /**
     * Attempts to skip the requested number of bytes.
     * @param numberOfBytesToSkip the number of bytes to attempt to skip.
     * @return the number of bytes actually skipped.
     * @throws Exception if thrown by the event handler.
     */
    private long skip(long numberOfBytesToSkip) throws Exception {
        long numberOfBytesSkipped;
        if (pipe.availableBeyondBoundary() >= numberOfBytesToSkip) {
            numberOfBytesSkipped = (int) numberOfBytesToSkip;
            pipe.extendBoundary((int) numberOfBytesSkipped);
            peekIndex = pipe.getBoundary(); //+= (int) numberOfBytesSkipped; // TODO check
        } else {
            numberOfBytesSkipped = fillOrSkip(true);
        }
        if (numberOfBytesSkipped > 0) {
            long numberOfBytesToReport = numberOfBytesSkipped;
            while (numberOfBytesToReport > 0) {
                int numberOfBytesToReportThisIteration = (int) Math.min(Integer.MAX_VALUE, numberOfBytesToReport);
                dataHandler.onData(numberOfBytesToReportThisIteration);
                numberOfBytesToReport -= numberOfBytesToReportThisIteration;
            }
            subtractConsumedBytesFromParent(numberOfBytesSkipped);
        }
        return numberOfBytesSkipped;
    }

    /*
     * The state transitions of the fillInput() method are summarized by the following diagram.
     *
     *                                 fillInput()
     *                                       |
     *                                       |   +----------------------------------------------------+
     *                                       |   |                                                    |
     *       Read first byte of IVM +--------v---v-+                                                  |
     *   +--------------------------+BEFORE_TYPE_ID<--+                                               |
     *   |   Or length was inferred |              +--+1-byte value(null or bool) read                |
     *   |    from the type ID    +>+------+-------+                                                  |
     *   |                        |        |                                                          |
     *   |                        |        |No bytes available                                        |
     *   |           1-byte value |        v                                                          |
     *   |                        | +----------------+                                                |
     *   |                        +-+READING_TYPE_ID <-+                                              |
     *   |                          |                +-+No bytes available                            |
     *   |                          +------+---------+                                                |
     *   |                                 |Read type-id of multi-byte value                          |
     *   |                                 v                                                          |
     *   |      Finished header     +----------------+                                                |
     *   | +------------------------+READING_HEADER  <--+                                             |
     *   | |                        |                +--+Not enough bytes to complete header          |
     *   | |                        +------+---------+                                                |
     *   | |                               |Read annotation wrapper with symbol table annotation      |
     *   | |                               v                                                          |
     *   | |             +---------------------------------------------+                              |
     *   | |             |READING_VALUE_WITH_SYMBOL_TABLE_ANNOTATION   <--+                           |
     *   | |             |                                             +--+No bytes available         |
     *   | |             +--+--------------+---------------------------+                              |
     *   | |                |              |The wrapped value is a struct                             |
     *   | | The wrapped    |              v                                                          |
     *   | |  value is not  |  +-----------------------------+                                        |
     *   | |   a struct     |  | READING_SYMBOL_TABLE_LENGTH <--+                                     |
     *   | |                |  |                             +--+Not enough bytes to complete length  |
     *   | |                |  +-----------+-----------------+                                        |
     *   | |                |              |Read length                                               |
     *   | |                |              v                                                          |
     *   | |                |      +-----------------+                                                |
     *   | +--------------->+----->| SKIPPING_VALUE  <--+                                             |
     *   |                         |                 +--+More bytes needed to complete skip           |
     *   +------------------------>+-------+---------+                                                |
     *                                     |                                                          |
     *                                     |All bytes skipped                                         |
     *                                     |                                                          |
     *                                     +----------------------------------------------------------+
     */
    @Override
    protected void fillInputHelper() throws Exception {
        // TODO might not need
    }

    // nextHeader();
    // // moreDataRequired() will be true until the header is complete. fillInput() must be called to complete it.
    // while (moreDataRequired()) {
    //   fillInput();
    // }
    // isOnContainer(); // Let's say this returns true.
    // // stepIn() doesn't advance over any bytes, so moreDataRequired() is always false following this call.
    // stepIn();
    // nextHeader();
    // while (moreDataRequired()) {
    //   fillInput();
    // }
    // isOnContainer(); // Let's say this returns false.
    // fillScalar();
    // // moreDataRequired() will be true until all bytes in the scalar are buffered.
    // while (moreDataRequired()) {
    //   fillInput();
    // }
    // // The caller can peek the buffer to extract the scalar value.
    // // Step out, skipping any other values at this depth.
    // stepOut();
    // // moreDataRequired() will be true until the remaining bytes in the container are skipped.
    // while (moreDataRequired()) {
    //   fillInput();
    // }
    // nextHeader();

    private void subtractConsumedBytesFromParent(long numberOfBytesConsumed) {
        if (!containerStack.isEmpty()) {
            containerStack.peek().remainingLength -= numberOfBytesConsumed;
        }
    }

    /**
     * Throw if the reader is attempting to process an Ion version that it does not support.
     */
    private void requireSupportedIonVersion() {
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
    }

    /**
     * Reads past the next value header at the current depth.
     * @throws Exception
     */
    public void nextHeader() throws Exception {
        if (event != Event.NEEDS_DATA && state == State.BEFORE_SCALAR || state == State.BEFORE_CONTAINER) {
            // Skip the previous value unless the last event was NEEDS_DATA, in which case attempt to complete it.
            state = State.SKIPPING_PREVIOUS_VALUE;
        }
        event = Event.NEEDS_DATA;
        if (state == State.SKIPPING_PREVIOUS_VALUE) {
            while (additionalBytesNeeded > 0) {
                long numberOfBytesSkipped = skip(additionalBytesNeeded);
                if (numberOfBytesSkipped < 1) {
                    return;
                }
                additionalBytesNeeded -= numberOfBytesSkipped;
            }
            if (isInStruct()) {
                state = State.BEFORE_FIELD_NAME;
            } else {
                state = State.BEFORE_TYPE_ID;
            }
        }
        if (!containerStack.isEmpty()) {
            if (containerStack.peek().remainingLength == 0) {
                state = State.AFTER_CONTAINER;
                event = Event.END_CONTAINER;
                // TODO reset other state?
                valueTid = null;
                return;
            } else if (containerStack.peek().remainingLength < 0) {
                throw new IonException("Contained values overflowed the parent container length.");
            }
        }
        while (true) {
            if (state == State.BEFORE_FIELD_NAME) {
                initializeVarUInt(VarUInt.Location.FIELD_NAME_SID);
                state = State.READING_FIELD_NAME;
            }
            if (state == State.READING_FIELD_NAME) {
                readVarUInt();
                if (!inProgressVarUInt.isComplete) {
                    return;
                }
                fieldSid = (int) inProgressVarUInt.value;
                state = State.BEFORE_TYPE_ID;
            }
            if (state == State.BEFORE_TYPE_ID || state == State.READING_TYPE_ID) {
                reset();
                state = State.READING_TYPE_ID;
                if (readTypeID(true) != ReadTypeIdResult.NO_DATA) {
                    // The previous line transfers at most one byte, so the pre-header index is the write index minus
                    // one.
                    valuePostHeaderIndex = peekIndex;
                    valuePreHeaderIndex = valuePostHeaderIndex - 1;
                    valueStartWriteIndex = valuePreHeaderIndex;
                    valueEndIndex = valuePostHeaderIndex + valueTid.length; // TODO not right for variable length, but doesn't matter
                }
            }
            if (state == State.READING_HEADER) {
                readHeader();
                if (!inProgressVarUInt.isComplete) {
                    return;
                }
                valuePostHeaderIndex = peekIndex;
                valueEndIndex = valuePostHeaderIndex + (int) additionalBytesNeeded; // TODO check
                // TODO seek past the header (freeing that space in the buffer) unless the value is annotated.
            }
            /*
            if (state == State.READING_VALUE_WITH_SYMBOL_TABLE_ANNOTATION) {
                // Skip annotations until positioned on the value's type ID.
                while (numberOfAnnotationSidBytesRemaining > 0) {
                    long numberOfBytesSkipped = skip(numberOfAnnotationSidBytesRemaining);
                    if (numberOfBytesSkipped < 1) {
                        return;
                    }
                    numberOfAnnotationSidBytesRemaining -= numberOfBytesSkipped;
                    additionalBytesNeeded -= numberOfBytesSkipped;
                }
                ReadTypeIdResult result = readTypeID(false);
                if (result == ReadTypeIdResult.NO_DATA) {
                    return;
                }
                // When successful, readTypeID reads exactly one byte.
                additionalBytesNeeded--;
                if (result == ReadTypeIdResult.STRUCT) {
                    state = State.READING_SYMBOL_TABLE_LENGTH;
                } else {
                    state = State.SKIPPING_VALUE;
                }
            }
            if (state == State.READING_SYMBOL_TABLE_LENGTH) {
                isSystemValue = true;
                if (inProgressVarUInt.location == VarUInt.Location.VALUE_LENGTH) {
                    readVarUInt();
                    if (!inProgressVarUInt.isComplete) {
                        return;
                    }
                    additionalBytesNeeded = inProgressVarUInt.value;
                }
                symbolTableMarkers.add(new Marker(peekIndex, (int) additionalBytesNeeded));
                state = State.SKIPPING_VALUE;
            }

             */
            if (state == State.SKIPPING_ANNOTATION_SIDS) {
                while (numberOfAnnotationSidBytesRemaining > 0) {
                    long numberOfBytesSkipped = skip(numberOfAnnotationSidBytesRemaining);
                    if (numberOfBytesSkipped < 1) {
                        return;
                    }
                    numberOfAnnotationSidBytesRemaining -= numberOfBytesSkipped;
                    additionalBytesNeeded -= numberOfBytesSkipped;
                }
                // TODO reading the header of the wrapped value could technically be deferred, but it adds complexity
                //  to this code and only saves time in the case where the user chooses to skip based on annotations
                //  alone.
                if (readTypeID(false) != ReadTypeIdResult.NO_DATA) {
                    // The previous line transfers at most one byte, so the pre-header index is the write index minus
                    // one.
                    additionalBytesNeeded -= 1;
                    valuePostHeaderIndex = peekIndex;
                    valuePreHeaderIndex = valuePostHeaderIndex - 1;
                    valueStartWriteIndex = valuePreHeaderIndex;
                }
                if (state == State.READING_HEADER) {
                    // Consume the length byte(s) from the header of the wrapped value.
                    continue;
                }
            }
            if (state == State.SKIPPING_BYTES) {
                if (valueTid.isNopPad) {
                    if (pipe.availableBeyondBoundary() <= additionalBytesNeeded) {
                        // There cannot be any meaningful data beyond the NOP pad, so the buffer can be truncated
                        // immediately and the rest of the NOP pad skipped.
                        additionalBytesNeeded -= pipe.availableBeyondBoundary();
                        startSkippingValue();
                        // NOP padding will not be buffered, so it is never considered oversized.
                        handlerNeedsToBeNotifiedOfOversizedValue = false;
                    }
                    // Else, the rest of the NOP pad is already buffered, and there is a value at least partially
                    // buffered beyond it. The NOP pad will only be deleted from the buffer if the next value is
                    // large enough that it doesn't fit within the buffer's configured maximum size.
                }
                while (additionalBytesNeeded > 0) {
                    long numberOfBytesSkipped = skip(additionalBytesNeeded);
                    if (numberOfBytesSkipped < 1) {
                        return;
                    }
                    additionalBytesNeeded -= numberOfBytesSkipped;
                }
                // TODO when "buffer entire top level value" is enabled, return to the state and position where the
                //  buffering began. Ensure additional buffering does not occur until the end of the value.
                state = State.BEFORE_TYPE_ID;
            }
            if (state == State.BEFORE_TYPE_ID) {
                valueEndIndex = peekIndex;
                if (isIvm || isSkippingCurrentValue() || valueTid.isNopPad) {
                    if (valueTid.isNopPad && nopPadStartIndex < 0) {
                        // This is the first NOP before the next value. Mark the start index in case the space needs to
                        // be reclaimed later.
                        nopPadStartIndex = valuePreHeaderIndex;
                    }
                    /*
                    if (isSystemValue && isSkippingCurrentValue()) {
                        // The symbol table(s) currently buffered exceed the maximum buffer size. This is not
                        // recoverable; future invocations of fillInput() will do nothing.
                        reset();
                        state = State.DONE;
                    } else {
                        if (isSystemValue && nopPadStartIndex > -1) {
                            // Reclaim any NOP pad space that precedes system values. This will usually not be strictly
                            // necessary, but it simplifies the implementation and will be rare in practice. Without
                            // this simplification, we would need to keep track of a list of NOP pad start/end indexes
                            // as we do with the symbol table markers. This way, we know that there can only be one
                            // uninterrupted run of NOP pad bytes immediately preceding any user value, making it easy
                            // to reclaim this space if necessary.
                            reclaimNopPadding();
                        }
                        // Just skipped over system value or an oversized value. Consume the next value too so that a
                        // call to reader.next() won't return null.
                        continue;
                    }
                     */
                    if (isIvm) {
                        if (nopPadStartIndex > -1) {
                            // Reclaim any NOP pad space that precedes system values. This will usually not be strictly
                            // necessary, but it simplifies the implementation and will be rare in practice. Without
                            // this simplification, we would need to keep track of a list of NOP pad start/end indexes
                            // as we do with the symbol table markers. This way, we know that there can only be one
                            // uninterrupted run of NOP pad bytes immediately preceding any user value, making it easy
                            // to reclaim this space if necessary.
                            reclaimNopPadding(); // TODO check if necessary.
                        }
                        majorVersion = pipe.peek(ivmSecondByteIndex++);
                        minorVersion = pipe.peek(ivmSecondByteIndex++);
                        if (pipe.peek(ivmSecondByteIndex++) != IVM_FINAL_BYTE) {
                            throw new IonException("Invalid Ion version marker.");
                        }
                        requireSupportedIonVersion();
                        ivmConsumer.ivmEncountered(majorVersion, minorVersion);
                        // TODO seek the pipe past the IVM, freeing space if necessary (check)
                        pipe.seekTo(ivmSecondByteIndex);
                    }
                    continue;
                }
            }
            break;
        }
        switch (state) {
            case BEFORE_SCALAR:
                event = Event.START_SCALAR;
                break;
            case BEFORE_CONTAINER:
                // Note: the following check is necessary to catch empty ordered structs, which are prohibited by the spec.
                // Unfortunately, this requires a check on every value for a condition that will probably never happen.
                if (
                    valueTid.type == IonType.STRUCT &&
                    valueTid.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
                    getValueStart() == valueEndIndex // TODO check whether getValueStart() is correct
                ) {
                    throw new IonException("Ordered struct must not be empty.");
                }
                event = Event.START_CONTAINER;
                break;
            default:
                break;
        }
    }

    /**
     *
     * @return a marker for the buffered value, or null if the value is not yet completely buffered.
     * @throws Exception
     */
    private void fillValue() throws Exception {
        // Must be positioned on a scalar.
        if (state != State.BEFORE_SCALAR && state != State.BEFORE_CONTAINER) {
            throw new IllegalStateException();
        }
        event = Event.NEEDS_DATA;

        if (pipe.availableBeyondBoundary() < additionalBytesNeeded) {
            fillOrSkip(false);
            if (pipe.availableBeyondBoundary() < additionalBytesNeeded) {
                return;
            }
        }
        valueMarker.startIndex = valuePostHeaderIndex;
        valueMarker.endIndex = valueEndIndex;
        event = Event.VALUE_READY;
    }

    public void stepIn() {
        // Must be positioned on a container.
        if (state != State.BEFORE_CONTAINER) {
            throw new IonException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        ContainerInfo containerInfo = containerStack.push();
        containerInfo.type = valueTid.type;
        if (additionalBytesNeeded > 0) {
            containerInfo.totalLength = additionalBytesNeeded;
            setAdditionalBytesNeeded(0, true);
        } else {
            // The user must have requested to load the container. // TODO check
            containerInfo.totalLength = valueMarker.endIndex - valueMarker.startIndex;
        }
        containerInfo.remainingLength = containerInfo.totalLength;
        // TODO seek past header (no need to hold onto it)
        pipe.seekTo(valuePostHeaderIndex); // tODO check
        //additionalBytesNeeded = 0;
        if (valueTid.type == IonType.STRUCT) {
            state = State.BEFORE_FIELD_NAME;
        } else {
            state = State.BEFORE_TYPE_ID;
        }
        // TODO reset other state
        valueTid = null;
        fieldSid = -1;
        event = Event.NEEDS_INSTRUCTION;
    }

    public void stepOut() throws Exception {
        // Seek past the remaining bytes at this depth, pop from the stack, and subtract the number of bytes
        // consumed at the previous depth from the remaining bytes needed at the current depth. Set the state to
        // BEFORE_TYPE_ID
        if (containerStack.isEmpty()) {
            // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        ContainerInfo containerInfo = containerStack.pop();
        setAdditionalBytesNeeded(containerInfo.remainingLength, true);

        // Seek past any remaining bytes from the previous value.
        if (additionalBytesNeeded > 0) {
            //startSkippingValue();
            //handlerNeedsToBeNotifiedOfOversizedValue = false;
            state = State.SKIPPING_BYTES;
        }

        while (additionalBytesNeeded > 0) {
            long numberOfBytesSkipped = skip(additionalBytesNeeded);
            if (numberOfBytesSkipped < 1) {
                return;
            }
            additionalBytesNeeded -= numberOfBytesSkipped;
        }
        if (!containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT) {
            state = State.BEFORE_FIELD_NAME;
        } else {
            state = State.BEFORE_TYPE_ID;
        }
        event = Event.NEEDS_INSTRUCTION;
        subtractConsumedBytesFromParent(containerInfo.totalLength);
        // tODO reset other state (e.g. annotations?)
        valueTid = null;
        fieldSid = -1;
    }

    @Override
    public Event next(Instruction instruction) {
        switch (instruction) {
            case STEP_IN:
                stepIn();
                break;
            case NEXT_VALUE:
                try {
                    nextHeader();
                } catch (Exception e) {
                    throw new IonException(e);
                }
                break;
            case LOAD_VALUE:
                try {
                    fillValue();
                } catch (Exception e) {
                    throw new IonException(e);
                }
                break;
            case STEP_OUT:
                try {
                    stepOut();
                } catch (Exception e) {
                    throw new IonException(e);
                }
                break;
        }
        return event;
    }

    @Override
    public Event getCurrentEvent() {
        return event;
    }

    @Override
    public void fill(InputStream inputStream) {

    }

    @Override
    void truncateToEndOfPreviousValue() {
        peekIndex = valueStartWriteIndex;
        pipe.truncate(valueStartWriteIndex, valueStartAvailable);
        handlerNeedsToBeNotifiedOfOversizedValue = true;
    }

    @Override
    public boolean moreDataRequired() { // TODO API no longer needed?
        return event == Event.NEEDS_DATA;
    }

    /**
     * Checks and clears the IVM indicator. Should be called between user values.
     */
    boolean checkAndResetIvm() { // TODO no longer needed?
        boolean isIvmEncountered = ivmEncountered;
        ivmEncountered = false;
        return isIvmEncountered;
    }

    int ionMajorVersion() {
        return majorVersion;
    }

    int ionMinorVersion() {
        return minorVersion;
    }

    /**
     * Clears the NOP pad index. Should be called between user values.
     */
    void resetNopPadIndex() {
        nopPadStartIndex = -1;
    }

    /**
     * @return the index of the first byte of the value representation (past the type ID and the optional length field).
     */
    int getValueStart() {
        if (hasAnnotations()) {
            return annotationSidsMarker.endIndex;
        }
        return valuePostHeaderIndex;
    }

    /**
     * @return the type ID of the current value.
     */
    IonTypeID getValueTid() {
        return valueTid;
    }

    /**
     * @return the index of the first byte after the end of the current value.
     */
    int getValueEnd() {
        return valueEndIndex;
    }


    /**
     * @return true if the current value has annotations; otherwise, false.
     */
    boolean hasAnnotations() {
        return annotationSidsMarker.startIndex >= 0;
    }
    /**
     * Returns the marker for the sequence of annotation symbol IDs on the current value. The startIndex of the
     * returned marker is the index of the first byte of the first annotation symbol ID in the sequence. The endIndex
     * of the returned marker is the index of the type ID byte of the value to which the annotations are applied.
     * @return  the marker.
     */
    Marker getAnnotationSidsMarker() {
        return annotationSidsMarker;
    }

    Marker getValueMarker() {
        return valueMarker;
    }

    public boolean isInStruct() {
        return !containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT;
    }

    int getFieldId() {
        return fieldSid;
    }

    public int getDepth() {
        return containerStack.size();
    }

}
