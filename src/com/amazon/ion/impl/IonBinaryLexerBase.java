package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReaderIncremental;
import com.amazon.ion.IonType;

import java.io.IOException;
import java.io.InputStream;

abstract class IonBinaryLexerBase<Buffer extends AbstractBuffer> implements IonReaderIncremental {

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

    protected enum CheckpointLocation {
        BEFORE_UNANNOTATED_TYPE_ID,
        BEFORE_ANNOTATED_TYPE_ID,
        AFTER_SCALAR_HEADER,
        AFTER_CONTAINER_HEADER
    }

    // TODO all indexes should be longs.

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
        private int startIndex;

        /**
         * The byte position of the end of the container.
         */
        private int endIndex;

        void set(IonType type, Marker marker) {
            this.type = type;
            this.startIndex = marker.startIndex;
            this.endIndex = marker.endIndex;
        }
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

    protected final Buffer buffer;

    /**
     * The handler that will be notified when data is processed.
     */
    protected final BufferConfiguration.DataHandler dataHandler;

    /**
     * Marker for the sequence of annotation symbol IDs on the current value. If there are no annotations on the
     * current value, the startIndex will be negative.
     */
    protected final Marker annotationSidsMarker = new IonBinaryLexerRefillable.Marker(-1, 0);

    protected final Marker valueMarker = new IonBinaryLexerRefillable.Marker(-1, 0);

    private final IvmNotificationConsumer ivmConsumer;

    private IonReaderIncremental.Event event = IonReaderIncremental.Event.NEEDS_DATA;

    // The major version of the Ion encoding currently being read.
    private int majorVersion = 1;

    // The minor version of the Ion encoding currently being read.
    private int minorVersion = 0;

    /**
     * The type ID byte of the current value.
     */
    private IonTypeID valueTid;

    private CheckpointLocation checkpointLocation = CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID;

    private int fieldSid;
    
    protected int checkpoint = 0;

    protected int peekIndex = 0;

    IonBinaryLexerBase(
        final Buffer buffer,
        final BufferConfiguration.DataHandler dataHandler,
        final IvmNotificationConsumer ivmConsumer
    ) {
        this.buffer = buffer;
        this.dataHandler = dataHandler;
        this.ivmConsumer = ivmConsumer;
        containerStack = new _Private_RecyclingStack<ContainerInfo>(
            CONTAINER_STACK_INITIAL_CAPACITY,
            CONTAINER_INFO_FACTORY
        );
    }

    protected abstract int readByte() throws Exception; // TODO remove throws Exception

    /**
     * Throw if the reader is attempting to process an Ion version that it does not support.
     */
    private void requireSupportedIonVersion() {
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
    }

    private void verifyValueLength() {
        if (!containerStack.isEmpty()) {
            if (valueMarker.endIndex > containerStack.peek().endIndex) {
                throw new IonException("Value exceeds the length of its parent container.");
            }
        }
    }

    private boolean checkContainerEnd() {
        if (!containerStack.isEmpty()) {
            if (containerStack.peek().endIndex == peekIndex) {
                // TODO set checkpoint?
                event = Event.END_CONTAINER;
                // TODO reset other state?
                valueTid = null;
                return true;
            } else if (containerStack.peek().endIndex < peekIndex) {
                throw new IonException("Contained values overflowed the parent container length.");
            }
        }
        return false;
    }

    protected void reset() {
        valueMarker.startIndex = -1;
        valueMarker.endIndex = -1;
        annotationSidsMarker.startIndex = -1;
        annotationSidsMarker.endIndex = -1;
        fieldSid = -1;
    }

    void setCheckpoint(CheckpointLocation location) throws IOException {
        if (location == CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID) {
            reset();
            buffer.seekTo(peekIndex);
        }
        checkpointLocation = location;
        checkpoint = peekIndex;
    }

    /**
     * Reads the type ID byte.
     * @param isUnannotated true if this type ID is not on a value within an annotation wrapper; false if it is.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     */
    private boolean parseTypeID(final int typeIdByte, final boolean isUnannotated) throws Exception {
        valueTid = IonTypeID.TYPE_IDS[typeIdByte];
        dataHandler.onData(1);
        int valueLength = 0;
        if (typeIdByte == IVM_START_BYTE && containerStack.isEmpty()) {
            if (!isUnannotated) {
                throw new IonException("Invalid annotation header.");
            }
            if (!buffer.fill(IVM_REMAINING_LENGTH)) {
                return false;
            }
            majorVersion = buffer.peek(peekIndex++);
            minorVersion = buffer.peek(peekIndex++);
            if (buffer.peek(peekIndex++) != IVM_FINAL_BYTE) {
                throw new IonException("Invalid Ion version marker.");
            }
            requireSupportedIonVersion();
            ivmConsumer.ivmEncountered(majorVersion, minorVersion);
            // TODO seek the pipe past the IVM, freeing space if necessary (check)
            buffer.seekTo(peekIndex);
            setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
        } else if (!valueTid.isValid) {
            throw new IonException("Invalid type ID.");
        } else if (valueTid.type == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
            // Annotation.
            if (!isUnannotated) {
                throw new IonException("Nested annotation wrappers are invalid.");
            }
            if (valueTid.variableLength) {
                valueLength = (int) readVarUInt(); // TODO unify typing
                if (valueLength < 0) {
                    return false;
                }
            } else {
                valueLength = valueTid.length;
            }
            int postLengthIndex = peekIndex;
            long annotationsLength = readVarUInt();
            if (annotationsLength < 0) {
                return false;
            }
            if (!buffer.fill((int) annotationsLength)) { // TODO skip if the value isalready oversized
                return false;
            }
            annotationSidsMarker.startIndex = peekIndex;
            annotationSidsMarker.endIndex = annotationSidsMarker.startIndex + (int) annotationsLength;
            peekIndex = annotationSidsMarker.endIndex;
            valueLength -= peekIndex - postLengthIndex; // TODO might not be necessary
            setCheckpoint(CheckpointLocation.BEFORE_ANNOTATED_TYPE_ID);
        } else {
            if (valueTid.isNull || valueTid.type == IonType.BOOL) {
                // null values are always a single byte.
            } else {
                // Not null
                if (valueTid.variableLength) {
                    // TODO only set to valueLength if unannotated? Is that necessary?
                    valueLength = (int) readVarUInt(); // TODO unify typing
                    if (valueLength < 0) {
                        return false;
                    }
                } else {
                    if (valueTid.isNopPad && !isUnannotated) {
                        throw new IonException(
                            "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
                        );
                    }
                    // TODO only set to valueLength if unannotated? Is that necessary?
                    valueLength = valueTid.length;
                }
            }
            if (IonType.isContainer(valueTid.type)) {
                setCheckpoint(CheckpointLocation.AFTER_CONTAINER_HEADER);
                event = Event.START_CONTAINER;
            } else {
                setCheckpoint(CheckpointLocation.AFTER_SCALAR_HEADER);
                event = Event.START_SCALAR;
            }
            if (valueTid.isNopPad) {
                if (!buffer.seek(valueLength)) {
                    return false;
                }
                peekIndex += valueLength;
                valueLength = 0;
                setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                checkContainerEnd();
            }
        }
        valueMarker.startIndex = checkpoint;
        valueMarker.endIndex = checkpoint + valueLength; // TODO check
        verifyValueLength(); // TODO check
        return true;
    }

    /**
     * Reads a VarUInt. NOTE: the VarUInt must fit in a `long`. This is not a true limitation, as IonJava requires
     * VarUInts to fit in an `int`.
     * @throws Exception if thrown by a handler method or if an IOException is thrown by the underlying InputStream.
     */
    private long readVarUInt() throws Exception {
        int currentByte;
        int numberOfBytesRead = 0;
        long value = 0;
        while (numberOfBytesRead < MAXIMUM_SUPPORTED_VAR_UINT_BYTES) {
            currentByte = readByte();
            if (currentByte < 0) {
                return -1;
            }
            numberOfBytesRead++;
            value =
                (value << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
            if ((currentByte & HIGHEST_BIT_BITMASK) != 0) {
                dataHandler.onData(numberOfBytesRead);
                return value;
            }
        }
        throw new IonException("Found a VarUInt that was too large to fit in a `long`");
    }

    private void prohibitEmptyOrderedStruct() {
        if (valueTid.type == IonType.STRUCT &&
            valueTid.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            event == Event.END_CONTAINER // TODO check
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
    }

    protected boolean handleHeaderEnd() throws IOException {
        return false;
    }

    private void nextHeader() throws Exception {
        peekIndex = checkpoint;
        event = Event.NEEDS_DATA;
        while (true) {
            if (!makeBufferReady() || checkContainerEnd()) {
                return;
            }
            switch (checkpointLocation) {
                case BEFORE_UNANNOTATED_TYPE_ID:
                    fieldSid = -1;
                    if (isInStruct()) {
                        fieldSid = (int) readVarUInt(); // TODO type alignment
                        if (fieldSid < 0) {
                            return;
                        }
                    }
                    int b = readByte();
                    if (b < 0) {
                        return;
                    }
                    if (!parseTypeID(b, true)) {
                        return;
                    }
                    if (checkpointLocation == CheckpointLocation.AFTER_SCALAR_HEADER) {
                        return;
                    }
                    if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                        prohibitEmptyOrderedStruct();
                        return;
                    }
                    if (handleHeaderEnd()) { // TODO this isn't clean
                        // The value is being skipped; continue to the next one.
                        continue;
                    }
                    // Either an IVM or NOP has been skipped, or an annotation wrapper has been consumed.
                    continue;
                case BEFORE_ANNOTATED_TYPE_ID:
                    checkpoint = peekIndex; // TODO not necessary?
                    b = readByte();
                    if (b < 0) {
                        return;
                    }
                    if (!parseTypeID(b, false)) {
                        return;
                    }
                    if (checkpointLocation == CheckpointLocation.AFTER_CONTAINER_HEADER) {
                        prohibitEmptyOrderedStruct();
                    }
                    if (handleHeaderEnd()) {
                        // The value is being skipped; continue to the next one.
                        continue;
                    }
                    // If already within an annotation wrapper, neither an IVM nor a NOP is possible, so the lexer
                    // must be positioned after the header for the wrapped value.
                    return;
                case AFTER_SCALAR_HEADER:
                case AFTER_CONTAINER_HEADER: // TODO can we unify these two states?
                    // TODO redundant?
                    if (!buffer.seekTo(valueMarker.endIndex)) {
                        return;
                    }
                    peekIndex = valueMarker.endIndex;
                    setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
                    // The previous value's bytes have now been skipped; continue.
            }
        }

    }

    /**
     *
     * @return a marker for the buffered value, or null if the value is not yet completely buffered.
     * @throws Exception
     */
    private void fillValue() throws Exception {
        if (!makeBufferReady()) {
            return;
        }
        // Must be positioned on a scalar.
        if (checkpointLocation != CheckpointLocation.AFTER_SCALAR_HEADER && checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
            throw new IllegalStateException();
        }
        event = Event.NEEDS_DATA;

        // TODO does something like fillTo() make sense?
        if (buffer.fill(valueMarker.endIndex - valueMarker.startIndex)) {
            event = Event.VALUE_READY;
        }
    }

    public void stepIn() throws IOException {
        if (!makeBufferReady() || checkContainerEnd()) { // TODO check need for checkContainerEnd
            return;
        }
        // Must be positioned on a container.
        if (checkpointLocation != CheckpointLocation.AFTER_CONTAINER_HEADER) {
            throw new IonException("Must be positioned on a container to step in.");
        }
        // Push the remaining length onto the stack, seek past the container's header, and increase the depth.
        ContainerInfo containerInfo = containerStack.push();
        containerInfo.set(valueTid.type, valueMarker);
        setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
        // TODO seek past header (no need to hold onto it)
        // TODO reset other state
        valueTid = null;
        //fieldSid = -1;
        event = Event.NEEDS_INSTRUCTION;
    }

    public void stepOut() throws Exception {
        if (!makeBufferReady()) {
            return;
        }
        // Seek past the remaining bytes at this depth, pop from the stack, and subtract the number of bytes
        // consumed at the previous depth from the remaining bytes needed at the current depth.
        if (containerStack.isEmpty()) {
            // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        ContainerInfo containerInfo = containerStack.peek();
        event = Event.NEEDS_DATA;
        // Seek past any remaining bytes from the previous value.
        if (!buffer.seekTo(containerInfo.endIndex)) {
            return;
        }
        peekIndex = containerInfo.endIndex;
        setCheckpoint(CheckpointLocation.BEFORE_UNANNOTATED_TYPE_ID);
        containerStack.pop();
        event = Event.NEEDS_INSTRUCTION;
        // tODO reset other state (e.g. annotations?)
        valueTid = null;
        //fieldSid = -1;
    }

    private boolean makeBufferReady() {
        try {
            if (!buffer.makeReady()) {
                event = Event.NEEDS_DATA;
                return false;
            }
            // TODO now that the buffer's ready, what's the checkpoint location?
        } catch (Exception e) {
            throw new IonException(e);
        }
        return true;
    }

    @Override
    public Event next(Instruction instruction) {
        switch (instruction) {
            case STEP_IN:
                try {
                    stepIn();
                } catch (IOException e) {
                    throw new IonException(e);
                }
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
                } catch (RuntimeException e) {
                    throw e;
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
        // TODO
    }

    /*
    @Override
    public boolean moreDataRequired() { // TODO not needed?
        return false;
    }

    @Override
    protected void fillInputHelper() throws Exception { // TODO not needed?

    }

     */


    int ionMajorVersion() {
        return majorVersion;
    }

    int ionMinorVersion() {
        return minorVersion;
    }

    /**
     * Clears the NOP pad index. Should be called between user values.
     */
    /*
    void resetNopPadIndex() {
        nopPadStartIndex = -1;
    }

     */

    /**
     * @return the index of the first byte of the value representation (past the type ID and the optional length field).
     */
    /*
    int getValueStart() {
        if (hasAnnotations()) {
            return annotationSidsMarker.endIndex;
        }
        return valuePostHeaderIndex;
    }

     */

    /**
     * @return the type ID of the current value.
     */
    IonTypeID getValueTid() {
        return valueTid;
    }

    /**
     * @return the index of the first byte after the end of the current value.
     */
    //int getValueEnd() {
    //    return valueEndIndex;
    //}


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

    boolean isAwaitingMoreData() {
        // TODO still probably not quite right, check
        return peekIndex > checkpoint; //!buffer.isReady();
    }
}
