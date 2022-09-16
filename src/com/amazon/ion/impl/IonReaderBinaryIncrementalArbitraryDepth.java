package com.amazon.ion.impl;

import com.amazon.ion.BufferConfiguration;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonReaderReentrantApplication;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This implementation differs from the existing non-incremental binary reader implementation in that if
 * {@link IonReader#next()} returns {@code null} at the top-level, it indicates that there is not (yet) enough data in
 * the stream to complete a top-level value. The user may wait for more data to become available in the stream and
 * call {@link IonReader#next()} again to continue reading. Unlike the non-incremental reader, the incremental reader
 * will never throw an exception due to unexpected EOF during {@code next()}. If, however, {@link IonReader#close()} is
 * called when an incomplete value is buffered, an {@link IonException} will be raised.
 * </p>
 * <p>
 * Although the incremental binary reader implementation provides performance superior to the non-incremental reader
 * implementation for both incremental and non-incremental use cases, there is one caveat: the incremental
 * implementation must be able to buffer an entire top-level value and any preceding system values (Ion version
 * marker(s) and symbol table(s)) in memory. This means that each value and preceding system values must be no larger
 * than any of the following:
 * <ul>
 * <li>The configured maximum buffer size of the {@link IonBufferConfiguration}.</li>
 * <li>The memory available to the JVM.</li>
 * <li>2GB, because the buffer is held in a Java {@code byte[]}, which is indexed by an {@code int}.</li>
 * </ul>
 * This will not be a problem for the vast majority of Ion streams, as it is
 * rare for a single top-level value or symbol table to exceed a few megabytes in size. However, if the size of the
 * stream's values risk exceeding the available memory, then this implementation must not be used.
 * </p>
 * <p>
 * To enable this implementation, use {@code IonReaderBuilder.withIncrementalReadingEnabled(true)}.
 * </p>
 */
class IonReaderBinaryIncrementalArbitraryDepth extends IonReaderBinaryIncrementalArbitraryDepthRaw implements IonReaderReentrantApplication {

    // Symbol IDs for symbols contained in the system symbol table.
    private static class SystemSymbolIDs {

        // The system symbol table SID for the text "$ion_symbol_table".
        private static final int ION_SYMBOL_TABLE_ID = 3;

        // The system symbol table SID for the text "name".
        private static final int NAME_ID = 4;

        // The system symbol table SID for the text "version".
        private static final int VERSION_ID = 5;

        // The system symbol table SID for the text "imports".
        private static final int IMPORTS_ID = 6;

        // The system symbol table SID for the text "symbols".
        private static final int SYMBOLS_ID = 7;

        // The system symbol table SID for the text "max_id".
        private static final int MAX_ID_ID = 8;
    }

    // An IonCatalog containing zero shared symbol tables.
    private static final IonCatalog EMPTY_CATALOG = new SimpleCatalog();

    // Initial capacity of the ArrayList used to hold the text in the current symbol table.
    private static final int SYMBOLS_LIST_INITIAL_CAPACITY = 128;

    // The imports for Ion 1.0 data with no shared user imports.
    private static final LocalSymbolTableImports ION_1_0_IMPORTS
        = new LocalSymbolTableImports(SharedSymbolTable.getSystemSymbolTable(1));

    // True if the annotation iterator will be reused across values; otherwise, false.
    private final boolean isAnnotationIteratorReuseEnabled;

    // Reusable iterator over the annotations on the current value.
    private final AnnotationIterator annotationIterator;

    // The text representations of the symbol table that is currently in scope, indexed by symbol ID. If the element at
    // a particular index is null, that symbol has unknown text.
    private final List<String> symbols;

    // The catalog used by the reader to resolve shared symbol table imports.
    private final IonCatalog catalog;

    private final SymbolTableReader symbolTableReader;

    // The shared symbol tables imported by the local symbol table that is currently in scope.
    private LocalSymbolTableImports imports = ION_1_0_IMPORTS;

    // A map of symbol ID to SymbolToken representation. Because most use cases only require symbol text, this
    // is used only if necessary to avoid imposing the extra expense on all symbol lookups.
    private List<SymbolToken> symbolTokensById = null;

    // The cached SymbolTable representation of the current local symbol table. Invalidated whenever a local
    // symbol table is encountered in the stream.
    private SymbolTable cachedReadOnlySymbolTable = null;

    // The SymbolTable that was transferred via the last call to pop_passed_symbol_table.
    private SymbolTable symbolTableLastTransferred = null;

    private final IonBinaryLexerBase.IvmNotificationConsumer ivmNotificationConsumer = new IonBinaryLexerRefillable.IvmNotificationConsumer() {
        @Override
        public void ivmEncountered(int majorVersion, int minorVersion) {
            // TODO use the versions to set the proper system symbol table and local symbol table processing
            //  logic.
            resetSymbolTable();
            resetImports();
        }
    };

    // ------

    /**
     * Constructor.
     * @param builder the builder containing the configuration for the new reader.
     * @param buffer the buffer that provides binary Ion data.
     */
    IonReaderBinaryIncrementalArbitraryDepth(IonReaderBuilder builder, IonBinaryLexerBase lexer) {
        super(lexer);
        this.catalog = builder.getCatalog() == null ? EMPTY_CATALOG : builder.getCatalog();
        if (builder.isAnnotationIteratorReuseEnabled()) {
            isAnnotationIteratorReuseEnabled = true;
            annotationIterator = new AnnotationIterator();
        } else {
            isAnnotationIteratorReuseEnabled = false;
            annotationIterator = null;
        }
        symbols = new ArrayList<String>(SYMBOLS_LIST_INITIAL_CAPACITY);
        symbolTableReader = new SymbolTableReader();
        resetImports();
        lexer.registerIvmNotificationConsumer(ivmNotificationConsumer);
    }

    /**
     * Constructor.
     * @param builder the builder containing the configuration for the new reader.
     * @param buffer the buffer that provides binary Ion data.
     */
    IonReaderBinaryIncrementalArbitraryDepth(final IonReaderBuilder builder, final IonBinaryLexerRefillable lexer) {
        super(lexer);
        this.catalog = builder.getCatalog() == null ? EMPTY_CATALOG : builder.getCatalog();
        if (builder.isAnnotationIteratorReuseEnabled()) {
            isAnnotationIteratorReuseEnabled = true;
            annotationIterator = new AnnotationIterator();
        } else {
            isAnnotationIteratorReuseEnabled = false;
            annotationIterator = null;
        }
        symbols = new ArrayList<String>(SYMBOLS_LIST_INITIAL_CAPACITY);
        symbolTableReader = new SymbolTableReader();
        resetImports();
        final IonBufferConfiguration configuration = (IonBufferConfiguration) lexer.getConfiguration();
        lexer.registerIvmNotificationConsumer(ivmNotificationConsumer);
        lexer.registerOversizedValueHandler(
            new BufferConfiguration.OversizedValueHandler() {
                @Override
                public void onOversizedValue() {
                    if (isReadingSymbolTable() || isPositionedOnSymbolTable()) {
                        configuration.getOversizedSymbolTableHandler().onOversizedSymbolTable();
                        lexer.terminate();
                    } else {
                        configuration.getOversizedValueHandler().onOversizedValue();
                    }
                }
            }
        );
    }

    /**
     * Reusable iterator over the annotations on the current value.
     */
    private class AnnotationIterator implements Iterator<String> {

        private IonReaderBinaryIncrementalArbitraryDepthRaw.AnnotationIterator sidIterator;

        @Override
        public boolean hasNext() {
            return sidIterator.hasNext();
        }

        @Override
        public String next() {
            int sid = sidIterator.next();
            String annotation = getSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            return annotation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support element removal.");
        }

        /**
         * Prepare the iterator to iterate over the annotations on the current value.
         */
        void ready() {
            sidIterator = iterateAnnotationSids();
            sidIterator.ready();
        }

        /**
         * Invalidate the iterator so that all future calls to {@link #hasNext()} will return false until the
         * next call to {@link #ready()}.
         */
        void invalidate() {
            if (sidIterator != null) {
                sidIterator.invalidate();
            }
        }
    }

    /**
     * Non-reusable iterator over the annotations on the current value. May be iterated even if the reader advances
     * past the current value.
     */
    private class SingleUseAnnotationIterator implements Iterator<String> {

        // All of the annotation SIDs on the current value.
        private final IntList annotationSids;
        // The index into `annotationSids` containing the next annotation to be returned.
        private int index = 0;

        SingleUseAnnotationIterator() {
            annotationSids = new IntList(getAnnotationSids());
        }

        @Override
        public boolean hasNext() {
            return index < annotationSids.size();
        }

        @Override
        public String next() {
            int sid = annotationSids.get(index);
            String annotation = getSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            index++;
            return annotation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support element removal.");
        }
    }

    /**
     * SymbolToken implementation that includes ImportLocation.
     */
    static class SymbolTokenImpl implements _Private_SymbolToken {

        // The symbol's text, or null if the text is unknown.
        private final String text;

        // The local symbol ID of this symbol within a particular local symbol table.
        private final int sid;

        // The import location of the symbol (only relevant if the text is unknown).
        private final ImportLocation importLocation;

        SymbolTokenImpl(String text, int sid, ImportLocation importLocation) {
            this.text = text;
            this.sid = sid;
            this.importLocation = importLocation;
        }

        @Override
        public String getText() {
            return text;
        }

        @Override
        public String assumeText() {
            if (text == null) {
                throw new UnknownSymbolException(sid);
            }
            return text;
        }

        @Override
        public int getSid() {
            return sid;
        }

        // Will be @Override once added to the SymbolToken interface.
        public ImportLocation getImportLocation() {
            return importLocation;
        }

        @Override
        public String toString() {
            return String.format("SymbolToken::{text: %s, sid: %d, importLocation: %s}", text, sid, importLocation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SymbolToken)) return false;

            // NOTE: once ImportLocation is available via the SymbolToken interface, it should be compared here
            // when text is null.
            SymbolToken other = (SymbolToken) o;
            if(getText() == null || other.getText() == null) {
                return getText() == other.getText();
            }
            return getText().equals(other.getText());
        }

        @Override
        public int hashCode() {
            if(getText() != null) return getText().hashCode();
            return 0;
        }
    }

    /**
     * Gets the system symbol table for the Ion version currently active.
     * @return a system SymbolTable.
     */
    private SymbolTable getSystemSymbolTable() {
        // Note: Ion 1.1 currently proposes changes to the system symbol table. If this is finalized, then
        // 'majorVersion' cannot be used to look up the system symbol table; both 'majorVersion' and 'minorVersion'
        // will need to be used.
        return SharedSymbolTable.getSystemSymbolTable(ionMajorVersion());
    }

    /**
     * Read-only snapshot of the local symbol table at the reader's current position.
     */
    private class LocalSymbolTableSnapshot implements SymbolTable, SymbolTableAsStruct {

        // The system symbol table.
        private final SymbolTable system = IonReaderBinaryIncrementalArbitraryDepth.this.getSystemSymbolTable();

        // The max ID of this local symbol table.
        private final int maxId;

        // The shared symbol tables imported by this local symbol table.
        private final LocalSymbolTableImports importedTables;

        // Map representation of this symbol table. Keys are symbol text; values are the lowest symbol ID that maps
        // to that text.
        final Map<String, Integer> mapView;

        // List representation of this symbol table, indexed by symbol ID.
        final List<String> listView;

        private SymbolTableStructCache structCache = null;

        LocalSymbolTableSnapshot() {
            int importsMaxId = imports.getMaxId();
            int numberOfLocalSymbols = symbols.size();
            // Note: 'imports' is immutable, so a clone is not needed.
            importedTables = imports;
            maxId = importsMaxId + numberOfLocalSymbols;
            // Map with initial size the number of symbols and load factor 1, meaning it must be full before growing.
            // It is not expected to grow.
            listView = new ArrayList<String>(symbols.subList(0, numberOfLocalSymbols));
            mapView = new HashMap<String, Integer>((int) Math.ceil(numberOfLocalSymbols / 0.75), 0.75f);
            for (int i = 0; i < numberOfLocalSymbols; i++) {
                String symbol = listView.get(i);
                if (symbol != null) {
                    mapView.put(symbol, i + importsMaxId + 1);
                }
            }
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public boolean isLocalTable() {
            return true;
        }

        @Override
        public boolean isSharedTable() {
            return false;
        }

        @Override
        public boolean isSubstitute() {
            return false;
        }

        @Override
        public boolean isSystemTable() {
            return false;
        }

        @Override
        public SymbolTable getSystemSymbolTable() {
            return system;
        }

        @Override
        public String getIonVersionId() {
            return system.getIonVersionId();
        }

        @Override
        public SymbolTable[] getImportedTables() {
            return importedTables.getImportedTables();
        }

        @Override
        public int getImportedMaxId() {
            return importedTables.getMaxId();
        }

        @Override
        public SymbolToken find(String text) {
            SymbolToken token = importedTables.find(text);
            if (token != null) {
                return token;
            }
            Integer sid = mapView.get(text);
            if (sid == null) {
                return null;
            }
            // The following per-call allocation is intentional. When weighed against the alternative of making
            // 'mapView' a 'Map<String, SymbolToken>` instead of a `Map<String, Integer>`, the following points should
            // be considered:
            // 1. A LocalSymbolTableSnapshot is only created when getSymbolTable() is called on the reader. The reader
            // does not use the LocalSymbolTableSnapshot internally. There are two cases when getSymbolTable() would be
            // called: a) when the user calls it, which will basically never happen, and b) when the user uses
            // IonSystem.iterate over the reader, in which case each top-level value holds a reference to the symbol
            // table that was in scope when it occurred. In case a), in addition to rarely being called at all, it
            // would be even rarer for a user to use find() to retrieve each symbol (especially more than once) from the
            // returned symbol table. Case b) may be called more frequently, but it remains equally rare that a user
            // would retrieve each symbol at least once.
            // 2. If we make mapView a Map<String, SymbolToken>, then we are guaranteeing that we will allocate at least
            // one SymbolToken per symbol (because mapView is created in the constructor of LocalSymbolTableSnapshot)
            // even though it's unlikely most will ever be needed.
            return new SymbolTokenImpl(text, sid, null);
        }

        @Override
        public int findSymbol(String name) {
            Integer sid = importedTables.findSymbol(name);
            if (sid > UNKNOWN_SYMBOL_ID) {
                return sid;
            }
            sid = mapView.get(name);
            if (sid == null) {
                return UNKNOWN_SYMBOL_ID;
            }
            return sid;
        }

        @Override
        public String findKnownSymbol(int id) {
            if (id < 0) {
                throw new IllegalArgumentException("Symbol IDs must be at least 0.");
            }
            if (id > getMaxId()) {
                return null;
            }
            return IonReaderBinaryIncrementalArbitraryDepth.this.getSymbolString(id, importedTables, listView);
        }

        @Override
        public Iterator<String> iterateDeclaredSymbolNames() {
            return new Iterator<String>() {

                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < listView.size();
                }

                @Override
                public String next() {
                    String symbol = listView.get(index);
                    index++;
                    return symbol;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("This iterator does not support element removal.");
                }
            };
        }

        @Override
        public SymbolToken intern(String text) {
            SymbolToken token = find(text);
            if (token != null) {
                return token;
            }
            throw new ReadOnlyValueException();
        }

        @Override
        public int getMaxId() {
            return maxId;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public void makeReadOnly() {
            // The symbol table is already read-only.
        }

        @Override
        public void writeTo(IonWriter writer) throws IOException {
            IonReader reader = new com.amazon.ion.impl.SymbolTableReader(this);
            writer.writeValues(reader);
        }

        @Override
        public String toString() {
            return "(LocalSymbolTable max_id:" + getMaxId() + ')';
        }

        @Override
        public IonStruct getIonRepresentation(ValueFactory valueFactory) {
            if (structCache == null) {
                structCache = new SymbolTableStructCache(this, getImportedTables(), null);
            }
            return structCache.getIonRepresentation(valueFactory);
        }
    }

    /**
     * Reset the local symbol table to the system symbol table.
     */
    private void resetSymbolTable() {
        // Note: when there is a new version of Ion, check majorVersion and minorVersion here and set the appropriate
        // system symbol table.
        symbols.clear();
        cachedReadOnlySymbolTable = null;
        if (symbolTokensById != null) {
            symbolTokensById.clear();
        }
    }

    /**
     * Resets the value's annotations.
     */
    private void resetAnnotations() {
        if (isAnnotationIteratorReuseEnabled) {
            annotationIterator.invalidate();
        }
    }

    /**
     * Clear the list of imported shared symbol tables.
     */
    private void resetImports() {
        // Note: when support for the next version of Ion is added, conditionals on 'majorVersion' and 'minorVersion'
        // must be added here.
        imports = ION_1_0_IMPORTS;
    }

    /**
     * Creates a shared symbol table import, resolving it from the catalog if possible.
     * @param name the name of the shared symbol table.
     * @param version the version of the shared symbol table.
     * @param maxId the max_id of the shared symbol table. This value takes precedence over the actual max_id for the
     *              shared symbol table at the requested version.
     */
    private SymbolTable createImport(String name, int version, int maxId) {
        SymbolTable shared = catalog.getTable(name, version);
        if (shared == null) {
            // No match. All symbol IDs that fall within this shared symbol table's range will have unknown text.
            return new SubstituteSymbolTable(name, version, maxId);
        } else if (shared.getMaxId() != maxId || shared.getVersion() != version) {
            // Partial match. If the requested max_id exceeds the actual max_id of the resolved shared symbol table,
            // symbol IDs that exceed the max_id of the resolved shared symbol table will have unknown text.
            return new SubstituteSymbolTable(shared, version, maxId);
        } else {
            // Exact match; the resolved shared symbol table may be used as-is.
            return shared;
        }
    }

    /**
     * Gets the String representation of the given symbol ID. It is the caller's responsibility to ensure that the
     * given symbol ID is within the max ID of the symbol table.
     * @param sid the symbol ID.
     * @param importedSymbols the symbol table's shared symbol table imports.
     * @param localSymbols the symbol table's local symbols.
     * @return a String, which will be null if the requested symbol ID has undefined text.
     */
    private String getSymbolString(int sid, LocalSymbolTableImports importedSymbols, List<String> localSymbols) {
        if (sid <= importedSymbols.getMaxId()) {
            return importedSymbols.findKnownSymbol(sid);
        }
        return localSymbols.get(sid - (importedSymbols.getMaxId() + 1));
    }

    /**
     * Calculates the symbol table's max ID.
     * @return the max ID.
     */
    private int maxSymbolId() {
        return symbols.size() + imports.getMaxId();
    }

    /**
     * Retrieves the String text for the given symbol ID.
     * @param sid a symbol ID.
     * @return a String.
     */
    private String getSymbol(int sid) {
        if (sid > maxSymbolId()) {
            throw new IonException("Symbol ID exceeds the max ID of the symbol table.");
        }
        return getSymbolString(sid, imports, symbols);
    }

    /**
     * Creates a SymbolToken representation of the given symbol ID.
     * @param sid a symbol ID.
     * @return a SymbolToken.
     */
    private SymbolToken getSymbolToken(int sid) {
        int symbolTableSize = maxSymbolId() + 1;
        if (symbolTokensById == null) {
            symbolTokensById = new ArrayList<SymbolToken>(symbolTableSize);
        }
        if (symbolTokensById.size() < symbolTableSize) {
            for (int i = symbolTokensById.size(); i < symbolTableSize; i++) {
                symbolTokensById.add(null);
            }
        }
        if (sid >= symbolTableSize) {
            throw new IonException("Symbol ID exceeds the max ID of the symbol table.");
        }
        SymbolToken token = symbolTokensById.get(sid);
        if (token == null) {
            String text = getSymbolString(sid, imports, symbols);
            ImportLocation importLocation = null;
            if (text == null) {
                // Note: this will never be a system symbol.
                if (sid > 0 && sid <= imports.getMaxId()) {
                    importLocation = imports.getImportLocation(sid);
                } else {
                    // All symbols with unknown text in the local symbol range are equivalent to symbol zero.
                    sid = 0;
                }
            }
            token = new SymbolTokenImpl(text, sid, importLocation);
            symbolTokensById.set(sid, token);
        }
        return token;
    }

    private class SymbolTableReader {

        private boolean hasSeenImports;
        private boolean hasSeenSymbols;
        private boolean isAppend;

        private String name = null;
        private int version = -1;
        private int maxId = -1;

        private List<SymbolTable> newImports = null;
        private List<String> newSymbols = null;

        private void resetState() {
            hasSeenImports = false;
            hasSeenSymbols = false;
            isAppend = false;
            newImports = null;
            newSymbols = null;
            resetImportInfo();
        }

        private void resetImportInfo() {
            name = null;
            version = -1;
            maxId = -1;
        }

        private boolean valueUnavailable() throws IOException {
            Event event = fillValue();
            return event == Event.NEEDS_DATA || event == Event.NEEDS_INSTRUCTION;
        }

        private void finishReadingSymbolTableStruct() throws IOException {
            stepOutOfContainer();
            if (!hasSeenImports) {
                resetSymbolTable();
                resetImports();
            }
            if (newSymbols != null) {
                symbols.addAll(newSymbols);
            }
            state = State.READING_VALUE;
        }

        private void readSymbolTableStructField() {
            int fieldId = getFieldId();
            if (fieldId == SystemSymbolIDs.SYMBOLS_ID) {
                state = State.ON_SYMBOL_TABLE_SYMBOLS;
                if (hasSeenSymbols) {
                    throw new IonException("Symbol table contained multiple symbols fields.");
                }
                hasSeenSymbols = true;
            } else if (fieldId == SystemSymbolIDs.IMPORTS_ID) {
                state = State.ON_SYMBOL_TABLE_IMPORTS;
                if (hasSeenImports) {
                    throw new IonException("Symbol table contained multiple imports fields.");
                }
                hasSeenImports = true;
            }
        }

        private void startReadingImportsList() {
            resetImports();
            resetSymbolTable();
            newImports = new ArrayList<SymbolTable>(3);
            newImports.add(getSystemSymbolTable());
            state = State.READING_SYMBOL_TABLE_IMPORTS_LIST;
        }

        private void preparePossibleAppend() {
            if (symbolValueId() == SystemSymbolIDs.ION_SYMBOL_TABLE_ID) {
                isAppend = true;
            } else {
                resetSymbolTable();
            }
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void finishReadingImportsList() throws IOException {
            stepOutOfContainer();
            imports = new LocalSymbolTableImports(newImports);
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingSymbolsList() {
            newSymbols = new ArrayList<String>(8);
            state = State.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void startReadingSymbol() {
            if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.STRING) {
                state = State.READING_SYMBOL_TABLE_SYMBOL;
            } else {
                newSymbols.add(null);
            }
        }

        private void finishReadingSymbol() {
            newSymbols.add(stringValue());
            state = State.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void finishReadingSymbolsList() throws IOException {
            stepOutOfContainer();
            state = State.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingImportStruct() throws IOException {
            resetImportInfo();
            if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.STRUCT) {
                stepIntoContainer();
                state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
            }
        }

        private void finishReadingImportStruct() throws IOException {
            stepOutOfContainer();
            newImports.add(createImport(name, version, maxId));
            state = State.READING_SYMBOL_TABLE_IMPORTS_LIST;
        }

        private void startReadingImportStructField() {
            int fieldId = getFieldId();
            if (fieldId == SystemSymbolIDs.NAME_ID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_NAME;
            } else if (fieldId == SystemSymbolIDs.VERSION_ID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_VERSION;
            } else if (fieldId == SystemSymbolIDs.MAX_ID_ID) {
                state = State.READING_SYMBOL_TABLE_IMPORT_MAX_ID;
            }
        }

        private void readImportName() {
            if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.STRING) {
                name = stringValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportVersion() {
            if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.INT) {
                version = intValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportMaxId() {
            if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.INT) {
                maxId = intValue();
            }
            state = State.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        void readSymbolTable() throws IOException {
            Event event;
            while (true) {
                switch (state) {
                    case ON_SYMBOL_TABLE_STRUCT:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            return;
                        }
                        state = State.ON_SYMBOL_TABLE_FIELD;
                        break;
                    case ON_SYMBOL_TABLE_FIELD:
                        event = IonReaderBinaryIncrementalArbitraryDepth.super.nextValue();
                        if (Event.NEEDS_DATA == event) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolTableStruct();
                            return;
                        }
                        readSymbolTableStructField();
                        break;
                    case ON_SYMBOL_TABLE_SYMBOLS:
                        if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                return;
                            }
                            startReadingSymbolsList();
                        } else {
                            state = State.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case ON_SYMBOL_TABLE_IMPORTS:
                        if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                return;
                            }
                            startReadingImportsList();
                        } else if (IonReaderBinaryIncrementalArbitraryDepth.super.getType() == IonType.SYMBOL) {
                            if (valueUnavailable()) {
                                return;
                            }
                            preparePossibleAppend();
                        } else {
                            state = State.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case READING_SYMBOL_TABLE_SYMBOLS_LIST:
                        event = IonReaderBinaryIncrementalArbitraryDepth.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolsList();
                            break;
                        }
                        startReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_SYMBOL:
                        if (valueUnavailable()) {
                            return;
                        }
                        finishReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_IMPORTS_LIST:
                        event = IonReaderBinaryIncrementalArbitraryDepth.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportsList();
                            break;
                        }
                        startReadingImportStruct();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_STRUCT:
                        event = IonReaderBinaryIncrementalArbitraryDepth.super.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportStruct();
                            break;
                        } else if (event != Event.START_SCALAR) {
                            break;
                        }
                        startReadingImportStructField();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_NAME:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportName();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_VERSION:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportVersion();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_MAX_ID:
                        if (valueUnavailable()) {
                            return;
                        }
                        readImportMaxId();
                        break;
                }
            }
        }
    }

    private enum State {
        ON_SYMBOL_TABLE_STRUCT,
        ON_SYMBOL_TABLE_FIELD,
        ON_SYMBOL_TABLE_SYMBOLS,
        READING_SYMBOL_TABLE_SYMBOLS_LIST,
        READING_SYMBOL_TABLE_SYMBOL,
        ON_SYMBOL_TABLE_IMPORTS,
        READING_SYMBOL_TABLE_IMPORTS_LIST,
        READING_SYMBOL_TABLE_IMPORT_STRUCT,
        READING_SYMBOL_TABLE_IMPORT_NAME,
        READING_SYMBOL_TABLE_IMPORT_VERSION,
        READING_SYMBOL_TABLE_IMPORT_MAX_ID,
        READING_VALUE
    }

    private State state = State.READING_VALUE;

    private boolean isReadingSymbolTable() {
        return state != State.READING_VALUE;
    }

    private boolean isPositionedOnSymbolTable() {
        return containerStack.isEmpty() &&
            hasAnnotations() &&
            peekType() == IonType.STRUCT &&
            iterateAnnotationSids().next() == SystemSymbolIDs.ION_SYMBOL_TABLE_ID;
    }

    @Override
    public Event nextValue() throws IOException {
        Event event;
        if (containerStack.isEmpty() || isReadingSymbolTable()) {
            while (true) {
                if (isReadingSymbolTable()) {
                    symbolTableReader.readSymbolTable();
                    if (state != State.READING_VALUE) {
                        event = Event.NEEDS_DATA;
                        break;
                    }
                }
                event = super.nextValue();
                if (isPositionedOnSymbolTable()) {
                    cachedReadOnlySymbolTable = null;
                    symbolTableReader.resetState();
                    state = State.ON_SYMBOL_TABLE_STRUCT;
                    continue;
                }
                break;
            }
        } else {
            event = super.nextValue();
        }
        resetAnnotations();
        return event;
    }

    @Override
    public SymbolTable getSymbolTable() {
        if (cachedReadOnlySymbolTable == null) {
            if (symbols.size() == 0 && imports == ION_1_0_IMPORTS) {
                cachedReadOnlySymbolTable = imports.getSystemSymbolTable();
            } else {
                cachedReadOnlySymbolTable = new LocalSymbolTableSnapshot();
            }
        }
        return cachedReadOnlySymbolTable;
    }

    public SymbolTable pop_passed_symbol_table() {
        SymbolTable currentSymbolTable = getSymbolTable();
        if (currentSymbolTable == symbolTableLastTransferred) {
            // This symbol table has already been returned. Since the contract is that it is a "pop", it should not
            // be returned twice.
            return null;
        }
        symbolTableLastTransferred = currentSymbolTable;
        return symbolTableLastTransferred;
    }

    @Override
    public String stringValue() {
        String value;
        IonType type = super.getType();
        if (type == IonType.STRING) {
            value = super.stringValue();
        } else if (type == IonType.SYMBOL) {
            int sid = symbolValueId();
            if (sid < 0) {
                // The raw reader uses this to denote null.symbol.
                return null;
            }
            value = getSymbol(sid);
            if (value == null) {
                throw new UnknownSymbolException(sid);
            }
        } else {
            throw new IllegalStateException("Invalid type requested.");
        }
        return value;
    }

    @Override
    public SymbolToken symbolValue() {
        int sid = symbolValueId();
        if (sid < 0) {
            // The raw reader uses this to denote null.symbol.
            return null;
        }
        return getSymbolToken(sid);
    }

    @Override
    public String[] getTypeAnnotations() {
        if (hasAnnotations()) {
            IntList annotationSids = getAnnotationSids();
            String[] annotationArray = new String[annotationSids.size()];
            for (int i = 0; i < annotationArray.length; i++) {
                String symbol = getSymbol(annotationSids.get(i));
                if (symbol == null) {
                    throw new UnknownSymbolException(annotationSids.get(i));
                }
                annotationArray[i] = symbol;
            }
            return annotationArray;
        }
        return _Private_Utils.EMPTY_STRING_ARRAY;
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        if (hasAnnotations()) {
            IntList annotationSids = getAnnotationSids();
            SymbolToken[] annotationArray = new SymbolToken[annotationSids.size()];
            for (int i = 0; i < annotationArray.length; i++) {
                annotationArray[i] = getSymbolToken(annotationSids.get(i));
            }
            return annotationArray;
        }
        return SymbolToken.EMPTY_ARRAY;
    }

    private static final Iterator<String> EMPTY_ITERATOR = new Iterator<String>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove from an empty iterator.");
        }
    };

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        if (hasAnnotations()) {
            if (isAnnotationIteratorReuseEnabled) {
                annotationIterator.ready();
                return annotationIterator;
            } else {
                return new SingleUseAnnotationIterator();
            }
        }
        return EMPTY_ITERATOR;
    }

    @Override
    public String getFieldName() {
        int fieldNameSid = getFieldId();
        if (fieldNameSid < 0) {
            return null;
        }
        String fieldName = getSymbol(fieldNameSid);
        if (fieldName == null) {
            throw new UnknownSymbolException(fieldNameSid);
        }
        return fieldName;
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        int fieldNameSid = getFieldId();
        if (fieldNameSid < 0) {
            return null;
        }
        return getSymbolToken(fieldNameSid);
    }

}
