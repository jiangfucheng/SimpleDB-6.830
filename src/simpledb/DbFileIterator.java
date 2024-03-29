package simpledb;

import java.util.*;

/**
 * DbFileIterator is the tupleIterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator {
    /**
     * Opens the tupleIterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    void open()
            throws DbException, TransactionAbortedException;

    /**
     * @return true if there are more tuples available, false if no more tuples or tupleIterator isn't open.
     */
    boolean hasNext()
            throws DbException, TransactionAbortedException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the tupleIterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the tupleIterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    void rewind() throws DbException, TransactionAbortedException;

    /**
     * Closes the tupleIterator.
     */
    void close();
}
