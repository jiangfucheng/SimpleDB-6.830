package simpledb;
import java.io.Serializable;
import java.util.*;

/**
 * OpIterator is the tupleIterator interface that all SimpleDB operators should
 * implement. If the tupleIterator is not open, none of the methods should work,
 * and should throw an IllegalStateException.  In addition to any
 * resource allocation/deallocation, an open method should call any
 * child tupleIterator open methods, and in a close method, an tupleIterator
 * should call its children's close methods.
 */
public interface OpIterator extends Serializable{
  /**
   * Opens the tupleIterator. This must be called before any of the other methods.
   * @throws DbException when there are problems opening/accessing the database.
   */
  public void open()
      throws DbException, TransactionAbortedException;

  /** Returns true if the tupleIterator has more tuples.
   * @return true f the tupleIterator has more tuples.
   * @throws IllegalStateException If the tupleIterator has not been opened
 */
  public boolean hasNext() throws DbException, TransactionAbortedException;

  /**
   * Returns the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return the next tuple in the iteration.
   * @throws NoSuchElementException if there are no more tuples.
   * @throws IllegalStateException If the tupleIterator has not been opened
   */
  public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

  /**
   * Resets the tupleIterator to the start.
   * @throws DbException when rewind is unsupported.
   * @throws IllegalStateException If the tupleIterator has not been opened
   */
  public void rewind() throws DbException, TransactionAbortedException;

  /**
   * Returns the TupleDesc associated with this OpIterator.
   * @return the TupleDesc associated with this OpIterator.
   */
  public TupleDesc getTupleDesc();

  /**
   * Closes the tupleIterator. When the tupleIterator is closed, calling next(),
   * hasNext(), or rewind() should fail by throwing IllegalStateException.
   */
  public void close();

}
