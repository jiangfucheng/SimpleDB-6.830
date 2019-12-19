package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;

    private OpIterator child;

    private TupleDesc resultTupleDesc;

    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.child = child;
        Type[] types = new Type[]{Type.INT_TYPE};
        this.resultTupleDesc = new TupleDesc(types);
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        return this.resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        this.called = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        if (called) return null;
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(transactionId, child.next());
                count++;
            } catch (IOException e) {
                throw new DbException(e.getMessage());
            }
        }
        called = true;
        Tuple result = new Tuple(resultTupleDesc);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
