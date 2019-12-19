package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private class LockManager {
        private Map<PageId, Set<TransactionId>> readMap;
        private Map<PageId, TransactionId> writeMap;

        public LockManager() {
            this.readMap = new HashMap<>();
            this.writeMap = new HashMap<>();
        }

        public synchronized void lock(TransactionId tid, PageId pid, LockType lockType) throws TransactionAbortedException {
            initReadSet(pid);
            if (lockType == LockType.SHARE) {
                while (writeMap.containsKey(pid) && !writeMap.get(pid).equals(tid)) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Set<TransactionId> readSet = readMap.get(pid);
				/*if (readSet.contains(tid))
					throw new TransactionAbortedException();*/
                readSet.add(tid);
            } else {
                Set<TransactionId> readTids = readMap.get(pid);
                if (readTids.size() == 1)
                    readTids.remove(tid);
                while (readMap.get(pid).size() > 0 || (writeMap.containsKey(pid) && !writeMap.get(pid).equals(tid))) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                writeMap.put(pid, tid);
            }
        }

        private void initReadSet(PageId pid) {
            if (!readMap.containsKey(pid))
                readMap.put(pid, new HashSet<>());
        }

        public synchronized void release(TransactionId tid, PageId pid) {
            initReadSet(pid);
            TransactionId writeMapTid = writeMap.get(pid);
            if (writeMapTid != null && writeMapTid.equals(tid))
                writeMap.remove(pid);

            Set<TransactionId> readMapTids = readMap.get(pid);
            readMapTids.remove(tid);
        }

        public synchronized void releaseLockByTransactionId(TransactionId tid) {
            for (Map.Entry<PageId, Set<TransactionId>> entry : readMap.entrySet()) {
                entry.getValue().removeIf(transactionId -> transactionId.equals(tid));
            }

            for (Iterator<Map.Entry<PageId, TransactionId>> iterator = writeMap.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<PageId, TransactionId> entry = iterator.next();
                if (entry.getValue().equals(tid)) {
                    iterator.remove();
                }
            }
            this.notifyAll();
        }

        public synchronized boolean holdLock(TransactionId tid, PageId pid) {
            initReadSet(pid);
            TransactionId writeMapTid = writeMap.get(pid);
            if (writeMapTid != null) {
                return tid.equals(writeMapTid);
            }
            Set<TransactionId> readMapTids = readMap.get(pid);
            return readMapTids.contains(tid);
        }

    }

    private enum LockType {
        SHARE, EXCLUSIVE
    }

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the capacity argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    //BufferPool 允许缓存的页面数量
    private int capacity;

    //页面缓冲池
    private Map<PageId, Page> pagePool;

    private LockManager lockManager;

    private ConcurrentHashMap<TransactionId, ConcurrentLinkedQueue<Page>> backupPage;


    /**
     * Creates a BufferPool that caches up to capacity pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.capacity = numPages;
        this.pagePool = new LinkedHashMap<>();
        this.lockManager = new LockManager();
        this.backupPage = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        LockType lockType = perm == Permissions.READ_ONLY ? LockType.SHARE : LockType.EXCLUSIVE;
        lockManager.lock(tid, pid, lockType);
        Page res = pagePool.get(pid);
        if (res != null) {
            pagePool.put(pid, res);
            return res;
        }
        if (pagePool.size() == capacity) evictPage();
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        res = dbFile.readPage(pid);
        pagePool.put(pid, res);
        //备份这个页面
        res.setBeforeImage();
        if (!backupPage.containsKey(tid)) {
            backupPage.put(tid, new ConcurrentLinkedQueue<>());
        }
        ConcurrentLinkedQueue<Page> pageQueue = backupPage.get(tid);
        pageQueue.offer(res);
        return res;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            recovery(tid);
        }
        lockManager.releaseLockByTransactionId(tid);
    }

    private void recovery(TransactionId tid) throws IOException {
        ConcurrentLinkedQueue<Page> pages = backupPage.get(tid);
        if (pages == null) return;
        while (!pages.isEmpty()) {
            Page page = pages.poll();
            Page beforePage = page.getBeforeImage();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(beforePage.getId().getTableId());
            dbFile.writePage(beforePage);
            pagePool.put(beforePage.getId(), beforePage);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = dbFile.insertTuple(tid, t);
        for (Page page : pages) {
            if (pagePool.size() == capacity) evictPage();
            pagePool.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            if (pagePool.size() == capacity) evictPage();
            pagePool.put(page.getId(), page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pagePool.keySet()) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        pagePool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page dirtyPage = pagePool.get(pid);
        TransactionId transactionId = dirtyPage.isDirty();
        if (transactionId == null) return;
        //缓冲池里的页面一定原来在磁盘中，将页面重写写到磁盘中覆盖掉原来就旧的内容
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(dirtyPage);
        dirtyPage.markDirty(false, transactionId);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        ConcurrentLinkedQueue<Page> pages = backupPage.get(tid);
        if (pages == null) return;
        while (!pages.isEmpty()) {
            Page page = pages.poll();
            flushPage(page.getId());
            page.markDirty(false, tid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        //用淘汰策略淘汰一个页面
        Iterator<Map.Entry<PageId, Page>> iterator = pagePool.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> pageEntry = iterator.next();
            Page page = pageEntry.getValue();
            if (page.isDirty() == null) {
                iterator.remove();
                return;
            }
			/*iterator.remove();
			Page page = pageEntry.getValue();
			if (page.isDirty() != null) {
				try {
					flushPage(page.getId());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}*/
        }
        throw new DbException("no clean page");
    }

    public Iterator<Map.Entry<PageId, Page>> iterator() {
        return pagePool.entrySet().iterator();
    }

}
