package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * Each table is represented by a single DbFile.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	//表文件
	private File file;
	//表结构
	private TupleDesc tupleDesc;
	//HeapFile 的唯一标识id
	//Each file has a unique id used to store metadata about the table in the Catalog
	private int id;

	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f the file that stores the on-disk backing store for this heap
	 *          file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.tupleDesc = td;
		this.id = Objects.hash(this.file.getAbsoluteFile());
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return this.file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere to ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return this.id;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		int pageSize = BufferPool.getPageSize();
		int pageNumber = pid.getPageNumber(); //从0开始的
		byte[] data = new byte[pageSize];
		Page page = null;
		try (FileInputStream inputStream = new FileInputStream(file)) {
			inputStream.skip(pageNumber * pageSize);
			inputStream.read(data);
			page = new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		//页面在文件中的偏移量
		int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw")) {
			randomAccessFile.skipBytes(offset);
			randomAccessFile.write(page.getPageData());
		}
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		long fileLength = file.length();
		int pageSize = BufferPool.getPageSize();
		int res = (int) (fileLength / pageSize);
		if (fileLength % pageSize != 0) res++;
		return res;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		ArrayList<Page> res = new ArrayList<>();
		BufferPool bufferPool = Database.getBufferPool();
		int numPages = numPages();
		boolean inserted = false;
		HeapPage page = null;
		for (int i = 0; i < numPages; i++) {
			page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(this.id, i), Permissions.READ_WRITE);
			if (page.getNumEmptySlots() > 0) {
				page.insertTuple(t);
				res.add(page);
				inserted = true;
				break;
			}
		}
		if (!inserted) {
			page = new HeapPage(new HeapPageId(id, numPages), new byte[BufferPool.getPageSize()]);
			page.insertTuple(t);
			res.add(page);
		}
		writePage(page);
		return res;
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		ArrayList<Page> res = new ArrayList<>();
		BufferPool bufferPool = Database.getBufferPool();
		HeapPage page = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
		page.deleteTuple(t);
		res.add(page);
		return res;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid);
	}

	/**
	 * 迭代整个表的所有元组
	 */
	private class HeapFileIterator implements DbFileIterator {
		//页面数量
		private int numPages;
		//当前在第几页
		private int cur;

		private BufferPool bufferPool;

		private TransactionId tid;

		private Permissions permissions;

		private PageId pageId;

		private HeapPage curPage;

		private Iterator<Tuple> tupleIterator;

		private boolean opened;

		HeapFileIterator(TransactionId tid) {
			this.numPages = numPages();
			this.cur = -1;
			this.bufferPool = Database.getBufferPool();
			this.tid = tid;
			this.permissions = Permissions.READ_ONLY;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.opened = true;
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (!opened) return false;
			while (tupleIterator == null || !tupleIterator.hasNext()) {
				if (cur == numPages - 1) return false;
				reset(++cur);
			}
			return cur < numPages && cur != -1;
		}


		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!opened || cur >= numPages) throw new NoSuchElementException();
			return tupleIterator.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			cur = 0;
			reset(cur);
		}

		@Override
		public void close() {
			cur = -1;
			pageId = null;
			curPage = null;
			tupleIterator = null;
			this.opened = false;
		}

		private void reset(int curPageId) throws TransactionAbortedException, DbException {
			pageId = new HeapPageId(id, curPageId);
			curPage = (HeapPage) bufferPool.getPage(tid, pageId, permissions);
			tupleIterator = curPage.iterator();
		}

	}


}

