package simpledb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

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
		// some code goes here
		// not necessary for lab1
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
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		// some code goes here
		return null;
		// not necessary for lab1
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid);
	}

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

		HeapFileIterator(TransactionId tid) {
			this.numPages = numPages();
			this.cur = -1;
			this.bufferPool = Database.getBufferPool();
			this.tid = tid;
			this.permissions = Permissions.READ_ONLY;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			reset(++cur);
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return cur < numPages && cur != -1 && tupleIterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if(cur == -1 || cur >= numPages) throw new NoSuchElementException();
			if (tupleIterator == null || !tupleIterator.hasNext()) {
				if(cur == numPages - 1) throw new NoSuchElementException();
				reset(++cur);
			}
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
		}

		private void reset(int curPageId) throws TransactionAbortedException, DbException {
			pageId = new HeapPageId(id, curPageId);
			curPage = (HeapPage) bufferPool.getPage(tid, pageId, permissions);
			tupleIterator = curPage.iterator();
		}

	}


}

