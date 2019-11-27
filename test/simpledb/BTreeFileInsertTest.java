package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import simpledb.Predicate.Op;
import simpledb.systemtest.SimpleDbTestBase;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BTreeFileInsertTest extends SimpleDbTestBase {
	private simpledb.TransactionId tid;
	
	/**
	 * Set up initial resources for each unit test.
	 */
	@Before
	public void setUp() throws Exception {
		tid = new simpledb.TransactionId();
	}

	@After
	public void tearDown() throws Exception {
		simpledb.Database.getBufferPool().transactionComplete(tid);
		
		// set the page size back to the default
		simpledb.BufferPool.resetPageSize();
		simpledb.Database.reset();
	}

	@Test
	public void testSplitLeafPages() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		simpledb.Database.reset();
		simpledb.BTreeFile empty = simpledb.BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 3);
		int tableid = empty.getId();
		int keyField = 0;

		// create the leaf page
		simpledb.BTreePageId leftPageId = new simpledb.BTreePageId(tableid, 2, simpledb.BTreePageId.LEAF);
		simpledb.BTreeLeafPage leftPage = simpledb.BTreeUtility.createRandomLeafPage(leftPageId, 2, keyField,
				0, simpledb.BTreeUtility.MAX_RAND_VALUE);
				
		// create the parent page
		simpledb.BTreePageId parentId = new simpledb.BTreePageId(tableid, 1, simpledb.BTreePageId.INTERNAL);
		simpledb.BTreeInternalPage parent = new simpledb.BTreeInternalPage(parentId,
				simpledb.BTreeInternalPage.createEmptyPageData(), keyField);
				
		// set the pointers
		leftPage.setParentId(parentId);
		
		simpledb.Field field = new simpledb.IntField(simpledb.BTreeUtility.MAX_RAND_VALUE/2);
		HashMap<simpledb.PageId, simpledb.Page> dirtypages = new HashMap<simpledb.PageId, simpledb.Page>();
		dirtypages.put(leftPageId, leftPage);
		dirtypages.put(parentId, parent);
		simpledb.BTreeLeafPage page = empty.splitLeafPage(tid, dirtypages, leftPage, field);
		assertTrue(page.getLeftSiblingId() != null || page.getRightSiblingId() != null);
		simpledb.BTreeLeafPage otherPage;
		if(page.getLeftSiblingId() != null) {
			otherPage = (simpledb.BTreeLeafPage) dirtypages.get(page.getLeftSiblingId());
			assertTrue(field.compare(Op.GREATER_THAN_OR_EQ, 
					otherPage.reverseIterator().next().getField(keyField)));
		}
		else { // page.getRightSiblingId() != null
			otherPage = (simpledb.BTreeLeafPage) dirtypages.get(page.getRightSiblingId());
			assertTrue(field.compare(Op.LESS_THAN_OR_EQ, 
					otherPage.iterator().next().getField(keyField)));
		}
		
		int totalTuples = page.getNumTuples() + otherPage.getNumTuples();
		assertEquals(simpledb.BTreeUtility.getNumTuplesPerPage(2), totalTuples);
		assertTrue(simpledb.BTreeUtility.getNumTuplesPerPage(2)/2 == page.getNumTuples() ||
				simpledb.BTreeUtility.getNumTuplesPerPage(2)/2 + 1 == page.getNumTuples());
		assertTrue(simpledb.BTreeUtility.getNumTuplesPerPage(2)/2 == otherPage.getNumTuples() ||
				simpledb.BTreeUtility.getNumTuplesPerPage(2)/2 + 1 == otherPage.getNumTuples());
		assertEquals(1, parent.getNumEntries());
	}

	@Test
	public void testSplitInternalPages() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		simpledb.Database.reset();
		int entriesPerPage = simpledb.BTreeUtility.getNumEntriesPerPage();
		simpledb.BTreeFile empty = simpledb.BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 3 + entriesPerPage);
		int tableid = empty.getId();
		int keyField = 0;

		// create the internal page
		simpledb.BTreePageId leftPageId = new simpledb.BTreePageId(tableid, 2, simpledb.BTreePageId.INTERNAL);
		simpledb.BTreeInternalPage leftPage = simpledb.BTreeUtility.createRandomInternalPage(leftPageId, keyField, simpledb.BTreePageId.LEAF,
				0, simpledb.BTreeUtility.MAX_RAND_VALUE, 3);
				
		// create the parent page
		simpledb.BTreePageId parentId = new simpledb.BTreePageId(tableid, 1, simpledb.BTreePageId.INTERNAL);
		simpledb.BTreeInternalPage parent = new simpledb.BTreeInternalPage(parentId,
				simpledb.BTreeInternalPage.createEmptyPageData(), keyField);
				
		// set the pointers
		leftPage.setParentId(parentId);
		
		simpledb.Field field = new simpledb.IntField(simpledb.BTreeUtility.MAX_RAND_VALUE/2);
		HashMap<simpledb.PageId, simpledb.Page> dirtypages = new HashMap<simpledb.PageId, simpledb.Page>();
		dirtypages.put(leftPageId, leftPage);
		dirtypages.put(parentId, parent);
		simpledb.BTreeInternalPage page = empty.splitInternalPage(tid, dirtypages, leftPage, field);
		simpledb.BTreeInternalPage otherPage;
		assertEquals(1, parent.getNumEntries());
		simpledb.BTreeEntry parentEntry = parent.iterator().next();
		if(parentEntry.getLeftChild().equals(page.getId())) {
			otherPage = (simpledb.BTreeInternalPage) dirtypages.get(parentEntry.getRightChild());
			assertTrue(field.compare(Op.LESS_THAN_OR_EQ, 
					otherPage.iterator().next().getKey()));
		}
		else { // parentEntry.getRightChild().equals(page.getId())
			otherPage = (simpledb.BTreeInternalPage) dirtypages.get(parentEntry.getLeftChild());
			assertTrue(field.compare(Op.GREATER_THAN_OR_EQ, 
					otherPage.reverseIterator().next().getKey()));
		}
		
		int totalEntries = page.getNumEntries() + otherPage.getNumEntries();
		assertEquals(entriesPerPage - 1, totalEntries);
		assertTrue(entriesPerPage/2 == page.getNumEntries() || 
				entriesPerPage/2 - 1 == page.getNumEntries());
		assertTrue(entriesPerPage/2 == otherPage.getNumEntries() || 
				entriesPerPage/2 - 1 == otherPage.getNumEntries());
	}    

	@Test
	public void testReusePage() throws Exception {
		File emptyFile = File.createTempFile("empty", ".dat");
		emptyFile.deleteOnExit();
		simpledb.Database.reset();
		simpledb.BTreeFile empty = simpledb.BTreeUtility.createEmptyBTreeFile(emptyFile.getAbsolutePath(), 2, 0, 3);
		int tableid = empty.getId();
		int keyField = 0;

		// create the leaf page
		HashMap<simpledb.PageId, simpledb.Page> dirtypages = new HashMap<simpledb.PageId, simpledb.Page>();
		empty.setEmptyPage(tid, dirtypages, 2);
		simpledb.BTreePageId leftPageId = new simpledb.BTreePageId(tableid, 3, simpledb.BTreePageId.LEAF);
		simpledb.BTreeLeafPage leftPage = simpledb.BTreeUtility.createRandomLeafPage(leftPageId, 2, keyField,
				0, simpledb.BTreeUtility.MAX_RAND_VALUE);
				
		// create the parent page
		simpledb.BTreePageId parentId = new simpledb.BTreePageId(tableid, 1, simpledb.BTreePageId.INTERNAL);
		simpledb.BTreeInternalPage parent = new simpledb.BTreeInternalPage(parentId,
				simpledb.BTreeInternalPage.createEmptyPageData(), keyField);
				
		// set the pointers
		leftPage.setParentId(parentId);
		
		simpledb.Field field = new simpledb.IntField(simpledb.BTreeUtility.MAX_RAND_VALUE/2);
		dirtypages.put(leftPageId, leftPage);
		dirtypages.put(parentId, parent);
		simpledb.BTreeLeafPage page = empty.splitLeafPage(tid, dirtypages, leftPage, field);
		assertTrue(page.getLeftSiblingId() != null || page.getRightSiblingId() != null);
		simpledb.BTreeLeafPage otherPage;
		if(page.getLeftSiblingId() != null) {
			otherPage = (simpledb.BTreeLeafPage) dirtypages.get(page.getLeftSiblingId());
		}
		else { // page.getRightSiblingId() != null
			otherPage = (simpledb.BTreeLeafPage) dirtypages.get(page.getRightSiblingId());
		}
		
		assertTrue(page.getId().getPageNumber() == 2 || otherPage.getId().getPageNumber() == 2);
	}

	/**
	 * JUnit suite target
	 */
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(BTreeFileInsertTest.class);
	}
}
