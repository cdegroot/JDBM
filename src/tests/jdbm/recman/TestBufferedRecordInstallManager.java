/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id$
 */
package jdbm.recman;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import org.CognitiveWeb.extser.Stateless;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.helper.Serializer;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test suite for feature providing buffered installation of physical row
 * updates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferedRecordInstallManager
 * 
 * @todo Examine resulting files using the {@link DumpUtility}.
 * @todo Verify abort discards data. The easiest way might be to introduce
 *       transaction semantics into
 *       {@link #doManyRecordsTest(int, int, int, jdbm.recman.TestBufferedRecordInstallManager.Op, boolean)}
 *       using a temporary buffer for changes that commits to the existing
 *       buffer, an appropriate testing within the tx before testing outside of
 *       it, and adding an "abort" operation.
 * @todo show %of records and/or bytes written for stress tests.  Consider adding
 *       a "counters" option to show the counters.
 */
public class TestBufferedRecordInstallManager extends TestCase {

	/**
	 * 
	 */
	public TestBufferedRecordInstallManager() {
		super();
	}

	/**
	 * 
	 */
	public TestBufferedRecordInstallManager(String name) {
		super(name);
	}

	public static junit.framework.Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestBucket.class);
		suite.addTestSuite(TestOp.class);
		suite.addTestSuite(TestBufferedRecordInstallManager.class);
		return suite;
	}

	RecordManager recman;
    BaseRecordManager baserecman;
    
    /**
     * The #of bytes of data to be written by the various multi-page stress
     * tests using {@link #doManyRecordsTest(int, int, int)}.  These tests
     * require that we retain a copy of the inserted data in memory, so if
     * you make this value too large you will run out of memory unless you
     * explicitly increase the available heap for the test run.
     */
    public final int STRESS = 10 * 1024 * 1024; // 10M
    
    /**
	 * The tests are run with the buffered installs and lazy insert features
	 * enabled.
	 */
    public void setUp() throws Exception {

    	super.setUp();
    	
        TestRecordFile.deleteTestFile();

        openStore();

    }

    public void tearDown() throws Exception {

    	try {
    		recman.close();
    	}
    	catch( Throwable t ) {
    		System.err.println("WARN: Exception while closing recman: "+t);
    		// ignore.
    	}

    	TestRecordFile.deleteTestFile();
    	
        super.tearDown();    	

    }

    /**
	 * You have to edit this code to run the test suite with or without the
	 * {@link CacheRecordManager}.
	 * 
	 * @throws IOException
	 */
    void openStore() throws IOException {
        
    	Properties properties = new Properties();
        
        properties.setProperty(RecordManagerOptions.BUFFERED_INSTALLS, "true");
        properties.setProperty(RecordManagerOptions.LAZY_INSERT, "true");
//        properties.setProperty(RecordManagerOptions.CACHE_TYPE,RecordManagerOptions.NO_CACHE);
        
        openStore( properties );
        
    }
    
    void openStore( Properties properties ) throws IOException {
    	
        recman = RecordManagerFactory.createRecordManager(
				TestRecordFile.testFileName, properties);
        
        baserecman = (BaseRecordManager) recman.getBaseRecordManager();
        
    }
    
    void reopenStore() throws IOException {
    
    	recman.commit(); // do no leave any uncommitted changes!
    	
    	recman.close();
    	
    	openStore();
    	
    }
    
    /**
	 * Test verifies that the {@link Provider} configures the
	 * {@link BufferedRecordInstallManager} on the {@link BaseRecordManager}.
	 * 
	 * @throws IOException
	 */
    public void test_provider() throws IOException {
    	
    	assertTrue(baserecman._bufMgr!=null);
    	
    }

    /**
     * Tests of the {@link BufferedRecordInstallManager.Bucket} inner class
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestBucket extends TestCase {
    
    public void test_bucket() {
    	/*
    	 * Create an empty bucket.
    	 */
    	BufferedRecordInstallManager.Bucket bucket = new BufferedRecordInstallManager.Bucket();
    	
    	/*
    	 * Verify initial conditions.
    	 */
    	assertEquals(DataPage.DATA_PER_PAGE,bucket._capacity);
    	assertEquals(DataPage.DATA_PER_PAGE,bucket._avail);
    	assertEquals(0,bucket._size);
    	assertEquals(0,bucket.getRecordCount());
    	assertFalse(bucket.iterator().hasNext());
    	assertTrue(bucket.isEmpty());
    	
    	/*
    	 * Add some buffered records, verifying the change in the state of the bucket.
    	 */
    	Random r = new Random();
    	int expectedSize = 0;
    	int expectedAvail = DataPage.DATA_PER_PAGE;
    	int nrecs = 0;
    	final int limit = 10;
    	final int maxDataPerRecord = 128;
    	assertTrue(limit*(RecordHeader.SIZE+maxDataPerRecord)<=DataPage.DATA_PER_PAGE);
    	for( int i=0; i<limit; i++ ) {
    		// Assign distinct logical row identifiers (they are junk, but distinct).
    		Location logRowId = new Location((long)i);
    		/*
			 * Generate random data of random length, but total length of all
			 * data must not exceed bucket size.  The +1 is there to avoid a zero
			 * length record.
			 */
    		byte[] data = new byte[r.nextInt(maxDataPerRecord)+1];
    		r.nextBytes(data);
    		// create buffered record.
    		assertFalse(bucket.add(logRowId, new Location(0L), data));
    		expectedSize += (RecordHeader.SIZE + data.length);
    		expectedAvail -= (RecordHeader.SIZE + data.length);
    		nrecs++;
    		assertEquals( expectedSize, bucket._size );
    		assertEquals( expectedAvail, bucket._avail );
    		assertEquals( nrecs, bucket.getRecordCount() );
        	assertFalse(bucket.isEmpty());
    	}
    	
    	/*
    	 * Verify that we can visit all buffered records in the bucket.
    	 */
    	Iterator itr = bucket.iterator();
    	final int n = bucket.getRecordCount();
    	for( int i=0; i<n; i++ ) {
    		BufferedRecordInstallManager.BufferedRecord rec = (BufferedRecordInstallManager.BufferedRecord) itr.next();
    		assertNotNull( rec );
    	}
    	assertFalse(itr.hasNext()); // and no more.
    	
    	/*
    	 * Clear the bucket and reverify that the initial conditions hold again.
    	 */
    	bucket.clear();
    	assertEquals(DataPage.DATA_PER_PAGE,bucket._capacity);
    	assertEquals(DataPage.DATA_PER_PAGE,bucket._avail);
    	assertEquals(0,bucket._size);
    	assertEquals(0,bucket.getRecordCount());
    	assertFalse(bucket.iterator().hasNext());
    	assertTrue(bucket.isEmpty());
    	
    	/*
		 * Verify that the bucket correctly detects when a buffered record is
		 * being replaced by another buffered record for the same logical row
		 * identifier.
		 */
    	if (true) {
			Location logRowId = new Location(12L);
			byte[] data = new byte[] { 1, 2, 3 };
			assertFalse(bucket.add(logRowId,new Location(0L),data));
			assertTrue(bucket.add(logRowId,new Location(0L),data));
	    	assertFalse(bucket.isEmpty());
		}
    	
    	/*
		 * Verify that the bucket correctly handles a request to delete() a
		 * record that is not in the bucket and verify that the bucket correctly
		 * handles a request to delete() a buffered record from the bucket. This
		 * also does some cursory testing of the bucket's fetch() method.
		 */
    	bucket.clear();
    	if( true ) {
    		/*
    		 * Add a record.
    		 */
			Location logRowId = new Location(12L);
			Location logRowId2 = new Location(13L); // a different location.
			byte[] data = new byte[] { 1, 2, 3 };
			bucket.add(logRowId,new Location(0L),data);
    		assertEquals(RecordHeader.SIZE+data.length, bucket._size );
    		assertEquals(DataPage.DATA_PER_PAGE-(RecordHeader.SIZE+data.length), bucket._avail );
    		assertEquals(1,bucket.getRecordCount());
    		assertEquals(data,bucket.fetch(logRowId));
    		assertNull(bucket.fetch(logRowId2));
    		/*
    		 * Remove a record not found in the bucket. 
    		 */
    		assertFalse(bucket.delete(logRowId2));
    		assertEquals(RecordHeader.SIZE+data.length, bucket._size );
    		assertEquals(DataPage.DATA_PER_PAGE-(RecordHeader.SIZE+data.length), bucket._avail );
    		assertEquals(1,bucket.getRecordCount());
    		assertEquals(data,bucket.fetch(logRowId));
    		assertNull(bucket.fetch(logRowId2));
    		/*
    		 * Remove a record found in the bucket. 
    		 */
    		assertTrue(bucket.delete(logRowId));
    		assertEquals(0, bucket._size );
    		assertEquals(DataPage.DATA_PER_PAGE, bucket._avail );
    		assertEquals(0,bucket.getRecordCount());
    		assertNull(bucket.fetch(logRowId));
    		assertNull(bucket.fetch(logRowId2));
    		/*
    		 * Attempt to remove a record that was already deleted from the bucket. 
    		 */
    		bucket.delete(logRowId);
//    		try {
//    			bucket.delete(logRowId);
//    			assertTrue("Expecting exception",false);
//    		}
//    		catch( IllegalStateException ex ) {
//    			System.err.println("Ignoring expected exception: "+ex);
//    		}
    		/*
    		 * Attempt to double delete a record that was never in the bucket.
    		 */
    		bucket.delete(logRowId2);
    	}

	}
    }

    /**
     * Correct rejection test for a zero length record.
     * 
     * @throws IOException
     */
    public void test_zeroLengthRecord() throws IOException {

    	recman.insert(new byte[]{},MyNopSerializer.INSTANCE);
    	
    	try {
    		recman.commit();
    		assertTrue("Zero length records are not allowed.",false);
    	}
    	catch( IllegalArgumentException ex ) {
    		System.err.println("Ignoring expected exception: "+ex);
    		recman.rollback(); // discard the changes so that we can close the file!
    	}

    }

    /**
	 * Inserts a single one byte record. Closes and reopens the store and
	 * verifies that we can fetch the record and that the correct data is
	 * recovered.
	 * 
	 * @throws IOException
	 */
    public void test_oneByteRecord() throws IOException {

    	doSingleRecordInsertTest( 1 );

    }

    /**
	 * Inserts a single record. Closes and reopens the store and verifies that
	 * we can fetch the record and that the correct data is recovered.
	 */
    public void test_singleRecord() throws IOException {

    	long recid = recman.insert("HelloWorld!");

    	assertEquals("HelloWorld!", recman.fetch(recid));

    	recman.commit();
    	
    	reopenStore();
    	
    	assertEquals("HelloWorld!", recman.fetch(recid));
    	
	}
    
    /**
	 * Inserts a single large record. Closes and reopens the store and verifies
	 * that we can fetch the record and that the correct data is recovered.
	 */
    public void test_singleLargeRecord() throws IOException {

    	doSingleRecordInsertTest( RecordFile.BLOCK_SIZE * 20 );
    	
	}

    /**
	 * Inserts a single record. Closes and reopens the store and verifies that
	 * we can fetch the record and that the correct data is recovered.
	 * 
	 * @param len
	 *            The size of the record. The record will be filled with random
	 *            data.
	 */
    void doSingleRecordInsertTest( int len ) throws IOException {
    	Serializer ser = MyNopSerializer.INSTANCE;
    	Random r = new Random();
    	
    	byte[] data = new byte[ len ];
    	r.nextBytes(data);
    	
    	long recid = recman.insert(data,ser);

    	assertEquals(data, (byte[])recman.fetch(recid,ser));

    	recman.commit();
    	
    	reopenStore();
    	
    	assertEquals(data, (byte[])recman.fetch(recid,ser));
    	
    }
    
    /**
	 * Verify with record that would fill the space available on an empty page
	 * (less one byte, exactly, with one extra byte). This is fence post
	 * testing.
	 * 
	 * @throws IOException
	 */
    public void test_pageFencePosts() throws IOException {

    	doSingleRecordInsertTest( RecordFile.BLOCK_SIZE - 1 );

    	reopenStore();

    	doSingleRecordInsertTest( RecordFile.BLOCK_SIZE );

    	reopenStore();

    	doSingleRecordInsertTest( RecordFile.BLOCK_SIZE + 1 );

    }
    
    /**
	 * Inserts a single record and fetches it to verify its state. Closes and
	 * reopens the store and verifies that we can fetch the record and that the
	 * correct data is recovered. Updates the record and fetches it to verify
	 * its new state. Closes and reopens the store and fetches the record to
	 * verify its new state. Deletes the record and verifies that the record can
	 * no longer be fetched. Closes and reopens the store and verifies that the
	 * record can no longer be fetched.
	 */
    public void test_crudForSingleRecord() throws IOException {

    	Random r = new Random();
    	
    	byte[] expected = new byte[128];
    	byte[] expected2 = new byte[128];
    	r.nextBytes(expected);
    	r.nextBytes(expected2);
    	
    	final Serializer ser = MyNopSerializer.INSTANCE;
    	
    	long recid = recman.insert(expected,ser);

    	assertEquals(expected, (byte[])recman.fetch(recid,ser));

    	recman.commit();
    	
    	reopenStore();
    	
    	assertEquals(expected, (byte[])recman.fetch(recid,ser));

    	recman.update(recid, expected2, ser);
    	
    	assertEquals(expected2, (byte[])recman.fetch(recid,ser));
    	
    	reopenStore();
    	
    	assertEquals(expected2, (byte[])recman.fetch(recid,ser));

    	recman.delete(recid);
    	
    	assertNull(recman.fetch(recid,ser));

    	reopenStore();
    	
    	assertNull(recman.fetch(recid,ser));
    	
    }

    /**
	 * Test insert followed by an insert followed by a commit. This looks for
	 * problems including where the same recid is assigned to each insert!
	 * 
	 * <pre>
	 *  log: op#1=insert, recid=65594, data.length=1
	 *  log: op#2=insert, recid=65594, data.length=97
	 * </pre>
	 */
    public void test_multipleInsert() throws IOException {
    	Random r = new Random();
    	
    	byte[] expected = new byte[128];
    	byte[] expected2 = new byte[128];
    	r.nextBytes(expected);
    	r.nextBytes(expected2);
    	
    	final Serializer ser = MyNopSerializer.INSTANCE;
    	
    	long recid = recman.insert(expected,ser);
    	long recid2 = recman.insert(expected2,ser);

    	assertFalse("recids are the same", recid == recid2 );

    	assertEquals(expected, (byte[])recman.fetch(recid,ser));
    	assertEquals(expected2, (byte[])recman.fetch(recid2,ser));

    	recman.commit();

    	assertEquals(expected, (byte[])recman.fetch(recid,ser));
    	assertEquals(expected2, (byte[])recman.fetch(recid2,ser));

    }
    
    /**
	 * <p>
	 * Test insert followed by a commit. Then an update followed by another
	 * update and then a commit. Verify that the last update was properly
	 * installed.
	 * </p>
	 * <p>
	 * Note: This test was generated by analysis of a log written by
	 * {@link #doManyRecordsTest(int, int, int, jdbm.recman.TestBufferedRecordInstallManager.Op)}
	 * and isolates a specific bug. The problem was using a logical row
	 * identifier in place of a physical row identifier in
	 * {@link BufferedRecordInstallManager} and has since been fixed.
	 * </p>
	 */
    public void test_multipleUpdate() throws IOException {
    	
    	Random r = new Random();
    	
    	byte[] expected = new byte[29];
    	byte[] expected2 = new byte[8];
    	byte[] expected3 = new byte[1];
    	r.nextBytes(expected);
    	r.nextBytes(expected2);
    	r.nextBytes(expected3);
    	
    	final Serializer ser = MyNopSerializer.INSTANCE;
    	
    	long recid = recman.insert(expected,ser);
    	recman.commit();
    	recman.update(recid, expected2, ser);
    	recman.update(recid, expected3, ser);

    	assertEquals(expected3, (byte[])recman.fetch(recid,ser));

    	recman.commit();
    	
    	reopenStore();
    	
    	assertEquals(expected3, (byte[])recman.fetch(recid,ser));

    }

    /**
	 * <p>
	 * Test verifies that a record whose insert was buffered and which was
	 * deleted before it was installed onto a page may be deleted again without
	 * complaint (since that is the behavior of the cache record manager, but
	 * this is an area where the {@link RecordManager} interface semantics are
	 * weak).
	 * </p>
	 * 
	 * @throws IOException
	 */
    public void test_multipleDelete() throws IOException {

    	Random r = new Random();
    	
    	byte[] expected = new byte[29];
    	r.nextBytes(expected);
    	
    	final Serializer ser = MyNopSerializer.INSTANCE;
    	
    	long recid = recman.insert(expected,ser);
    	recman.delete(recid);
    	recman.delete(recid);
    	
//    	try {
//    		recman.delete(recid);
//    		assertTrue("Expecting exception", false );
//    	}
//    	catch( IllegalStateException ex ) {
//    		System.err.println("Ignoring expected exception: "+ex);
//    	}

    }
    
    /**
	 * Test inserts a set of records whose total size is approximately 1/2 of a
	 * page with the result that the records are inserted using a record at a
	 * time strategy during the commit. This does not test that record at a time
	 * vs page at strategy is actually used, but the test is designed for the 
	 * condition in which the {@link BufferedRecordInstallManager} will elect a
	 * record at a time install.
	 */
    public void test_multipleRecordsUnderOnePage() throws IOException {
    	Op gen = new Op(.3f,.2f,.2f,0,0,0,0); // insert, fetch, update in place.
    	doManyRecordsTest( 128, 0, DataPage.DATA_PER_PAGE/2, gen, true );
    }
    
    /**
	 * Test inserts a bunch of records (each of which must be smaller than the
	 * waste margin) and works its way up to the waste margin for the page. At
	 * this point the page at a time install strategy will be choosen.
	 */
    public void test_multipleRecordsFillsPageWithinMargin() throws IOException {
    	int wasteMargin = baserecman._bufMgr._wasteMargin;
    	int maxRecordSize = wasteMargin/2+1;
    	Op gen = new Op(.3f,.2f,.2f,0,0,0,0); // insert, fetch, update in place.
    	doManyRecordsTest( maxRecordSize, 0, DataPage.DATA_PER_PAGE, gen, false );
    }

    /**
	 * Writes {@link #STRESS} bytes of random data and then verifies that it can
	 * all be recovered. This uses small record sizes so it should always fill
	 * the page up to within the waste margin.
	 */
    public void test_multipleRecordsFillsManyPages1() throws IOException {
    	int wasteMargin = baserecman._bufMgr._wasteMargin;
    	int maxRecordSize = wasteMargin/2+1;
    	Op gen = new Op(.5f,.2f,.2f,.1f,.1f,.001f,.0001f);
    	doManyRecordsTest( maxRecordSize, 0, STRESS, gen , false );
    }
    
    /**
	 * Writes {@link #STRESS} of random data and then verifies that it can all
	 * be recovered. This uses record sizes that should occasionally result in
	 * pages that are filled to within the waste margin and occassionally result
	 * in pages that are not.
	 */
    public void test_multipleRecordsFillsManyPages2() throws IOException {
    	int wasteMargin = baserecman._bufMgr._wasteMargin;
    	int maxRecordSize = wasteMargin * 2 + 1;
    	Op gen = new Op(.5f,.2f,.2f,.1f,.1f,.001f,.0001f);
    	doManyRecordsTest( maxRecordSize, 0, STRESS, gen, false );
    }
    
    /**
	 * Writes {@link #STRESS} of random data and then verifies that it can all
	 * be recovered. This uses record sizes that can span a page and hence mixes
	 * both batched and immediate operations.  The record size distribution is
	 * normal with a maximum size of 1.5 times the page size.
	 */
    public void test_multipleRecordsFillsManyPages3() throws IOException {
    	int maxRecordSize = (int)(RecordFile.BLOCK_SIZE * 1.5 + 1);
    	Op gen = new Op(.5f,.2f,.2f,.1f,.1f,.001f,.0001f);
    	doManyRecordsTest( maxRecordSize, 0, STRESS, gen, false );
    }
    

    /**
	 * Helper class generates a random sequence of operation codes obeying the
	 * probability distribution described in the constuctor call.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
	 * @version $Id$
	 */
    static class Op {
    	
    	/** Insert a record. */
		static final public int insert = 0;

		/** Fetch a record. */
		static final public int fetch = 1;

		/**
		 * Update a record in place (new record does not exceed old record
		 * size).
		 */
		static final public int update1 = 2;

		/**
		 * Update a record forcing reallocation of the record (new record
		 * exceeds the old record size).
		 */
		static final public int update2 = 3;

		/**
		 * Delete a record.
		 */
		static final public int delete = 4;

		/**
		 * Commit the current transaction.
		 */
		static final public int commit = 5; // and abort?

		/**
		 * Close and then reopen the store.
		 */
		static final public int reopen = 6;

		/**
		 * The last defined operator.
		 */
		static final int lastOp = reopen;
		
    	final private Random r = new Random();
    	
    	final private float[] _dist;

    	public Op(float insertRate, float fetchRate, float update1Rate, float update2Rate, float deleteRate, float commitRate, float reopenRate )
    	{
    		if (insertRate < 0 || fetchRate < 0 || update1Rate < 0
					|| update2Rate < 0 || deleteRate < 0 || commitRate < 0
					|| reopenRate < 0) {
				throw new IllegalArgumentException("negative rate");
			}
			float total = insertRate + fetchRate + update1Rate + update2Rate
					+ deleteRate + commitRate + reopenRate;
			if( total == 0.0 ) {
				throw new IllegalArgumentException("all rates are zero.");
			}
			/*
			 * Convert to normalized distribution in [0:1].
			 */
			insertRate /= total;
			fetchRate /= total;
			update1Rate /= total;
			update2Rate /= total;
			deleteRate /= total;
			commitRate /= total;
			reopenRate /= total;
			/*
			 * Save distribution.
			 */
			int i = 0;
			_dist = new float[lastOp+1];
			_dist[ i++ ] = insertRate;
			_dist[ i++ ] = fetchRate;
			_dist[ i++ ] = update1Rate;
			_dist[ i++ ] = update2Rate;
			_dist[ i++ ] = deleteRate;
			_dist[ i++ ] = commitRate;
			_dist[ i++ ] = reopenRate;

			/*
			 * Checksum.
			 */
			float sum = 0f;
			for( i = 0; i<_dist.length; i++ ) {
				sum += _dist[ i ];
			}
			if( Math.abs( sum - 1f) > 0.01 ) {
				throw new AssertionError("sum of distribution is: "+sum+", but expecting 1.0");
			}
			
    	}
    	
    	/**
    	 * Return the name of the operator.
    	 * 
    	 * @param op
    	 * @return
    	 */
    	public String getName( int op ) {
    		if( op < 0 || op > lastOp ) {
    			throw new IllegalArgumentException();
    		}
    		switch( op ) {
    		case insert: return "insert";
    		case fetch:  return "fetch";
    		case update1: return "update1";
    		case update2: return "update2";
    		case delete:  return "delete";
    		case commit:  return "commit";
    		case reopen:  return "reopen";
    		default:
    			throw new AssertionError();
    		}
    	}
    	
    	/**
		 * An array of normalized probabilities assigned to each operator. The
		 * array may be indexed by the operator, e.g., dist[{@link #fetch}]
		 * would be the probability of a fetch operation.
		 * 
		 * @return The probability distribution over the defined operators.
		 */
    	public float[] getDistribution() {
    		return _dist;
    	}

    	/**
		 * Generate a random operator according to the distribution described to
		 * to the constructor.
		 * 
		 * @return A declared operator selected according to a probability
		 *         distribution.
		 */
    	public int nextOp() {
    		final float rand = r.nextFloat(); // [0:1)
    		float cumprob = 0f;
    		for( int i=0; i<_dist.length; i++ ) {
    			cumprob += _dist[ i ];
    			if( rand <= cumprob ) {
    				return i;
    			}
    		}
    		throw new AssertionError();
    	}
    	
    }

    /**
     * Tests of the {@link Op} test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestOp extends TestCase {

		public void test_Op() {
			Op gen = new Op(.2f, .1f, .1f, 0f, .1f, .05f, .01f);
			doOpTest(gen);
		}

		public void test_Op2() {
			Op gen = new Op(1f, 0f, 0f, 0f, 0f, 0f, 0f);
			doOpTest(gen);
		}

		/**
		 * Verifies the {@link Op} class given an instance with some probability
		 * distribution.
		 */
		void doOpTest(final Op gen) {
			final int limit = 10000;
			int[] ops = new int[limit];
			int[] sums = new int[Op.lastOp + 1];
			for (int i = 0; i < limit; i++) {
				int op = gen.nextOp();
				assertTrue(op >= 0);
				assertTrue(op <= Op.lastOp);
				ops[i] = op;
				sums[op]++;
			}
			float[] expectedProbDistribution = gen.getDistribution();
			float[] actualProbDistribution = new float[Op.lastOp + 1];
			float sum = 0f;
			for (int i = 0; i <= Op.lastOp; i++) {
				sum += expectedProbDistribution[i];
				actualProbDistribution[i] = (float) ((double) sums[i] / (double) limit);
				float diff = Math.abs(actualProbDistribution[i]
						- expectedProbDistribution[i]);
				System.err.println("expected[i=" + i + "]="
						+ expectedProbDistribution[i] + ", actual[i=" + i
						+ "]=" + actualProbDistribution[i] + ", diff="
						+ ((int) (diff * 1000)) / 10f + "%");
				assertTrue(diff < 0.02); // difference is less than 2%
											// percent.
			}
			assertTrue(Math.abs(sum - 1f) < 0.01); // essential 1.0
		}

	}

    /**
	 * Test helper performs a set random operations (insert, update, delete,
	 * fetch) using random records not to exceed the specified constraints.
	 * After the constraints have been satisfied, all records are read to verify
	 * their final contents. The transaction is then committed and the store is
	 * closed and reopened. Each record is then fetched by its record identifier
	 * and its contents are re-verified.
	 * 
	 * @param maxDataPerRecord
	 *            The maximum size of the data for a record to be inserted. The
	 *            record data is generated randomly using a normal distribution
	 *            with this value as its maximum range. The range of the
	 *            distribution is [1:maxDataPerRecord], inclusive.
	 * @param maxRecords
	 *            The maximum #of records that will be inserted or ZERO (0) to
	 *            not impose this constraint.
	 * @param maxBytesWritten
	 *            The maximum number of bytes that will be written or ZERO(0L)
	 *            to not impose this constraint. The constraint is on the total
	 *            #of bytes of data plus the
	 *            {@link RecordHeader#SIZE record header size}.
	 * @param gen
	 *            A generator for random operations on the database.
	 * @param log
	 *            When true a log of operations will be written. This can be
	 *            examined to identify path dependent interactions resulting in
	 *            an exception or failed assertion.
	 */

    void doManyRecordsTest(final int maxDataPerRecord, final int maxRecords,
			final int maxBytesWritten, final Op gen, final boolean log ) throws IOException {
    	
    	if (maxDataPerRecord <= 0 || maxRecords < 0 || maxBytesWritten < 0) {
			throw new IllegalArgumentException();
		}
		if (maxRecords == 0 && maxBytesWritten == 0) {
			throw new IllegalArgumentException("no stopping condition");
		}
    	
    	Random r = new Random();
    	Vector recs = new Vector(); // of currently defined Records.
    	HashSet recids = new HashSet(); // of currently defined recids. 
    	
    	int nrecs = 0;
    	long bytesWritten = 0;
    	boolean done = false;
    	int[] opsum = new int[ Op.lastOp + 1 ]; // #of times we do each operation.
    	long optotal = 0;
    	
    	final Serializer ser = MyNopSerializer.INSTANCE;
    	
    	while( ! done && ( maxRecords == 0 || nrecs < maxRecords ) ) {

    		// next operation. always an insert if there are no records in the store.
    		int op = ( recs.size() == 0 ? Op.insert : gen.nextOp() );

    		switch (op) {

			case Op.insert: {
				/*
				 * Generate data. The length is random. The contents are a
				 * random byte that is used to fill the length. This approach is
				 * used to minimize the memory overhead required to retain the
				 * current state of the inserted or updated records.
				 */
				int len = r.nextInt(maxDataPerRecord) + 1;
				byte[] data = new byte[len];
				Arrays.fill(data, (byte)r.nextInt(256));

				// Check to see if we would exceed the maxBytesWritten
				// constraint.
				if (maxBytesWritten != 0
						&& (len + RecordHeader.SIZE + bytesWritten > maxBytesWritten)) {
					done = true;
					break;
				}
				bytesWritten += len + RecordHeader.SIZE;

				// insert the record.
				long recid = recman.insert(data, ser );
				Record rec = new Record(recid, data[0], len );
				if( log ) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op) + ", recid=" + recid
							+ ", data.length=" + data.length);
				}
				recs.add(rec);
				if( ! recids.add(new Location(recid)) ) {
					throw new AssertionError("recid already assigned: "+recid);
				}
				nrecs++;
				break;
			}
			
			case Op.fetch: {
				
				// randomly choose an existing record.
				Record rec = (Record) recs.elementAt(r.nextInt(recs.size()));
				// fetch the data.
				if( ! recids.contains(new Location(rec.recid)) ) {
					throw new AssertionError("recid not assigned: "+rec.recid);
				}
				byte[] actualData = (byte[])recman.fetch( rec.recid, ser );
				if( log ) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op) + ", recid=" + rec.recid
							+ ", data.length=" + actualData.length);
				}
				byte[] expectedData = new byte[rec.len];
				Arrays.fill(expectedData,rec.data);
				assertEquals( "fetch: recid="+rec.recid, expectedData, actualData );
				break;
				
			}
			
			case Op.update1: {
				/*
				 * In place update of a record.
				 */
				// randomly choose an existing record.
				Record rec = (Record) recs.elementAt(r.nextInt(recs.size()));
				if( ! recids.contains(new Location(rec.recid)) ) {
					throw new AssertionError("recid not assigned: "+rec.recid);
				}
				/*
				 * Generate data. Since we do not directly know the current
				 * capacity, we just place the new data length at no more than
				 * the current data length. The contents are a random byte that
				 * is used to fill the length.
				 */
				int len = r.nextInt(rec.len) + 1;
				byte[] data = new byte[len];
				Arrays.fill(data, (byte)r.nextInt(256));

				// Check to see if we would exceed the maxBytesWritten
				// constraint.
				if (maxBytesWritten != 0
						&& (data.length + RecordHeader.SIZE + bytesWritten > maxBytesWritten)) {
					done = true;
					break;
				}
				bytesWritten += data.length + RecordHeader.SIZE;

				// update the record.
				if( log ) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op) + ", recid=" + rec.recid
							+ ", data.length=" + data.length);
				}
				recman.update( rec.recid, data, ser );
				rec.update(data[0],len);
				break;
			}
			
			case Op.update2: {
				/*
				 * Update of a record forces re-allocation of the record to a
				 * new physical row.
				 */
				// randomly choose an existing record.
				Record rec = (Record) recs.elementAt(r.nextInt(recs.size()));
				if( ! recids.contains(new Location(rec.recid)) ) {
					throw new AssertionError("recid not assigned: "+rec.recid);
				}
				/*
				 * Generate data. The length is choosen with a minimum of the
				 * maximum historical capacity for the record and a maximum of
				 * the maximum historical capacity plus the [maxDataPerRecord].
				 * The contents are a random byte that is used to fill the
				 * length.
				 */
				int len = r.nextInt(rec.maxCapacity+maxDataPerRecord) + 1;
				byte[] data = new byte[len];
				Arrays.fill(data, (byte)r.nextInt(256));

				// Check to see if we would exceed the maxBytesWritten
				// constraint.
				if (maxBytesWritten != 0
						&& (data.length + RecordHeader.SIZE + bytesWritten > maxBytesWritten)) {
					done = true;
					break;
				}
				bytesWritten += data.length + RecordHeader.SIZE;

				// update the record.
				if( log ) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op) + ", recid=" + rec.recid
							+ ", data.length=" + data.length);
				}
				recman.update( rec.recid, data, ser );
				rec.update(data[0],len);
				break;
			}
			
			case Op.delete: {
				// randomly choose an existing record.
				Record rec = (Record) recs.elementAt(r.nextInt(recs.size()));
				// delete the record.
				recman.delete( rec.recid );
				recs.remove( rec );
				if( ! recids.remove(new Location(rec.recid)) ) {
					throw new AssertionError("recid not assigned: "+rec.recid);
				}
				if( log ) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op) + ", recid=" + rec.recid);
				}
				/*
				 * Note: do not decrement this -- it is the #of records
				 * inserted, not the current #of records in the database.
				 */
//				nrecs--;
				break;
			}
			
			case Op.commit: {
				if (log||true) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op));
				}
				recman.commit();
				break;
			}
			
			case Op.reopen: {
				if (log||true) {
					System.err.println("log: op#" + (optotal + 1) + "="
							+ gen.getName(op));
				}
				reopenStore();
				break;
			}
			
			default:
				throw new AssertionError("Unknown operation: op=" + op);
			}
    		
    		opsum[ op ] ++;
    		optotal++;
    		
    	}

    	/*
    	 * Last operation is always a commit.
    	 */
    	recman.commit();
    	
		System.err.println("\nOperation histogram");
    	for(int i=0; i<opsum.length; i++ ) {
    		System.err.println("" + gen.getName(i) + "\t" + opsum[i] + "\t"
					+ ((int) (opsum[i] / (float) optotal * 1000) / 10f) + "%");
    	}
    	
    	System.err.println("Wrote "+nrecs+" records totaling "+bytesWritten+" bytes.");

    	if( baserecman._bufMgr != null ) {
    		/*
    		 * Write counters.
    		 */
    		baserecman._bufMgr.getCounters().writeCounters();
    	}
    	
    	verify( recs );
    	
    	reopenStore();

    	verify( recs );
    	
    }
    
	/**
	 * Verify data.
	 * 
	 * @param recs
	 *            A vector containing {@link Record}s to be verified against
	 *            the store.
	 * 
	 * @todo Consider randomized scan.
	 */
    private void verify( Vector recs ) throws IOException {

    	System.err.print("Scanning "+recs.size()+" records in store...");
    	
    	int i = 0;

    	long bytesRead = 0;
    	
    	long begin = System.currentTimeMillis();

    	Iterator itr = recs.iterator();
    	
    	while( itr.hasNext() ) {
    	
    		Record rec = (Record) itr.next();
    		
    		byte[] actualData = (byte[]) recman.fetch(rec.recid,MyNopSerializer.INSTANCE);
    		
    		assertEquals("recid["+i+"]="+rec.recid, new byte[]{rec.data}, actualData );
    		
    		bytesRead += actualData.length;
    		
    		i++;
    		
    	}

    	long elapsed = System.currentTimeMillis() - begin;

    	System.err.println("Verified " + i + " records totaling " + bytesRead
				+ " bytes in " + elapsed + "ms");
    	
    }

    /**
     * <p>
	 * Compares byte[]s by value (not reference).
	 * </p>
	 * <p>
	 * Note: This method will only be invoked if both arguments can be typed as
	 * byte[] by the compiler. If either argument is not strongly typed, you
	 * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
	 * invoked instead.
	 * </p>
	 * 
     * @param expected
     * @param actual
     */
    static public void assertEquals( byte[] expected, byte[] actual )
    {

        assertEquals( null, expected, actual );
        
    }
    
    /**
	 * <p>
	 * Compares byte[]s by value (not reference).
	 * </p>
	 * <p>
	 * Note: This method will only be invoked if both arguments can be typed as
	 * byte[] by the compiler. If either argument is not strongly typed, you
	 * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
	 * invoked instead.
	 * </p>
	 * 
	 * @param msg
	 * @param expected
	 * @param actual
	 */
    static public void assertEquals( String msg, byte[] expected, byte[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        assertEquals
            ( msg+"length differs.",
              expected.length,
              actual.length
              );
        
        for( int i=0; i<expected.length; i++ ) {
            
            assertEquals
                ( msg+"values differ: index="+i,
                   expected[ i ],
                   actual[ i ]
                 );
            
        }
        
    }

    /**
	 * <p>
	 * A logical row identifier together with the data inserted for that record.
	 * </p>
	 * <p>
	 * This class is aware of the {@link BufferedRecordInstallManager} and uses
	 * it to resolve buffered records given a logical row identifier before
	 * attempting to resolve them against the {@link PhysicalRowIdManager}.
	 * </p>
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
	 * @version $Id$
	 */
    static class Record {
    	final long recid;
    	byte data; // data value (replicated for len bytes).
    	int len; // data length.
    	int maxCapacity;
    	int nupdates = 0;
    	
    	/**
		 * Caches the recid, the {@link Location logical row identifier}, the
		 * currently assigned {@link Location physical row identifier}, and the
		 * current capacity of the assigned physical row.
		 * 
		 * @param recid
		 * @param data A byte whose value is replicated <i>len</i> times.
		 * @param len The length of the data in the record.
		 * @throws IOException
		 */
    	public Record(long recid,byte data,int len) throws IOException {
    		if( recid <= 0L ) {
    			throw new IllegalArgumentException();
    		}
    		if( len <= 0 ) {
    			throw new IllegalArgumentException("non-positive length");
    		}
    		this.recid = recid;
    		this.data = data;
    		this.len = len;
    		this.maxCapacity = len;
    	}
    	
    	/**
		 * Notes that the contents of the record have been updated and tracks
		 * the maximum capacity achieved by the record.
		 * 
		 * @param data
		 *            A byte whose value is replicated <i>len</i> times.
		 * @param len
		 *            The length of the data in the record.
		 */
    	public void update(byte data,int len) 
    		throws IOException
    	{
    		if( len <= 0 ) {
    			throw new IllegalArgumentException("non-positive length");
    		}
    		this.data = data;
    		this.len = len;
    		if( len > maxCapacity ) {
    			maxCapacity = len;
    		}
    		nupdates++;
    	}
    }
    
    /**
	 * Serialize for byte[] data. The serializer does not transform the data.
	 * This makes it possible to attempt to serializer a zero length record.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
	 * @version $Id$
	 */
    static class MyNopSerializer implements Serializer, Stateless {

		private static final long serialVersionUID = 4732861135901281444L;

		static Serializer INSTANCE = new MyNopSerializer(); 
		
		public byte[] serialize(Object obj) throws IOException {
			return (byte[])obj;
		}

		public Object deserialize(byte[] data) throws IOException {
			return data;
		}
    	
    }

    /**
	 * <p>
	 * This is a performance test that can be used to page at a time with record
	 * at a time install strategies. A random insert, fetch, update (in place),
	 * update (forcing move), delete, commit, and reopen store operations are
	 * generated using a probability distribution described in the code. For
	 * each inser operation, the test allocates random records using a normal
	 * distribution centered around 128 bytes. The test continues until it has
	 * written 100M (counting both the record header and the record data). At
	 * the end of the test a histogram is written showing the actual
	 * distribution of operations. Some counters are also written if the page at
	 * a time (buffered installs) option was used.
	 * </p>
	 * <p>
	 * jdbm options must be specified using the JVM <code>-Dname=version</code>
	 * argument. The most relevant options for testing include:
	 * <ul>
	 * <li>{@link RecordManagerOptions#BUFFERED_INSTALLS} - defaults to "true"
	 * unless overriden.</li>
	 * <li>{@link RecordManagerOptions#CACHE_TYPE} - The object cache is not
	 * defaulted by this test, so the jdbm default will be used. However, you
	 * should be aware that while the page at a time strategy is integrated with
	 * the {@link BaseRecordManager}, applications normally run using the
	 * object cache. Further, the object cache delays installs until objects are
	 * evicted from cache, so using the object cache provides additional
	 * buffering which both has a performance impact and could hide problems
	 * related to buffering. </li>
	 * <li>{@link RecordManagerOptions#LAZY_INSERT} - This is defaulted by the
	 * test to "true" unless overridden. This option introduces additional
	 * buffering by the object cache (when the object cache is used). In
	 * particular, insert operations are buffered by the object cache where
	 * otherwise they are immediately turned into insert operations against the
	 * {@link BaseRecordManager}.</li>
	 * </ul>
	 * </p>
	 * 
	 * @param args
	 * @throws Exception
	 */
    public static void main(String[] args ) throws Exception {

    	/*
		 * The maximum record size. A normal distribution is used with a mean of
		 * 1/2 of this value and a limit of this value minus one.
		 */
    	final int maxRecordSize = 256;
    	
    	/*
		 * The maximum #of bytes written by record insert or update operations.
		 */
    	final int maxBytesWritten = 100 * 1024 * 1024; // 100M.

    	/*
		 * The relative distribution of insert, fetch, update(in place),
		 * update(forces move), delete, commit, and reopen operations.
		 */
    	final Op gen = new Op(.6f,.2f,.2f,.1f,.01f,.001f,.0001f);
    	
    	// Access to helper methods.
    	TestBufferedRecordInstallManager test = new TestBufferedRecordInstallManager();
    	
    	/*
		 * Inherit default properties from the environment and JVM -Dname=value
		 * args.
		 */
        Properties properties = new Properties(System.getProperties());
        
        /*
		 * Default some properties. We set them unless they have been explictly
		 * set.
		 */
        if( properties.getProperty(RecordManagerOptions.BUFFERED_INSTALLS) == null ) {
        	properties.setProperty(RecordManagerOptions.BUFFERED_INSTALLS, "true");
        }
//        properties.setProperty(RecordManagerOptions.CACHE_TYPE,RecordManagerOptions.NO_CACHE);
//        properties.setProperty(RecordManagerOptions.LAZY_INSERT, "true");

        TestRecordFile.deleteTestFile();
        
        test.openStore( properties );
        
    	test.doManyRecordsTest( maxRecordSize, 0, maxBytesWritten, gen , false );
    	
    	test.tearDown();
    	
    }

}
