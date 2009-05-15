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
/*
 * Created on May 29, 2006
 */

package jdbm.recman;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import jdbm.RecordManagerOptions;

/**
 * <p>
 * This class supports buffered installation writes for jdbm and is designed to
 * interoperate with an interface designating how to cluster objects. The basic
 * principle is that update() operates directed at the base record manager are
 * buffered. Buffered records are tentatively assigned to a bucket. A pool of
 * buckets is maintained and each bucket represents a distinct possible <i>empty</i>
 * page onto which the buffered records could be installed. When a bucket is
 * filled to within a waste margin, the bucket is bulk installed onto an empty
 * page, which is mark as dirty and will be eventually written against the
 * database. The {@link RecordManagerOptions#LAZY_INSERT} feature should be on
 * to use this class as it will defer all insert() operations so that they can
 * be handled as updates().
 * </p>
 * <p>
 * If an update can be done in place, then just do so. Otherwise free the old
 * physical row and insert into the pool of buffered records. Records are
 * installed from the buffer to a page one page at a time. Track the total
 * length of the records in the pool, assigning records to logical target pages
 * based on clustering rules. If the record will not fit in the suggested target
 * page, continue to apply rules trying other target pages. If no target page
 * could hold the record, then create a new target page. If the free space
 * within a target page becomes less than a waste margin, then obtain a free
 * page, mark it as a DATA page, and install all records assigned to that target
 * page onto the page. If a commit signal comes, then combine all partially
 * filled pages into some “minimum” #of pages (respecting the waste margin) and
 * install leftover records per the normal jdbm behavior. If an abort signal
 * comes, then throw away all buffered records and target pools and create a new
 * set of target pools.
 * </p>
 * 
 * @todo Develop support for multiple buckets or at least deferring assignment
 *       of buffered records with a single bucket (for records that are not
 *       appropriate to that bucket), integrate with a clustering API, and write
 *       tests and performance tests (for both clustering data and for the
 *       impact on read performance).
 * 
 * @todo Track waste in recently filled pages (or maintain top 10 pages based on
 *       waste) and fill them in using a modified record-at-once technique. When
 *       filling in waste on pages, be sure to subtract the waste from the
 *       counters and track the reclaimedWaste as well. This information would
 *       be transient and would survive a commit, but not an abort. It would not
 *       survive a database restart. The most appropriate page with waste could
 *       be choosen based on (a) it is still in memory; (b) it clusters well
 *       with the record that needs to be installed; and (c) the record fits
 *       within the available space on the page.
 * 
 * FIXME I am seeing a very weird error from {@link TestStress#testBasics()}
 * with a random seed of 42. The odd thing is that very few distinct entries are
 * appearing in the bucket (4). This only occurs when the CacheRecordManager is
 * NOT used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BufferedRecordInstallManager {

	static boolean DEBUG = false;
	
	/**
	 * Block oriented transactional I/O.
	 */
	final private RecordFile _file; 
	
    /**
     * Logigal to Physical row identifier manager.
     */
    final private LogicalRowIdManager _logMgr;

    /**
     * Physical row identifier manager.
     */
    final private PhysicalRowIdManager _physMgr;

    /**
     * Page manager.
     */
    private PageManager _pageman;

    /**
     * The current bucket that we are filling.
     */
    final private Bucket _bucket;

    /**
	 * The waste margin in bytes that will be accepted. If a bucket is filled to
	 * within this margin of its capacity then it will be installed onto a page.
	 */
    final /*private*/ int _wasteMargin;

    /**
	 * The waste margin in bytes that will be accepted when a partly filled
	 * bucket MUST be installed due to a commit. If a bucket is filled to within
	 * this margin of its capacity then it will be installed onto a page. Otherwise
	 * it will be installed using the standard record at a time strategy.
	 */
    final private int _wasteMargin2;
    
    /**
     * Counters.
     */
    final private Counters _counters = new Counters();

    /**
     * Counters.
     */
    public Counters getCounters() {
    	return _counters;
    }

    /**
	 * Counters. Counters must be explicitly reset using resetCounters().
	 * 
	 * @author thompsonbry
	 * @version $Id$
	 */
    public class Counters {
		
		/**
		 * #of records either inserted or updated (by the recman using our API).
		 */
		/*private*/ int recordsUpdated;

		/**
		 * #of records fetched successfully (by the recman using our API).
		 */
		/*private*/ int recordsFetched;

		/**
		 * #of records deleted successfully (by the recman using our API).
		 */
		/*private*/ int recordsDeleted;

		/**
		 * #of commit operations.
		 */
		/*private*/ int commit;

		/**
		 * #of abort operations.
		 */
		/*private*/ int abort;

		/**
		 * #of buckets existing at this moment and ONE(1) if the algorithm only
		 * fills one bucket at a time.
		 */
		/*private*/ int bucketsExisting;

		/**
		 * #of buckets installed.
		 */
		/*private*/ int bucketsInstalled;
		
		/**
		 * #of buckets installed using a page at once technique.
		 */
		/*private*/ int bucketsInstalledPageAtOnce;

		/**
		 * #of buckets installed using a record at once technique.
		 */
		/*private*/ int bucketsInstalledRecordAtOnce;

		/**
		 * #of records written using a page at once strategy.
		 */
		/*private*/ int recordsInstalledPageAtOnce;

		/**
		 * #of records written using a record at once strategy.
		 */
		/*private*/ int recordsInstalledRecordAtOnce;

		/**
		 * #of records written (whether using page at once or record at once).
		 */
		/*private*/ int recordsInstalled;

		/**
		 * #of bytes written (counting both the record header and the record
		 * data, but not counting incidental storage such as the translation
		 * table or free lists).
		 */
		/*private*/ long bytesWritten;

		/**
		 * Bytes wasted in buckets installed page at once.
		 */
		/*private*/ long bytesWasted;

		public Counters() {
			resetCounters();
		}
		
		/**
		 * Resets internal counters.
		 * 
		 * @see #writeCounters()
		 * @todo track all counters.
		 */
		public void resetCounters() {
			recordsUpdated = recordsFetched = recordsDeleted = 0;
			commit = abort = 0;
			bucketsExisting = 1; // Note: this is fixed by the current algorithm.
			bucketsInstalled = bucketsInstalledPageAtOnce = bucketsInstalledRecordAtOnce = 0;
			recordsInstalledPageAtOnce = 0;
			recordsInstalledRecordAtOnce = 0;
			recordsInstalled = 0;
			bytesWritten = 0L;
			bytesWasted = 0L;
		}

		/**
		 * Writes internal counters on {@link System#err}.
		 * 
		 * @see #resetCounters()
		 */
		public void writeCounters() {
			System.err.println("BufferedRecordInstallManager:: counters");
			System.err.println("records: insertOrUpdate=" + recordsUpdated
					+ ", fetched=" + recordsFetched + ", deleted="
					+ recordsDeleted + ", commit=" + commit + ", abort="
					+ abort);
			System.err.println("records installed: pageAtOnce="
					+ recordsInstalledPageAtOnce + ", recordAtOnce="
					+ recordsInstalledRecordAtOnce + ", total=" + recordsInstalled);
			System.err
					.println("buckets: existing=" + bucketsExisting
							+ ", installed=" + bucketsInstalled
							+ ", installedPageAtOnce="
							+ bucketsInstalledPageAtOnce
							+ ", installedRecordAtOnce="
							+ bucketsInstalledRecordAtOnce);
			System.err
					.println("bytes: written="
							+ bytesWritten
							+ ", wasted="
							+ bytesWasted
							+ (bytesWritten > 0 ? ", "
									+ ((int) (((float) bytesWasted / bytesWritten) * 1000))
									/ 10f + "% wasted"
									: ""));
			System.err.println("wasteMargin1=" + _wasteMargin
					+ ", wasteMargin2=" + _wasteMargin2);
		}
	}
    
// /**
// * The #of buckets to which we will simultaneously be assigning records
// * pending their bulk allocation onto a page. Each bucket represents an
//	 * empty page that will be filled by physical rows.
//	 */
//	final private int _nbuckets;

	/**
	 * @todo javadoc.
	 * @param logMgr
	 *            The logical row manager in use by the store.
	 * @param physMgr
	 *            The physical row manager in use by the store.
//	 * @param nbuckets
//	 *            The #of buckets to which we will simultaneously be assigning
//	 *            records pending their bulk allocation onto a page. Each bucket
//	 *            represents an empty page that will be filled by physical rows.
//	 *            This is a positive integer.
	 */
	public BufferedRecordInstallManager( RecordFile file, LogicalRowIdManager logMgr, PhysicalRowIdManager physMgr, PageManager pageman, int wasteMargin , int wasteMargin2 ) { // , int nbuckets ) {
		if( file == null || logMgr == null || physMgr == null /*|| nbuckets<0*/ || pageman == null ) {
			throw new IllegalArgumentException();
		}
		if( wasteMargin < 0 || wasteMargin > DataPage.DATA_PER_PAGE ) {
			throw new IllegalArgumentException("wasteMargin");
		}
		if( wasteMargin2 < 0 || wasteMargin2 > DataPage.DATA_PER_PAGE ) {
			throw new IllegalArgumentException("wasteMargin2");
		}
		_file = file;
		_logMgr = logMgr;
		_physMgr = physMgr;
		_pageman = pageman;
		_bucket = new Bucket();
		_wasteMargin = wasteMargin;
		_wasteMargin2 = wasteMargin2;
//		_nbuckets = nbuckets;
	}

	/**
	 * Since installation of records on pages is deferred, fetch operations must
	 * invoke this method to test whether the record is currently buffered
	 * awaiting installation on a page.
	 * 
	 * @param logRowId
	 *            The logical row identifier for the record.
	 * 
	 * @return The data for the buffered record or <code>null</code> iff the
	 *         record is not buffered.
	 */

	public byte[] fetch( Location logRowId )
	{
		/*
		 * Note: I am not cloning the data since it is generally copied
		 * through the deserialization process, thereby restricting the
		 * application from modifying the data in the buffer. You can
		 * circumvent that by using a "NOP" serializer that only accepts and
		 * returns byte[]s, but you have to go out of your way to create a
		 * problem so it seems that it is not worth the overhead of cloning
		 * the data here.
		 */
		byte[] data = _bucket.fetch( logRowId );
		if( data != null ) {
			_counters.recordsFetched++;
		}
		return data;
//		byte[] data = _bucket.fetch( logRowId );
//		if( data != null ) {
//			return data.clone();
//		}
//		return null;
	}
	
	/**
	 * Deletes the identified record from the buffer. Does nothing if the record
	 * is not in the buffer.
	 * 
	 * @param logRowId
	 *            The logical row identifier.
	 */
	public void delete( Location logRowId )
	{
		if( _bucket.delete( logRowId ) ) {
			_counters.recordsDeleted++;
		}
	}
	
	/**
	 * <p>
	 * This method must be invoked whenever a new record is inserted or an
	 * existing record is updated. The logic either buffers the record so that
	 * it can be installed on a page as part of an efficient batch operation or
	 * immediately installs the record onto a page. The choice of whether or not
	 * to buffer the record depends on the following criteria:
	 * <ul>
	 * <li>If the physical row for this record already exists and the new data
	 * would fit within the available space in that physical row then the record
	 * is installed immediately onto the pre-existing physical row. This ensures
	 * that in place updates are preferred whenever possible.</li>
	 * <li>If the record would fill a page (within a waste margin) or more than
	 * one page then it is immediately installed by the
	 * {@link PhysicalRowIdManager}. Whether or not in place updates are
	 * performed depends on the {@link PhysicalRowIdManager}.</li>
	 * <li>If the physical row for this record was already allocated and the
	 * record does not exceed various size constraints, then an additional flag
	 * is set and the recordd is buffered. When it is time to install the
	 * buffered record the existing physical row will be deleted before the new
	 * physical row is assigned and the logical to physical row mapping is
	 * updated.</li>
	 * <li>If the physical row for this record was never allocated and the
	 * record does not exceed the various size constraints, then the record will
	 * be buffered.</li>
	 * </ul>
	 * </p>
	 * 
	 * @param logRowId
	 *            The logical row identifier for this record. We will update the
	 *            entry for the logical row identifier in the translation table
	 *            once we install the <i>data</i> into a physical row on a
	 *            page.
	 * @param data
	 *            A serialized record.
	 */

	public void update( final Location logRowId, final byte[] data )
		throws IOException
	{
		if( logRowId == null || data == null ) {
			throw new IllegalArgumentException();
		}
		_counters.recordsUpdated++;
		/*
		 * Lookup the physical row location in the translation table. The result
		 * will be (0,0) if the physical row has not been assigned yet.
		 */
        Location physRowId = _logMgr.fetch( logRowId );

        if( data.length > DataPage.DATA_PER_PAGE ) {
			/*
			 * If the record is too large for a page then install it
			 * immediately.
			 */
			updateOrInsertRecord( logRowId, physRowId, data );
			return;
		}
		
        if (physRowId.getBlock() != 0L) {
			/*
			 * There is a pre-existing physical row for this record. If there is
			 * enough space in the existing physical row to hold the updated
			 * record, then install the record immediately in the pre-existing
			 * physical row.
			 * 
			 * @todo Consider scheduling the test for later if the page is not
			 * in memory. Scheduled tests would be accumulated in another
			 * buffer. The test can be more efficiently performed when we have
			 * multiple tests for a given page, when the page happens to enter
			 * memory for other reasons, or at the commit at the latest.
			 */
			// fetch the record header
			final boolean enoughSpace;
			BlockIo block = _file.get(physRowId.getBlock());
			try {
				RecordHeader header = new RecordHeader(block, physRowId
						.getOffset());
				enoughSpace = data.length <= header.getAvailableSize();
			} finally {
				_file.release(block);
			}
			if( enoughSpace ) {
		        /*
				 * There is enough space, so we update the physical row with
				 * the new data and return immediately.
				 */
		        _physMgr.write( physRowId, data, 0, data.length );
		        return;
			}
		}
		
        /*
         * At this point the record will be buffered.
         */
        
        if( _bucket._avail < ( RecordHeader.SIZE + data.length ) ) {
        	/*
			 * FIXME This presumes that we will buffer records that would put
			 * this bucket over capacity. We can handle that either through a
			 * pool of buckets or by keeping a linked list of unassigned
			 * buffered records and then assigning those first to the new bucket
			 * once we finish with the current bucket. In either case we have to
			 * put a limit on the amount of buffered that will be performed.
			 * That limit could be expressed on the one hand in terms of the
			 * maximum #of buckets that can be allocated and on the other hand
			 * in terms of the maximum #of unassigned buffered records. When the
			 * limit is reached we need to install a bucket.
			 */
//			if( _bucket._avail < _wasteMargin ) {
				/*
				 * The bucket has been filled to within its waste margin so we
				 * install the buffered records onto a page. The bucket is
				 * cleared as a side effect so that it is immediately available
				 * for reuse.
				 */
				installBucketPageAtOnce( _bucket );
//			}
		}
		
        /*
		 * Assign the record to a bucket. It will be buffered until the bucket
		 * fills up, the tx commits or the tx aborts.
		 */
        
		_bucket.add( logRowId, physRowId, data );
		
	}

	/**
	 * This method must be invoked if the transaction aborts. It clears all
	 * buckets and buffered records so that they will not be installed onto the
	 * database.
	 */
	public void abort() {
		
		_counters.abort++;
		
		_bucket.clear();
		
	}
	
	/**
	 * <p>
	 * This method must be invoked if the transaction commits. It examines the
	 * partially filled bucket(s). For each one, it makes a decision whether to
	 * coalesce it with another bucket or directly install the buffered records
	 * into the database using the facilities provided by the
	 * {@link PhysicalRowIdManager}.
	 * </p>
	 * <p>
	 * While using the standard physical row (re-)allocation techniques is
	 * slower, it has the following advantages: (a) it consumes free physical
	 * rows in the database; and (b) it does not leave as much empty space on
	 * the page. Buckets which are only "partly" full therefore get installed
	 * using the standard record at a time mechanisms. The heuristic for
	 * "partly" is a configurable waste parameter. Note that clustering is NOT
	 * possible when using the record at a time strategy!
	 * </p>
	 * 
	 * @see RecordManagerOptions#BUFFERED_INSTALLS_WASTE_MARGIN2
	 */
	public void commit() throws IOException {

		_counters.commit++;
		
		if( _bucket.isEmpty() ) return; // empty bucket.
		
		if( _bucket._avail < _wasteMargin2 ) {
			
			/*
			 * Install the records from the bucket using a batch technique onto
			 * a single page.
			 */
			
			installBucketPageAtOnce( _bucket );
			
		} else {
			
			/*
			 * Install the records from the bucket onto one or more pages using
			 * the record at a time strategy.
			 */
			
			installBucketRecordAtOnce( _bucket );
			
		}
		
	}
	
	/**
	 * <p>
	 * Installs the buffered records in the bucket onto a new page and then
	 * clears the bucket.
	 * </p>
	 * <p>
	 * An empty data page is obtained. If there is a page on the free list, then
	 * we use the first such page. Otherwise a new page is allocated at the end
	 * of the store. The buffered records assigned to the bucket are then
	 * written using a batch oriented technique onto the data page. Finally, the
	 * page is marked dirty and released. Eventually it will be flushed from
	 * cache and written onto the database.
	 * </p>
	 * 
	 * @param bucket
	 *            The bucket to be installed.
	 * 
	 * @return The bucket, which has been cleared and may be recycled.
	 */
	void installBucketPageAtOnce(Bucket bucket) throws IOException {
		if( bucket.getRecordCount() == 0 ) {
			/*
			 * The bucket must not empty.
			 */
			throw new IllegalStateException();
		}
		_counters.bucketsInstalledPageAtOnce++;
		/*
		 * Create a new data page. The records will be installed with a perfect
		 * fit policy and each record will be packed immediately after the one
		 * before it.
		 */
		long blockId = _pageman.allocate(Magic.USED_PAGE);
		BlockIo block = _file.get(blockId);
		try {
			DataPage curPage = DataPage.getDataPageView(block);
			curPage.setFirst(DataPage.O_DATA);
			/*
			 * Install records onto that page.
			 */
			int avail = DataPage.DATA_PER_PAGE;
			short offset = DataPage.O_DATA;
			int nrecs = 0;
			Iterator itr = bucket.iterator();
			while( itr.hasNext() ) {
				// next buffered record.
				BufferedRecord rec = (BufferedRecord) itr.next();
				// logical row identifier for the record.
				Location logRowId = rec.getLogicalRowId();
				/*
				 * If the record was previously installed on a page then we
				 * first delete the record from its old page.
				 */
				Location oldPhysRowId = rec.getPhysicalRowId();
				if( ! oldPhysRowId.isZero() ) {
					_physMgr.delete(oldPhysRowId);
				}
				/*
				 * This is the new physical row location for installation of
				 * record.  It installs the record onto the current block at
				 * the current offset.
				 */
				Location physRowId = new Location( blockId, offset );
				/*
				 * Updates translation table entry to point at the new physical
				 * row.
				 */
	            _logMgr.update( logRowId, physRowId );
	            /* 
	             * Create a view on record header and set its metadata.
	             */
				RecordHeader hdr = new RecordHeader(block, offset);
				byte[] data = rec.getData();
				final int len = data.length;
				hdr.setAvailableSize( len );
				hdr.setCurrentSize( len );
				// copy data into block.
				short dataOffset = (short) (offset + RecordHeader.SIZE);
				System.arraycopy(data, 0, block.getData(), dataOffset, len);
				/*
				 * Update counters and do sanity checks.
				 */
				int size = RecordHeader.SIZE + len;
				offset += size;
				avail -= size;
				nrecs++;
				if( avail < 0 ) {
					throw new AssertionError();
				}
				if( offset > RecordFile.BLOCK_SIZE ) {
					throw new AssertionError();
				}
			}
			if( DEBUG ) 
			System.err.println("Installed " + nrecs + " records on page="
					+ blockId + ", waste=" + avail);
			_counters.recordsInstalled+=nrecs;
			_counters.recordsInstalledPageAtOnce+=nrecs;
			_counters.bucketsInstalled++;
			_counters.bucketsInstalledPageAtOnce++;
			_counters.bytesWritten+=bucket._size;
			_counters.bytesWasted+=bucket._avail;
			/*
			 * Mark the block as dirty and release it.
			 */
			_file.release(blockId, true);
		} catch (Throwable t) {
			/*
			 * If anything goes wrong, then release the block (it is not marked
			 * as dirty) and masquerade and rethrow the exception.
			 */
			_file.release(block);
			IOException ex = new IOException();
			ex.initCause(t);
			throw ex;
		}
		/*
		 * Clear the bucket now that the buffered records have been installed.
		 */
		bucket.clear();
	}

	/**
	 * Install each buffered record in the bucket one at a time into the
	 * database using the standard physical row (re-)allocation technique and
	 * then clears the bucket.
	 */
	void installBucketRecordAtOnce(Bucket bucket) throws IOException {
		if( bucket.getRecordCount() == 0 ) {
			/*
			 * The bucket must not empty.
			 */
			throw new IllegalStateException();
		}
		_counters.bucketsInstalledRecordAtOnce++;
		int nrecs = 0;
		Iterator itr = _bucket.iterator();
		while (itr.hasNext()) {
			// A record to be installed.
			BufferedRecord rec = (BufferedRecord) itr.next();
			// The logical row identifier.
			Location logRowId = rec.getLogicalRowId();
			// The existing physical row.
			Location physRowId = _logMgr.fetch(logRowId);
			/*
			 * Algorithm detects a non-existing physical row from a lazy insert
			 * and allocates a physical row. If the physical row exists, then it
			 * will be reused if it has sufficient capacity and otherwise
			 * reallocated.
			 */
			final Location newPhysRowId;
			byte[] data = rec.getData();
			if (physRowId.getBlock() == 0L) {
				/*
				 * The physical row does not exist (insert as performed by the
				 * cache layer defers allocation of the physical record).
				 */
				newPhysRowId = _physMgr.insert(data, 0, data.length);
			} else {
				/*
				 * The physical row exists (record was either inserted by base
				 * recman or already updated).
				 */
				newPhysRowId = _physMgr.update(physRowId, data, 0, data.length);
			}
			if (!newPhysRowId.equals(physRowId)) {
				/*
				 * Since the physical row was changed we need to update the
				 * mapping from the logical row onto the physical row.
				 */
				_logMgr.update(logRowId, newPhysRowId);
			}
			nrecs++;
		}
		if( DEBUG )
		System.err.println("Installed " + nrecs
				+ " records using record at a time allocation.");
		_counters.recordsInstalled+=nrecs;
		_counters.recordsInstalledRecordAtOnce+=nrecs;
		_counters.bucketsInstalled++;
		_counters.bucketsInstalledRecordAtOnce++;
		_counters.bytesWritten+=bucket._size;
		/*
		 * Clear the bucket now that the buffered records have been installed.
		 */
		bucket.clear();
	}
	
	/**
	 * Installs an update of single record. If the physical row exists, then it
	 * will be reused if it has sufficient capacity and otherwise reallocated.
	 * 
	 * @param logRowId
	 *            The logical row identifier.
	 * @param physRowId
	 *            The physical row identifier and (0,0) if the record has never
	 *            been installed on a page.
	 * @param data
	 *            The new record's data.
	 */
	private void updateOrInsertRecord( final Location logRowId, final Location physRowId, final byte[] data )
		throws IOException
	{
        final Location newRecid;
        if( physRowId.getBlock() == 0L ) {
            // physical row does not exist (insert as performed by the cache layer defers
            // allocation of the physical record).
            newRecid = _physMgr.insert( data, 0, data.length );
        } else {
            // physical row exists (record was either inserted by base recman or already
            // updated).
            newRecid = _physMgr.update( physRowId, data, 0, data.length );
        }
        if ( ! newRecid.equals( physRowId ) ) {
            _logMgr.update( logRowId, newRecid );
        }
	}
	
	/**
	 * A bucket into which we collect buffered records. The bucket represents a
	 * page that could be filled by those records. When the bucket is fill
	 * (within a waste margin) it gets installed onto a new data page.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
	 * @version $Id$
	 * 
	 * @see BufferedRecordInstallManager
	 * @see BufferedRecord
	 */
	final static class Bucket
	{
		/**
		 * Map from the logical row identifier {@link Location} to the {@link
		 * BufferedRecord} for that {@link Location}.
		 */
		final private HashMap _recs = new HashMap();

//		/**
//		 * Set of logical row identifiers {@link Location}s that were inserted
//		 * into the bucket and subsequently deleted from the bucket (using its
//		 * public API).
//		 */
//		final private HashSet _deleted = new HashSet();
		
		/**
		 * The capacity of the page that will be filled by the bucket.
		 */            
		final static int _capacity = DataPage.DATA_PER_PAGE;
		/**
		 * Space available in the bucket.
		 */
		int _avail = _capacity;
		/**
		 * Current size of the bucket (sum of the length of the buffered records
		 * assigned to the bucket plus the size of their record headers).
		 */
		int _size = 0;
		
		/**
		 * Create a new, empty bucket.
		 */
		Bucket() {
		}

		/**
		 * Clears the state of the bucket so that it may be reused. 
		 */
		void clear() {
			_avail = _capacity;
			_size = 0;
			_recs.clear();
//			_deleted.clear();
		}
		
		/**
		 * Assigns a buffered record to this bucket. If there was already a
		 * buffered record in the bucket for the same logical row identifier
		 * then that buffered record is replaced.
		 * 
		 * @param logRowId
		 *            The logical row identifier for the buffered record.
		 * 
		 * @param physRowId
		 *            The physical row identifier for the buffered record or
		 *            (0,0) if the buffered record was never installed on a
		 *            page.
		 * 
		 * @param data
		 *            The data for that record.
		 * 
		 * @exception IllegalStateException
		 *                if adding the record would put this bucket over
		 *                capacity.
		 * 
		 * @return true iff the record replaces a previous version for the same
		 *         logical row id.
		 */
		boolean add(final Location logRowId, final Location physRowId, final byte[] data ) {
			if( logRowId == null || physRowId == null || data == null ) {
				throw new IllegalArgumentException();
			}
			/*
			 * Compute the space required on a data page to hold this record,
			 * including both the record header and the data itself.
			 */
			final int len = RecordHeader.SIZE + data.length;
			if( len > _avail ) {
				throw new IllegalStateException();
			}
			boolean deleted = _delete( logRowId );
			if( _recs.put(logRowId, new BufferedRecord(logRowId, physRowId, data)) != null) {
				throw new AssertionError();
			}
			_size += len;
			_avail -= len;
			if( _size > _capacity ) {
				throw new AssertionError();
			}
			if( _avail < 0 ) {
				throw new AssertionError();
			}
			return deleted; // implies that we replace the previous entry.
		}
		
		/**
		 * Visits the {@link BufferedRecord}s in this bucket. 
		 */
		public Iterator iterator() {
			return _recs.values().iterator();
		}
		
		/**
		 * The #of buffered records in this bucket.
		 */
		public int getRecordCount() {
			return _recs.size();
		}
		
		public boolean isEmpty() {
			return _size == 0;
		}
		
		/**
		 * Fetch the data for the record iff the record is buffered by this
		 * bucket.
		 * 
		 * @param logRowId
		 *            The logical record identifier.
		 * 
		 * @return The data or null if that record is not buffered by this
		 *         bucket.
		 */
		public byte[] fetch( Location logRowId ) {
			if( logRowId == null ) {
				throw new IllegalArgumentException();
			}
			BufferedRecord rec = (BufferedRecord) _recs.get(logRowId);
			if( rec == null ) {
				return null;
			}
			return rec.getData();
		}

		/**
		 * Attempts to delete a buffered record and updates the various counters
		 * (size, available, #of records).
		 * 
		 * @param logRowId
		 *            The logical row identifier.
		 * 
		 * @return true iff the record was deleted from the bucket.
		 * 
//		 * @throws IllegalStateException
//		 *             If the record was historically buffered but has since
//		 *             been deleted. This condition is essentially a double
//		 *             delete, or alternatively a delete against an invalid
//		 *             record identifier.
		 */
		public boolean delete(Location logRowId) throws IllegalStateException {

			return _delete( logRowId );
//			if( _deleted.contains( logRowId ) ) {
//				throw new IllegalStateException("record was already deleted");
//			}
//			
//			if( _delete( logRowId ) ) {
//				
//				if( ! _deleted.add( logRowId ) ) {
//					
//					throw new AssertionError();
//					
//				}
//			
//				return true;
//				
//			}
//			
//			return false;
			
		}
		
		
		/**
		 * Deletes the identified buffered record from this bucket and updates
		 * the various counters (size, available, #of records). Does nothing if
		 * the identified record is not in this bucket.
		 * 
		 * @param logRowId
		 *            The logical row identifier.
		 * 
		 * @return true iff the record was deleted from the bucket.
		 */
		private boolean _delete( Location logRowId ) {
			if( logRowId == null ) {
				throw new IllegalArgumentException();
			}
			BufferedRecord rec = (BufferedRecord) _recs.remove( logRowId );
			if( rec != null ) {
				/*
				 * Update size and available counters since this record was
				 * removed from the bucket.
				 */
				int size = RecordHeader.SIZE + rec.size();
				_size -= size;
				_avail += size;
				return true;
			}
			return false;
		}
		
	}

	/**
	 * A buffered record. This class keeps the logical row identifier together
	 * with the data for that row.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
	 * @version $Id$
	 * 
	 * @see Bucket#iterator()
	 */
	final static class BufferedRecord
	{
		final private Location _logRowId;
		final private Location _physRowId;
		final private byte[] _data;
		/**
		 * 
		 * @param logRowId
		 *            The logical row identifier for the buffered record.
		 * @param physRowId
		 *            The physical row identifier for the buffered record or
		 *            (0,0) if the record was never installed on a page.
		 * @param data
		 *            The data.
		 */
		BufferedRecord( Location logRowId, Location physRowId, byte[] data ) {
			if( logRowId == null || physRowId == null || data == null ) {
				throw new IllegalArgumentException();
			}
			_logRowId = logRowId;
			_physRowId = physRowId; // may be null.
			_data = data;
		}
		/**
		 * The logical row identifier for the buffered record.
		 */
		public Location getLogicalRowId() {
			return _logRowId;
		}
		/**
		 * The data for the buffered record.
		 */
		public byte[] getData() {
			return _data;
		}
		/**
		 * The length of the buffered record (does not include the record
		 * header, just the data).
		 */
		public int size() {
			return _data.length;
		}
		/**
		 * The physical row identifier for the buffered record or (0,0) if the
		 * buffered record was never installed on a page.
		 */
		public Location getPhysicalRowId() {
			return _physRowId;
		}
		public String toString() {
			return "BR<"+_logRowId+",len="+_data.length+">";
		}
	}
	
}
