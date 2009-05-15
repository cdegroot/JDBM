/*

 *  $Id: TestStress.java,v 1.10 2006/05/31 22:28:41 thompsonbry Exp $

 *

 *  Package stress test

 *

 *  Simple db toolkit

 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>

 *

 *  This library is free software; you can redistribute it and/or

 *  modify it under the terms of the GNU Library General Public License

 *  as published by the Free Software Foundation; either version 2

 *  of the License, or (at your option) any later version.

 *

 *  This library is distributed in the hope that it will be useful,

 *  but WITHOUT ANY WARRANTY; without even the implied warranty of

 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU

 *  Library General Public License for more details.

 *

 *  You should have received a copy of the GNU Library General Public License

 *  along with this library; if not, write to the Free Software Foundation,

 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA

 */
package jdbm.recman;

import java.util.Properties;
import java.util.Random;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.ByteArraySerializer;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 * This class contains stress tests for this package.
 * 
 */
public class TestStress extends TestCase {
	public TestStress(String name) {
		super(name);
	}

	public void setUp() {
		TestRecordFile.deleteTestFile();
	}

	public void tearDown() {
		TestRecordFile.deleteTestFile();
	}

	// test parameters
	final int RECORDS = 10000;

	final int MAXSIZE = 500;

	final int ROUNDS = 1 * 1000 * 1000;

	final int RPPROMILLE = ROUNDS / 1000;

	Random rnd = new Random(); // Note: this test used 42 as the seed historically.

	// holder for record data so we can compare
	static class RecordData {
		long rowid;

		int size;

		byte b;

		RecordData(long rowid, int size, byte b) {
			this.rowid = rowid;
			this.size = size;
			this.b = b;
		}

		public String toString() {
			return "slot(" + rowid + ",sz=" + size + ",b=" + b + ")";
		}
	}

	private int getRandomAllocatedSlot(RecordData[] d) {
		int slot = rnd.nextInt(RECORDS);
		while (d[slot] == null) {
			slot++;
			if (slot == RECORDS)
				slot = 0; // wrap
		}
		return slot;
	}

	// holder for root records
	long[] roots = new long[FileHeader.NROOTS];

	private int getRandomAllocatedRoot() {
		int slot = rnd.nextInt(FileHeader.NROOTS);
		while (roots[slot] == 0) {
			slot++;
			if (slot == FileHeader.NROOTS)
				slot = 0; // wrap
		}
		return slot;
	}

	private int getRandomSize(){
		return rnd.nextInt(MAXSIZE-1) + 1; // we must chose a size between 1 and MAXSIZE (0 size is not allowed by record manager) 
	}
	
	/**
	 * 
	 * Test basics
	 * 
	 */
	public void testBasics() throws Exception {
		RecordManager recman;
		
		Properties props = new Properties();
		props.setProperty("jdbm.cache.type", "none");
		//props.setProperty("jdbm.disableTransactions", "true");
		
		recman = RecordManagerFactory
				.createRecordManager(TestRecordFile.testFileName, props);
		// as this code is meant to test data structure calculcations
		// and stuff like that, we may want to disable transactions
		// that just slow us down.
		// mgr.disableTransactions();
		RecordData[] d = new RecordData[RECORDS];
		int recordCount = 0, rootCount = 0;
		int inserts = 0, updates = 0, deletes = 0, fetches = 0;
		int rootgets = 0, rootsets = 0;
		int slot = -1;
		int op = -1; // always start with an insert - otherwise, there is no data to work with
		try {
			for (int i = 1; i < ROUNDS; i++) {
				// get next operation to perform
				op = op == -1 ? 1 : rnd.nextInt(100);
				
				if ((i % RPPROMILLE) == 0)
					System.out.print("\rComplete: " + i / RPPROMILLE
							+ "/" + RPPROMILLE + "th");
				// close and re-open a couple of times during the
				// test, in order to check flushing etcetera.
				if ((i % (ROUNDS / 5)) == 0) {
					System.out.print(" (reopened at round " + i / RPPROMILLE
							+ ")");
					recman.close();
					recman = RecordManagerFactory
							.createRecordManager(TestRecordFile.testFileName, props);
					// recman.disableTransactions();
				}
				// generate a random number and assign ranges to operations:
				// 0-10 = insert, 20 = delete, 30-50 = update, 51 = set root,
				// 52 = get root, rest = fetch.
				if (op <= 10) {
					// INSERT RECORD
					if (recordCount == RECORDS) {
						i -= 1;
						continue;
					}
					slot = 0;
					while (d[slot] != null)
						slot++;

					d[slot] = new RecordData(0, getRandomSize(), 
							(byte) rnd.nextInt());
					d[slot].rowid = recman.insert(TestUtil.makeRecord(
							d[slot].size, d[slot].b), ByteArraySerializer.INSTANCE);
					recordCount++;
					inserts++;
					
				} else if (op == 20) {
					// DELETE RECORD
					if (recordCount == 0) {
						i -= 1;
						continue;
					}
					slot = getRandomAllocatedSlot(d);

					recman.delete(d[slot].rowid);
					d[slot] = null;
					recordCount--;
					deletes++;
				} else if (op <= 50) {
					// UPDATE RECORD
					if (recordCount == 0) {
						i -= 1;
						continue;
					}
					slot = getRandomAllocatedSlot(d);

					d[slot].size = getRandomSize();
					d[slot].b = (byte) rnd.nextInt();
					recman.update(d[slot].rowid, TestUtil.makeRecord(
							d[slot].size, d[slot].b), ByteArraySerializer.INSTANCE);
					updates++;
				} else if (op == 51) {
					// SET ROOT, but not any of the roots that are actually in use!
					int root = Math.max( BaseRecordManager.FIRST_FREE_ROOT, rnd.nextInt(FileHeader.NROOTS) );
					roots[root] = rnd.nextLong();
					recman.setRoot(root, roots[root]);
					rootsets++;
				} else if (op == 52) {
					// GET ROOT
					if (rootCount == 0) {
						i -= 1;
						continue;
					}
					int root = getRandomAllocatedRoot();
					assertEquals("root", roots[root], recman.getRoot(root));
					rootgets++;
				} else {
					// FETCH RECORD
					if (recordCount == 0) {
						i -= 1;
						continue;
					}
					slot = getRandomAllocatedSlot(d);

					byte[] data = (byte[]) recman.fetch(d[slot].rowid,
							ByteArraySerializer.INSTANCE);
					boolean result = TestUtil.checkRecord(data, d[slot].size,
							d[slot].b);
					assertTrue("fetch round=" + i + ", slot=" + slot + ", "
							+ d[slot], result );
					fetches++;
				}
				
			}
			recman.close();
		} catch (Throwable e) {
//			e.printStackTrace();
			AssertionFailedError err = new AssertionFailedError("aborting test at slot " + slot + ": " + e);
			err.initCause( e );
			throw err;
		} finally {
			System.out.println("records : " + recordCount);
			System.out.println("deletes : " + deletes);
			System.out.println("inserts : " + inserts);
			System.out.println("updates : " + updates);
			System.out.println("fetches : " + fetches);
			System.out.println("rootget : " + rootgets);
			System.out.println("rootset : " + rootsets);
			int totalSize = 0;
			for (int i = 0; i < RECORDS; i++)
				if (d[i] != null)
					totalSize += d[i].size;
			System.out.println("total outstanding size: " + totalSize);
			// System.out.println("---");
			// for (int i = 0; i < RECORDS; i++)
			// if (d[i] != null)
			// System.out.println("slot " + i + ": " + d[i]);
		}
	}

	/**
	 * 
	 * Runs all tests in this class
	 * 
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(new TestSuite(TestStress.class));
	}
}
