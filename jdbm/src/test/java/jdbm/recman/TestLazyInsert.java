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
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: TestLazyInsert.java,v 1.3 2006/05/15 14:42:13 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.helper.ICacheEntry;

import junit.framework.TestCase;

/**
 * Test lazy insert feature of the {@link CacheRecordManager}.
 * 
 * @author thompsonbry
 */

public class TestLazyInsert extends TestCase {

    CacheRecordManager recman;

    public TestLazyInsert(String name) {
        super(name);
    }

    public void setUp() {
        TestRecordFile.deleteTestFile();
        try {
            openStore();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void tearDown() {
        //        TestRecordFile.deleteTestFile();
    }

    public void openStore() throws IOException {
        Properties properties = new Properties();
        properties.put
            ( RecordManagerOptions.LAZY_INSERT,
              "true"
              );
        recman = (CacheRecordManager) RecordManagerFactory.createRecordManager
            ( TestRecordFile.testFileName,
              properties
              );
    }

    public void reopenStore() throws IOException {
        recman.commit();
        recman.close();
        openStore();
    }

    /**
     * Test helper extracts the blockId of the physical row allocated to a
     * logical row id. For the lazy insert feature the blockId will be zero
     * until the commit.
     */
    
    public long getBlockId( long recid )
    	throws IOException
    {
        
        BaseRecordManager br = (BaseRecordManager) recman.getRecordManager();
        
        Location logRecid = new Location( recid );
        
        Location physRecid = br._logMgr.fetch( logRecid );
        
        return physRecid.getBlock();
        
    }
    
    /**
     * Test makes sure that an insert followed by a commit flushes the record to
     * the store and that it can be read back in if the store is reopened. (This
     * requires us to mark the record as dirty in the cache when we insert it
     * since we have not actually inserted the physical row for that record.)
     */

    public void test_lazyInsert_001()
    	throws IOException
    {
        
        final String expected = "Hello World!";

        // Insert should allocate a logical row id, place the record into the
        // cache (it should be marked as dirty), and NOT allocate the physical
        // row id.
        final long recid = recman.insert( expected );

        // Verify allocated recid.
        assertTrue( "recid is zero", recid != 0L );
        
        // Verify object is in cache and cache entry is marked as dirty.
//        CacheEntry cacheEntry = (CacheEntry) recman.getCachePolicy().get( new Long(recid) );
//        assertTrue( "cache entry does not exist", cacheEntry != null );
//        assertTrue( "cache entry is not dirty", cacheEntry.isDirty() );
//        assertEquals( "object in cache", expected, cacheEntry.getObject() );
//        assertEquals( "recid in cache", recid, cacheEntry.getRecid() );
        assertEquals("object",expected, recman.getCachePolicy().get( new Long(recid) ) );
        
        // Scan the cache to find the corresponding cache entry.
        boolean found = false;
        Enumeration en = recman.getCachePolicy().entries();
        while( en.hasMoreElements() ) {
            ICacheEntry entry = (ICacheEntry)en.nextElement();
            if( ((Long)entry.getKey()).longValue() == recid ) {
                /*
                 * Found the entry for this object.
                 */
                assertTrue( "dirty", entry.isDirty() );
                found = true;
                break;
            }
        }
        assertTrue("found", found );
        
        // Verify that the blockId is zero (no physical row is allocated).
        assertEquals( "blockId", 0L, getBlockId(recid));
        
        // Verify we can fetch the record.  This should be a cache hit.
        assertEquals( "record", expected, recman.fetch( recid ) );
        
        // Commit - this should force the object to serialized, a physical row should
        // be allocated, and the record should be written into that physical row in
        // addition to the normal commit.
        recman.commit();

        // Verify that the blockId is non-zero (physical row was allocated).
        assertTrue( "blockId", 0L != getBlockId(recid));

        reopenStore();

        // Verify that the blockId is non-zero (physical row was allocated).
        assertTrue( "blockId", 0L != getBlockId(recid));

        // Verify that we can fetch the record.  This time we should be going to
        // disk.
        assertEquals( "record", expected, recman.fetch( recid ) );
        
        recman.close();
        
    }

    /**
     * Test the ability to insert and then update a record. The record is not
     * allocated a physical row until the commit.
     */

    public void test_lazyInsert_002()
	throws IOException
    {
    
        String expected = "Hello World!";

        // Insert should allocate a logical row id, place the record into the
        // cache (it should be marked as dirty), and NOT allocate the physical
        // row id.
        final long recid = recman.insert( expected );

        assertEquals( "record", expected, recman.fetch( recid ) );
        
        // Verify that the blockId is zero (no physical row is allocated).
        assertEquals( "blockId", 0L, getBlockId(recid));

        expected = "Goodbye";
        recman.update( recid, expected );
        
        // Verify that the blockId is zero (no physical row is allocated).
        assertEquals( "blockId", 0L, getBlockId(recid));

        // Commit forces the allocation of the physical row.
        recman.commit();

        // Verify that the blockId is non-zero (physical row was allocated).
        assertTrue( "blockId", 0L != getBlockId(recid));

        recman.close();

    }

    /**
     * Write tests that an inserted object that is never updated may be deleted.
     * Such objects should never have a physical row allocated to them.
     */

    public void test_lazyInsert_003()
    	throws IOException
    {
        
        final String expected = "Hello World!";

        // Insert should allocate a logical row id, place the record into the
        // cache (it should be marked as dirty), and NOT allocate the physical
        // row id.
        final long recid = recman.insert( expected );

        // Verify that the blockId is zero (no physical row is allocated).
        assertEquals( "blockId", 0L, getBlockId(recid));

        assertEquals( "record", expected, recman.fetch( recid ) );
        
        // Verify that the blockId is zero (no physical row is allocated).
        assertEquals( "blockId", 0L, getBlockId(recid));

        recman.delete( recid );

        // Verify record was deleted.
        try {
            recman.fetch( recid );
            fail( "Expecting exception for fetch on deleted record." );
        }
        catch( Throwable t ) {
            System.err.println
                ( "Ignoring expected exception: "+t
                  );
        }
        
        recman.commit();
        
        recman.close();
        
    }
    
}
