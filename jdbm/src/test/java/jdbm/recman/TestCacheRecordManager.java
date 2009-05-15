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
 */
/*
 * Created on May 3, 2006
 */
package jdbm.recman;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Random;

import jdbm.helper.CachePolicy;
import jdbm.helper.MRU;
import jdbm.helper.WeakCache;
import junit.framework.TestCase;

/**
 * Tests cache semantics at the record manager level.
 * 
 * @version $Id: TestCacheRecordManager.java,v 1.2 2006/05/03 16:11:47 thompsonbry Exp $
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class TestCacheRecordManager extends TestCase {

    public TestCacheRecordManager() {
        super();
    }
    
    /**
     * @param string The name of the test.
     */
    public TestCacheRecordManager(String string) {
        super(string);
    }

    /**
     * Test verifies that an object that is strongly held will not be evicted
     * from a weak cache. The test sets up a {@link WeakReference}indicator
     * object to determine when the garbage collector has swept weak references.
     * It then populates the cache with several large objects by inserting them
     * into the store. The test holds a hard reference to the first object
     * inserted into the store, so it SHOULD NOT be possible for the weak cache
     * entry for that object to be cleared. The test then attempts to force the
     * garbage collector to clear weak references by continuing to insert new
     * large objects into the store (thus overflowing both the internal MRU hard
     * reference cache and the available memory for the JVM) and verifies (a)
     * the weakly reachable cache entries are cleared and (b) that the cache
     * entry for the object that was strongly held by the test harness was not
     * cleared.
     */
    
    public void test_weakCache()
    	throws IOException
    {

        /*
         * You can set this flag to true in order to verify that the test is testing
         * what it is supposed to be testing.  When true, the test does not hold a
         * hard reference and the expected behavior is that the corresponding cache
         * entry gets cleared.
         */
        final boolean testTest = false;
        
        /*
         * The maximum #of large objects that we will insert into the store during
         * this test.  If the GC does not force references to be cleared before
         * this limit it reached then the test is inconclusive and should be re-run
         * with less available memory.
         */
        final int MAX_TRIALS = 100;
                
        /*
         * Setup cache policy. We use a fairly small MRU cache size so that
         * objects are quickly evicted from the internal cache.
         */
        final int capacity = 5;
        final CachePolicy internal = new MRU(capacity);
        final CachePolicy level2 = new WeakCache(internal);

        // create recman using that cache policy.
        CacheRecordManager recman = new CacheRecordManager(new BaseRecordManager(getName()),level2);

        /*
         * Since soft/weak reference clears are not predictable, we use an
         * indicator.
         * 
         * Note: Some JVMs monitor when [tmp] is no longer reachable and will
         * permit the GC to clear the weak reference even through it is in
         * lexical scope since it can no longer be used given the program
         * dynamics. For this reason we explicitly clear [tmp := null] once the
         * cache is at capacity and also test the value of tmp when we are
         * verifing the post-condition for the test. The result is that the #of
         * trials actually required by the test has a lower bound of the inner
         * (MRU) cache size.
         */
        final WeakReference indicator = new WeakReference(createLargeObject());
        Object tmp = indicator.get();
        assertNotNull("indicator is null", tmp);

        // hold hard reference to one of the objects that we insert into the store.
        Object hardRef = null;
        long[] recids = new long[MAX_TRIALS];

        int trial;
        for( trial=0; trial<MAX_TRIALS; trial++) {

        		System.gc();
                Object obj = createLargeObject();
                recids[ trial ] = recman.insert( obj );
                if( trial == 0 ) {
                    /* Hold a hard reference to the first object inserted into the
                     * record manager and also note the assigned record identifier.
                     */
                    if( ! testTest ) {
                        hardRef = obj;
                    }
                }
                if( trial > capacity ) {
                    /*
                     * We do not permit the indicator to be cleared until the
                     * cache is over capacity. Once the cache is over capacity
                     * objects are evicted by the internal MRU policy. Since the
                     * internal MRU policy holds hard references to those
                     * objects it is not possible to clear a weak reference for
                     * objects until they have been evicted from the MRU.
                     */
                    tmp = null; // after this the indicator can now be cleared.
                }
                if( indicator.get() == null ) {

                    /*
                     * Since the indicator was cleared we have reason to believe
                     * that weak references were cleared by the garbage
                     * collector. At this point we can test the weak cache and
                     * verify that it still holds a reference to the object for
                     * which this test is holding a hard reference.
                     */

                    assertEquals("tmp not null", null, tmp );

                    if( testTest ) {
                        
                        assertNull("entry was not cleared", level2.get(new Long(recids[0])));
                        
                    } else {
                    
                        assertNotNull("entry was cleared", level2.get(new Long(recids[0])));

                        assertEquals("value is wrong", hardRef, level2.get(new Long(recids[0])));
                        
                    }

                    System.err.println("#trials="+trial);
                    
                    return; // success.
                    
                }
                
        }

        fail("Indicator was never cleared: #trials="+trial);
        
    }

    /**
     * Clones and returns a 20M array of random data.
     */
    protected Object createLargeObject() {
        byte[] data = new byte[ _data.length ];
        System.arraycopy(_data,0,data,0,_data.length);
        return data;
    }

    /**
     * 1M of random data.
     */
    private byte[] _data = null;
    
    public void setUp() throws Exception
    {
        super.setUp();
        final int size = 1024 * 1024 * 1; // 1M.
        _data = new byte[ size ];
        Random r = new Random();
        r.nextBytes( _data );
    }
}
