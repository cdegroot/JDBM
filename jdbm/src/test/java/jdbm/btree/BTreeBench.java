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

package jdbm.btree;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.LongComparator;
import jdbm.helper.LongPackedSerializer;
import jdbm.helper.LongSerializer;
import jdbm.recman.TestRecordFile;
import junit.framework.TestCase;

import java.util.Enumeration;
import java.util.Hashtable;

import java.io.IOException;

/**
 *  Random insertion/removal test for B+Tree data structure.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: BTreeBench.java,v 1.7 2005/11/08 20:58:26 thompsonbry Exp $
 */
public class BTreeBench extends TestCase {

    RecordManager recman;

    public void setUp() throws IOException {
        TestRecordFile.deleteTestFile();
        recman = RecordManagerFactory.createRecordManager( TestRecordFile.testFileName );
    }

    /**
     * Test w/o compression or specialized key or value serializers.
     * 
     * @throws IOException
     */
    
    public void test_001() throws IOException {
        BTree tree = BTree.createInstance( recman, new LongComparator(), null, null, 32 );
        doTest( recman, tree, 5001 );
        recman.close();
    }
    
    /**
     * Test w/ compression and specialized (long) key and value serializers.
     * 
     * @throws IOException
     */
    
    public void test_002() throws IOException {
        BTree tree = BTree.createInstance( recman, new LongComparator(), new LongSerializer(), new LongSerializer(), 32 );
        doTest( recman, tree, 5001 );
        recman.close();
    }
    
    /**
     * Test w/ compression and packed long key and value serializers.
     * 
     * @throws IOException
     */
    
    public void test_003() throws IOException {
        BTree tree = BTree.createInstance( recman, new LongComparator(), new LongPackedSerializer(), new LongPackedSerializer(), 32 );
        doTest( recman, tree, 5001 );
        recman.close();
    }
    
    public static void main( String[] args ) {

        int ITERATIONS = 1000000;
        RecordManager recman;
        BTree tree = null;
        
        try {
            recman = RecordManagerFactory.createRecordManager( "test" );
            tree = BTree.createInstance( recman, new LongComparator(), null, null, 32 );
            doTest( recman, tree, ITERATIONS );
            recman.close();
        } catch ( Throwable except ) {
            except.printStackTrace();
        }
    }

    public static void doTest( RecordManager recman, BTree tree, int ITERATIONS )
    	throws IOException
    {

        long beginTime = System.currentTimeMillis();
        Hashtable hash = new Hashtable();

        for ( int i=0; i<ITERATIONS; i++) {
            Long random = new Long( random( 0, 64000 ) );

            if ( ( i % 5000 ) == 0 ) {
                long elapsed = System.currentTimeMillis() - beginTime;
                System.out.println( "Iterations=" + i + " Objects=" + tree.entryCount()+", elapsed="+elapsed+"ms" );
                recman.commit();
            }
            if ( hash.get( random ) == null ) {
                //System.out.println( "Insert " + random );
                hash.put( random, random );
                tree.insert( random, random, false );
            } else {
                //System.out.println( "Remove " + random );
                hash.remove( random );
                Object removed = (Object) tree.remove( random );
                if ( ( removed == null ) || ( ! removed.equals( random ) ) ) {
                    throw new IllegalStateException( "Remove expected " + random + " got " + removed );
                }
            }
            // tree.assertOrdering();
            compare( tree, hash );
        }

    }
    
    static long random( int min, int max ) {
        return Math.round( Math.random() * ( max-min) ) + min;
    }

    static void compare( BTree tree, Hashtable hash ) throws IOException {
        boolean failed = false;
        Enumeration enumeration;

        if ( tree.entryCount() != hash.size() ) {
            throw new IllegalStateException( "Tree size " + tree.entryCount() + " Hash size " + hash.size() );
        }

        enumeration = hash.keys();
        while ( enumeration.hasMoreElements() ) {
            Object key = (Object) enumeration.nextElement();
            Object hashValue = hash.get( key );
            Object treeValue = tree.find( key );
            if ( ! hashValue.equals( treeValue ) ) {
                System.out.println( "Compare expected " + hashValue + " got " + treeValue );
                failed = true;
            }
        }
        if ( failed ) {
            throw new IllegalStateException( "Compare failed" );
        }
    }

}
