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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 * $Id: TestStringTable.java,v 1.1 2005/10/04 17:03:17 thompsonbry Exp $
 */
/*
 * Created on Sep 26, 2005
 */
package jdbm.strings;

import junit.framework.TestCase;

import jdbm.RecordManager;
import jdbm.recman.BaseRecordManager;
import jdbm.recman.TestRecordFile;

import java.io.IOException;
import java.util.Random;

/**
 * Test cases for interning and disinterning of strings.<p>
 * 
 * @author thompsonbry
 */

public class TestStringTable
    extends TestCase
{

    RecordManager recman = null;
    StringTable stbl = null;
    long tableId = 0L;

    public void setUp()
    	throws IOException
    {

        TestRecordFile.deleteTestFile();

        recman = new BaseRecordManager( TestRecordFile.testFileName );

        stbl = StringTable.createInstance( recman );
        
        tableId = recman.insert( stbl );
        
    }

    public void tearDown()
    	throws IOException
    {
        
        recman.close();

        TestRecordFile.deleteTestFile();

    }

    public RecordManager reopenStore()
    	throws IOException
    {
        
        System.err.println( "Doing commit." );
        recman.commit();
        
        System.err.println( "Closing store." );
        recman.close();

        System.err.println( "Re-opening store." );
        
        recman = new BaseRecordManager( TestRecordFile.testFileName ); 

        System.err.println( "Fetching string table." );
        
        stbl = StringTable.load( recman, tableId );
        
        return recman;
        
    }

    /**
     * Test helper produces a random sequence of indices in the
     * range [0:n-1] suitable for visiting the elements of an
     * array of n elements in a random order. 
     */
    
    public static int[] getRandomOrder( final int n )
    {

	final class Pair
	    implements Comparable
	{
	    public double r = Math.random();
	    public int val;
	    public Pair( int val ) {this.val = val;}
	    public int compareTo(Object other)
	    {
		if( this == other ) return 0;
		if( this.r < ((Pair)other).r ) return -1;
		else return 1;
	    }

	}

	Pair[] pairs = new Pair[ n ];

	for( int i=0; i<n; i++ ) {

	    pairs[ i ] = new Pair( i );

	}

	java.util.Arrays.sort( pairs );

	int order[] = new int[ n ];
	
	for( int i=0; i<n; i++ ) {

	    order[ i ] = pairs[ i ].val;

	}

	return order;
	
    }

    /**
     * Random number generator used by {@link #getNormalInt( int range
     * )}.
     */

    private Random m_random = new Random();

    /**
     * Returns a random integer normally distributed in [0:range].
     */

    public int getNormalInt( int range )
    {

	final double bound = 3d;        
	
	double rand = m_random.nextGaussian();
	
	if( rand < -bound ) rand = -bound;
	else if( rand > bound ) rand = bound;
	
	rand = ( rand + bound ) / ( 2 * bound );
				// normal distribution in [0:1].
	
	if( rand < 0d ) rand = 0d;
				// make sure it is not slightly
				// negative
	
	int r = (int)( rand * 1024 );

	return r;

    }

    /**
     * Returns a random but unique string of Unicode characters with a
     * maximum length of len and a minimum length.
     *
     * @param len The maximum length of the string.  Each generated
     * literal will have a mean length of <code>len/2</code> and the
     * lengths will be distributed using a normal distribution (bell
     * curve).
     *
     * @param id A unique index used to obtain a unique string.
     * Typically this is a one up identifier.
     */

    public String getRandomString( int len, int id )
    {

//	final String data = "0123456789!@#$%^&*()`~-_=+[{]}\\|;:'\",<.>/?QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";
	final String data = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";

	int[] order = getRandomOrder( data.length() );
	
	int n = getNormalInt( len );
	
	StringBuffer sb = new StringBuffer( n );

	int index = 0;
	
	for( int i=0; i<n; i++ ) {

	    sb.append( data.charAt( order[ index++ ] ) );

	    if( index == order.length ) {
	        
	        index = 0;
	        
	    }
	    
	}
 	        
	sb.append( id );

	return sb.toString();

    }

    /**
     * Tests intern() and disintern().
     */
    
    public void test_intern_001()
    	throws IOException
    {
        
        final String testString = "Hello World!";
        
        int stringId = stbl.intern( testString, false );

        // Expected value when not interned and insert == false is zero.
        assertEquals( 0, stringId );
        
        stringId = stbl.intern( testString, true );

        // Verify disintern.
        assertEquals( testString, stbl.disintern( stringId ) );
        
        // The expected value for the first interned string is one (1).
        assertEquals( 1, stringId );

        // close and reopen the store and reverify disintern.
        reopenStore();
        
        // Verify disintern.
        assertEquals( testString, stbl.disintern( stringId ) );

    }

    /**
     * Tests intern( null, [true|false] ), which should always return
     * -1.
     */

    public void test_intern_002()
    	throws IOException
    {

        assertEquals( StringTable.NULLID, stbl.intern( null, false ) );

        assertEquals( StringTable.NULLID, stbl.intern( null, true ) );
        
    }

    /**
     * Tests disintern of a stringId not generated by intern(),
     * which should result in an {@link IllegalArgumentException}.
     */

    public void test_intern_003()
    	throws IOException
    {

        try {
            
            stbl.disintern( 12 );
            
            fail( "Expecting: "+IllegalArgumentException.class );
            
        }
        
        catch( IllegalArgumentException ex ) {
            
            System.err.println
               ( "Ignoring expected exception: "+ex
                  );

        }
        
    }
    
    /**
     * Tests disintern of zero (0), which should throw an {@link
     * IllegalArgumentException}.
     */

    public void test_intern_004()
    	throws IOException
    {

        try {
            
            stbl.disintern( 0 );
            
            fail( "Expecting: "+IllegalArgumentException.class );
            
        }
        catch( IllegalArgumentException ex ) {
            
            System.err.println( "Ignoring expected exception: "+ex );
            
        }
        
    }
    
    /**
     * Tests intern() and disintern() on a large #of randomly generated
     * strings.
     */

    public void test_intern_005()
    	throws IOException
    {
 
        final int nstrings = 1000;
        
        String[] testStrings = new String[ nstrings ];

        int[] stringIds = new int[ nstrings ];
        
        final int avglen = 100;
        
        for( int i=0; i<testStrings.length; i++ ) {
            
            testStrings[ i ] = getRandomString( avglen, i );
            
        }

        // intern all of the test strings and save the generated
        // stringIds.
        for( int i=0; i<testStrings.length; i++ ) {
            
            stringIds[ i ] = stbl.intern( testStrings[ i ], true );
            
        }
        
        // verify that we can disintern all of the test strings.
        for( int i=0; i<testStrings.length; i++ ) {
            
            assertEquals
               ( testStrings[ i ],
                 stbl.disintern( stringIds[ i ] )
                 );
            
        }

        // close and reopen the store.
        recman = reopenStore();

        // use a random order to disintern the test strings.
        int[] order = getRandomOrder( testStrings.length );

        // verify that we can disintern all of the test strings.
        for( int i=0; i<testStrings.length; i++ ) {
            
            assertEquals
               ( testStrings[ order[ i ] ],
                 stbl.disintern( stringIds[ order[ i ] ] )
                 );
            
        }
        
    }

}
