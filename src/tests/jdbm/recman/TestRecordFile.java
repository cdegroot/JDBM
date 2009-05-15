/*
 *  $Id: TestRecordFile.java,v 1.5 2005/11/08 20:58:27 thompsonbry Exp $
 *
 *  Unit tests for RecordFile class
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

import junit.framework.*;
import java.io.File;
import java.io.IOException;

/**
 *  This class contains all Unit tests for {@link RecordFile}.
 */
public class TestRecordFile
    extends TestCase
{

    public final static String testFileName = "test";

    public TestRecordFile( String name )
    {
        super( name );
    }

    public static void deleteFile( String filename )
    {
        File file = new File( filename );

        if ( file.exists() ) {
            try {
                file.delete();
            } catch ( Exception except ) {
                except.printStackTrace();
            }
            if ( file.exists() ) {
                
                /*
                 * Since the same test file is reused over and over a failure to
                 * remove the old file can cause spurious failures for the tests
                 * that follow. In general this can be traced back to a test
                 * that failed to close the file so that Java is not
                 * willing/able to delete it in the setUp() for the next test.
                 * 
                 * Throwing an exception here generally means that you will
                 * catch the test whose tearDown() left the store open, since
                 * most test do (all should) delete the store file in their
                 * tearDown().
                 * 
                 * @todo We need a means to both report the exception observed
                 * by junit while still being able to tearDown the test.
                 * Unfortunately junit does not support this option for us. We
                 * basically need chained exceptions (multiple exceptions
                 * thrown, not just the initCause). This should be raised as an
                 * issue against junit.
                 */ 
                
//                throw new RuntimeException( "WARNING:  Cannot delete file: " + file );
                System.out.println( "WARNING:  Cannot delete file: " + file );
            }
        }
    }


    public static void deleteTestFile()
    {
        System.gc();

        deleteFile(testFileName);

        deleteFile(testFileName + RecordFile.extension);

        deleteFile(testFileName + TransactionManager.extension);
    }

    public void setUp()
    {
        deleteTestFile();
    }

    public void tearDown()
    {
        deleteTestFile();
    }


    /**
     *  Test constructor
     */
    public void testCtor()
        throws Exception
    {
        RecordFile file = new RecordFile( testFileName );
        file.close();
    }


    /**
     *  Test addition of record 0
     */
    public void testAddZero()
        throws Exception
    {
        RecordFile file = new RecordFile( testFileName );
        byte[] data = file.get( 0 ).getData();
        data[ 14 ] = (byte) 'b';
        file.release( 0, true );
        file.close();
        file = new RecordFile( testFileName );
        data = file.get( 0 ).getData();
        assertEquals( (byte) 'b', data[ 14 ] );
        file.release( 0, false );
        file.close();
    }


    /**
     *  Test addition of a number of records, with holes.
     */
    public void testWithHoles()
        throws Exception
    {
        RecordFile file = new RecordFile( testFileName );

        // Write recid 0, byte 0 with 'b'
        byte[] data = file.get( 0 ).getData();
        data[ 0 ] = (byte) 'b';
        file.release( 0, true );

        // Write recid 10, byte 10 with 'c'
        data = file.get( 10 ).getData();
        data[ 10 ] = (byte) 'c';
        file.release( 10, true );

        // Write recid 5, byte 5 with 'e' but don't mark as dirty
        data = file.get( 5 ).getData();
        data[ 5 ] = (byte) 'e';
        file.release( 5, false );

        file.close();

        file = new RecordFile( testFileName );
        data = file.get( 0 ).getData();
        assertEquals( "0 = b", (byte) 'b', data[ 0 ] );
        file.release( 0, false );

        data = file.get( 5 ).getData();
        assertEquals( "5 = 0", 0, data[ 5 ] );
        file.release( 5, false );

        data = file.get( 10 ).getData();
        assertEquals( "10 = c", (byte) 'c', data[ 10 ] );
        file.release( 10, false );

        file.close();
    }


    /**
     *  Test wrong release
     */
    public void testWrongRelease()
        throws Exception
    {
        RecordFile file = new RecordFile( testFileName );

        // Write recid 0, byte 0 with 'b'
        byte[] data = file.get( 0 ).getData();
        data[ 0 ] = (byte) 'b';
        try {
            file.release( 1, true );
            fail( "expected exception" );
        } catch ( IOException except ) {
            // ignore
        }
        file.release( 0, false );

        file.close();

        // @alex retry to open the file
        /*
        file = new RecordFile( testFileName );
        PageManager pm = new PageManager( file );
        pm.close();
        file.close();
        */
    }


    /**
     *  Runs all tests in this class
     */
    public static void main( String[] args )
    {
        junit.textui.TestRunner.run( new TestSuite( TestRecordFile.class ) );
    }
}
