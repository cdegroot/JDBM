/*
 * Created on Aug 27, 2005
 */
package jdbm.recman;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Proposed tests for fetch of (a) a record which was never inserted into the
 * store; and (b) a record which has since been deleted from the store.
 * 
 * @author thompsonbry
 */

public class TestFetchAfterDelete
   extends TestCase
{

    /**
     * Attempts to fetch a record which was never inserted into the store.
     * The correct behavior is to return a null since the identified object
     * does not exist in the store.
     */

    public void test_fetchUnknownRecord()
    	throws IOException
    {
        
        RecordManager recman = RecordManagerFactory.createRecordManager( TestRecordFile.testFileName );

        final long recid = recman.insert( "some data" );
        
        recman.commit();
        
        // Attempt to fetch data using a recid that should be invalid.
        
        assertEquals
            ( null,
              recman.fetch( recid + 100 )
              );
        
    }

    /**
     * Proposed test case for the behavior of the {@link RecordManager} interface
     * when a record is fetched which used to exist in the store but has since
     * been deleted.  The current behavior throws an {@link java.io.EOFException}
     * when trying to read from an empty byte[].  I believe that the correct behavior
     * is to detect a read against a recid that is no longer valid and to return null
     * since there is no such object in the store.
     */
    
    public void test_fetchAfterDelete()
    	throws IOException
    {
        
        RecordManager recman = RecordManagerFactory.createRecordManager( TestRecordFile.testFileName );

        Object obj1 = "Hello World!";
        
        final long recid = recman.insert( obj1 );
        
        recman.commit();
        
        assertEquals
            ( "Hello World!",
              recman.fetch( recid )
              );

        recman.delete( recid );
        
        assertEquals
            ( null,
              recman.fetch( recid )
              );
        
    }
   
}
