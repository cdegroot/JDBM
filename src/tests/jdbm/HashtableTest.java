
package jdbm;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Test case provided by Daniel Herlemont to demonstrate a bug in
 * HashDirectory.  The returned Enumeration got into an infinite loop
 * on the same key/val pair.
 *
 * @version $Id: HashtableTest.java,v 1.6 2005/06/25 23:12:32 doomdark Exp $
 */
public class HashtableTest {

    private RecordManager recman;
    private HTree hashtable;

    private boolean enumerate = false;
    private boolean populate = false;
    private boolean retrieve = false;
    private String jdbmName = "hashtest";
    private String name = "hashtable";
    private String onekey = "onekey";


    /**
     * Initialize RecordManager and HTree
     */
    protected void init()
        throws IOException
    {
        recman = RecordManagerFactory.createRecordManager( jdbmName );

        // create or reload HTree
        long recid = recman.getNamedObject( name );
        if ( recid == 0 ) {
            hashtable = HTree.createInstance( recman );
            recman.setNamedObject( name, hashtable.getRecid() );
        } else {
            hashtable = HTree.load( recman, recid );
        }

    }


    /**
     * Populate HTree with some data
     */
    protected void populate()
        throws IOException
    {
        try {
            int max = 1000;
            for ( int i=0; i<max; i++ ) {
                String key = "key" + i;
                String val = "val" + i;
                hashtable.put( key,val );
                System.out.println( "put key=" + key + " val=" + val );
            }

            System.out.println( "populate completed" );
        } finally {
            recman.close();
        }
    }


    /**
     * Retrieve a given object based on key
     */
    protected Object retrieve( Object key )
        throws IOException
    {
        init();

        try {
            Object val = hashtable.get( key );
            System.out.println( "retrieve key=" + key + " val=" + val );
            return val;
        } finally {
            recman.close();
        }
    }


    /**
     * Enumerate keys and objects found in HTree
     */
    protected void enumerate()
        throws IOException
    {
        init();

        try {
            FastIterator iter = hashtable.keys();
            Object key = iter.next();
            while ( key != null ) {
                Object val = hashtable.get( key );
                System.out.println( "enum key=" + key + " val=" + val );
            }
        } finally {
            recman.close();
        }
    }


    /**
     * Execute commands specified on command-line
     */
    protected void doCommands()
        throws IOException
    {
        if ( enumerate ) {
            enumerate();
        }

        if ( populate ) {
            populate();
        }

        if ( retrieve ) {
            retrieve( onekey );
        }
    }


    /**
     * Parse command-line arguments
     */
    protected void parseArgs( String args[] )
    {
        for ( int argn = 0; argn < args.length; argn++ ) {
            if ( args[ argn ].equals( "-enum" ) ) {
                enumerate = true;
            } else if ( args[ argn ].equals( "-populate" ) ) {
                populate = true;
            } else if ( args[ argn ].equals( "-retrieve" ) ) {
                retrieve = true;
            } else if ( args[ argn ].equals( "-jdbmName" ) && argn < args.length - 1 ) {
                jdbmName = args[ ++argn ];
            } else if (args[ argn ].equals( "-key" ) && argn < args.length - 1 ) {
                onekey = args[ ++argn ];
            } else if ( args[ argn ].equals( "-name" ) && argn < args.length - 1) {
                name = args[ ++argn ];
            } else {
                System.err.println( "Unrecognized option: " + args[ argn ] );
                usage( System.err );
            }
        }
    }


    /**
     * Display usage information
     */
    protected void usage( PrintStream ps )
    {
        ps.println( "Usage: java " + getClass().getName() + " Options" );
        ps.println();
        ps.println( "Options (with default values):" );
        ps.println( "-help print this" );
    }


    /**
     * Static program entrypoint
     */
    public static void main( String[] args )
    {
        HashtableTest instance = new HashtableTest();
        instance.parseArgs( args );
        try {
            instance.doCommands();
        } catch ( IOException except ) {
            except.printStackTrace();
        }
    }

}

