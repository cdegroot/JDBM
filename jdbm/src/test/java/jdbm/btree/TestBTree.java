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
import jdbm.RecordManagerOptions;

import jdbm.RecordManagerFactory;

import jdbm.recman.TestRecordFile;

import jdbm.helper.ByteArrayComparator;
import jdbm.helper.ByteArraySerializer;

import jdbm.helper.StringComparator;

import jdbm.helper.TupleBrowser;

import jdbm.helper.Tuple;



import junit.framework.*;



import java.io.IOException;

import java.io.Serializable;



import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.TreeMap;

import java.util.Iterator;



/**

 *  This class contains all Unit tests for {@link BTree}.

 *

 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>

 *  @version $Id: TestBTree.java,v 1.12 2006/01/17 01:18:38 trumpetinc Exp $

 */

public class TestBTree

    extends TestCase

{



      static final boolean DEBUG = false;



      // the number of threads to be started in the synchronization test

      static final int THREAD_NUMBER = 5;



      // the size of the content of the maps for the synchronization

      // test. Beware that THREAD_NUMBER * THREAD_CONTENT_COUNT < Integer.MAX_VALUE.

      static final int THREAD_CONTENT_SIZE = 150;



      // for how long should the threads run.

      static final int THREAD_RUNTIME = 10 * 1000;



    protected TestResult result_;



    public TestBTree( String name )

    {

        super( name );

    }



    public void setUp()

    {

        TestRecordFile.deleteTestFile();

    }



    public void tearDown()

    {

        TestRecordFile.deleteTestFile();

    }



    /**

     * Overrides TestCase.run(TestResult), so the errors from threads

     * started from this thread can be added to the testresult. This is

     * shown in

     * http://www.javaworld.com/javaworld/jw-12-2000/jw-1221-junit.html

     *

     * @param result the testresult

     */

    public void run( TestResult result )

    {

        result_ = result;

        super.run( result );

        result_ = null;

    }



//----------------------------------------------------------------------

/**

 * Handles the exceptions from other threads, so they are not ignored

 * in the junit test result. This method must be called from every

 * thread's run() method, if any throwables were throws.

 *

 * @param t the throwable (either from an assertEquals, assertTrue,

 * fail, ... method, or an uncaught exception to be added to the test

 * result of the junit test.

 */



  protected void handleThreadException(final Throwable t)

  {

    synchronized(result_)

    {

      if(t instanceof AssertionFailedError)

        result_.addFailure(this,(AssertionFailedError)t);

      else

        result_.addError(this,t);

    }

  }





    /**

     *  Basic tests

     */

    public void testBasics() throws IOException {

        RecordManager  recman;

        BTree          tree;

        byte[]         test0, test1, test2, test3;

        byte[]         value1, value2;



        test0 = "test0".getBytes();

        test1 = "test1".getBytes();

        test2 = "test2".getBytes();

        test3 = "test3".getBytes();



        value1 = "value1".getBytes();

        value2 = "value2".getBytes();



        if ( DEBUG ) {

            System.out.println("TestBTree.testBasics");

        }



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new ByteArrayComparator() );



        tree.insert( test1, value1, false );

        tree.insert( test2, value2, false );



        byte[] result;

        result = (byte[]) tree.find( test0 );

        if ( result != null ) {

            throw new Error( "Test0 shouldn't be found" );

        }



        result = (byte[]) tree.find( test1 );

        if ( result == null || ByteArrayComparator.compareByteArray( result, value1 ) != 0 ) {

            throw new Error( "Invalid value for test1: " + result );

        }



        result = (byte[]) tree.find( test2 );

        if ( result == null || ByteArrayComparator.compareByteArray( result, value2 ) != 0 ) {

            throw new Error( "Invalid value for test2: " + result );

        }



        result = (byte[]) tree.find( test3 );

        if ( result != null ) {

            throw new Error( "Test3 shouldn't be found" );

        }



        recman.close();

    }



    /**

     *  Basic tests, just use the simple test possibilities of junit (cdaller)

     */

    public void testBasics2() throws IOException {

        RecordManager  recman;

        BTree          tree;

        byte[]         test0, test1, test2, test3;

        byte[]         value1, value2;



        test0 = "test0".getBytes();

        test1 = "test1".getBytes();

        test2 = "test2".getBytes();

        test3 = "test3".getBytes();



        value1 = "value1".getBytes();

        value2 = "value2".getBytes();



        if ( DEBUG )

            System.out.println("TestBTree.testBasics2");



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new ByteArrayComparator() );



        tree.insert( test1, value1, false);

        tree.insert( test2, value2, false);



        assertEquals( null, tree.find( test0 ) );

        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.find( test1 ) ) );

        assertEquals( 0, ByteArrayComparator.compareByteArray( value2, (byte[]) tree.find( test2 ) ) );

        assertEquals( null, (byte[]) tree.find( test3 ) );



        recman.close();

     }





    /**

     *  Test what happens after the recmanager has been closed but the

     *  btree is accessed. WHAT SHOULD HAPPEN???????????

     * (cdaller)

     */

    public void testClose()

        throws IOException

    {

        RecordManager  recman;

        BTree          tree;

        byte[]         test0, test1, test2, test3;

        byte[]         value1, value2;



        test0 = "test0".getBytes();

        test1 = "test1".getBytes();

        test2 = "test2".getBytes();

        test3 = "test3".getBytes();



        value1 = "value1".getBytes();

        value2 = "value2".getBytes();



        if ( DEBUG )

            System.out.println("TestBTree.testClose");



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new ByteArrayComparator() );



        tree.insert( test1, value1, false );

        tree.insert( test2, value2, false );



        assertEquals( null, tree.find( test0 ) );

        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.find( test1 ) ) );

        assertEquals( 0, ByteArrayComparator.compareByteArray( value2, (byte[]) tree.find( test2 ) ) );

        assertEquals( null, (byte[]) tree.find( test3 ) );



        recman.close();



        try {

            tree.browse();

            fail("Should throw an IllegalStateException on access on not opened btree");

        } catch( IllegalStateException except ) {

            // ignore

        }



        try {

            tree.find( test0 );

            fail( "Should throw an IllegalStateException on access on not opened btree" );

        } catch( IllegalStateException except ) {

            // ignore

        }



        try {

            tree.findGreaterOrEqual( test0 );

            fail( "Should throw an IllegalStateException on access on not opened btree" );

        } catch( IllegalStateException except ) {

            // ignore

        }



        try {

            tree.insert( test2, value2, false );

            fail( "Should throw an IllegalStateException on access on not opened btree" );

        } catch( IllegalStateException except ) {

            // ignore

        }



        try {

            tree.remove( test0 );

            fail( "Should throw an IllegalStateException on access on not opened btree" );

        } catch( IllegalStateException except ) {

            // ignore

        }



        /*

        try {

            tree.size();

            fail( "Should throw an IllegalStateException on access on not opened btree" );

        } catch( IllegalStateException except ) {

            // ignore

        }

        */

     }





    /**

     *  Test to insert different objects into one btree. (cdaller)

     */

    public void testInsert()

        throws IOException

    {

        RecordManager  recman;

        BTree          tree;



        if ( DEBUG )

            System.out.println("TestBTree.testInsert");



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new StringComparator() );



        // insert differnt objects and retrieve them

        tree.insert("test1", "value1",false);

        tree.insert("test2","value2",false);

        tree.insert("one", new Integer(1),false);

        tree.insert("two",new Long(2),false);

        tree.insert("myownobject",new TestObject(new Integer(234)),false);



        assertEquals("value2",(String)tree.find("test2"));

        assertEquals("value1",(String)tree.find("test1"));

        assertEquals(new Integer(1),(Integer)tree.find("one"));

        assertEquals(new Long(2),(Long)tree.find("two"));



        // what happens here? must not be replaced, does it return anything?

        // probably yes!

        assertEquals("value1",tree.insert("test1","value11",false));

        assertEquals("value1",tree.find("test1")); // still the old value?

        assertEquals("value1",tree.insert("test1","value11",true));

        assertEquals("value11",tree.find("test1")); // now the new value!



        TestObject expected_obj = new TestObject(new Integer(234));

        TestObject btree_obj = (TestObject)tree.find("myownobject");

        assertEquals(expected_obj, btree_obj);



        recman.close();

    }





    /**

     *  Test to remove  objects from the btree. (cdaller)

     */

    public void testRemove()

        throws IOException

    {

        RecordManager  recman;

        BTree          tree;



        if ( DEBUG ) {

            System.out.println( "TestBTree.testRemove" );

        }



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new StringComparator() );



        tree.insert("test1", "value1",false);

        tree.insert("test2","value2",false);

        assertEquals("value1",(String)tree.find("test1"));

        assertEquals("value2",(String)tree.find("test2"));

        tree.remove("test1");

        assertEquals(null,(String)tree.find("test1"));

        assertEquals("value2",(String)tree.find("test2"));

        tree.remove("test2");

        assertEquals(null,(String)tree.find("test2"));



        int iterations = 1000;



        for ( int count = 0; count < iterations; count++ ) {

            tree.insert( "num"+count, new Integer( count ), false );

        }



        assertEquals( iterations, tree.entryCount() );



        for ( int count = 0; count < iterations; count++ ) {

            assertEquals( new Integer( count ), tree.find( "num" + count ) );

        }



        for ( int count = 0; count < iterations; count++ ) {

           tree.remove( "num" + count );

        }



        assertEquals( 0, tree.entryCount() );



        recman.close();

    }



    /**

     *  Test to find differents objects in the btree. (cdaller)

     */

    public void testFind()

        throws IOException

    {

        RecordManager  recman;

        BTree          tree;



        if ( DEBUG )

            System.out.println("TestBTree.testFind");



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new StringComparator() );



        tree.insert("test1", "value1",false);

        tree.insert("test2","value2",false);



        Object value = tree.find("test1");

        assertTrue(value instanceof String);

        assertEquals("value1",value);



        tree.insert("","Empty String as key",false);

        assertEquals("Empty String as key",(String)tree.find(""));



        assertEquals(null,(String)tree.find("someoneelse"));



        recman.close();

    }


    /**
     *  Test deletion of btree from record manager. (kday)
     *  
     *  After deletion, the BTree and all of it's BPage children (and their children)
     *  should be removed from the recordmanager.
     */
    public void testDelete()
        throws IOException
    {
        RecordManager  recman;
        BTree          tree;

        if ( DEBUG )
            System.out.println("TestBTree.testFind");

        // we are going to run this test without object cache enabled.  If it is turned on,
        // we will have problems with using a different deserializer for BPages than the standard
        // serializer.
        Properties props = new Properties();
        props.setProperty(RecordManagerOptions.CACHE_TYPE, RecordManagerOptions.NO_CACHE);
        recman = RecordManagerFactory.createRecordManager( "test", props );

        tree = BTree.createInstance( recman, new StringComparator() );


        // put enough data into the tree so we definitely have multiple pages
        for (int count = 1; count <= 1000; count++){
            tree.insert("num" + count,new Integer(count),false);
            if (count % 100 == 0)
                recman.commit();
        }

        List out = new ArrayList();
        tree.dumpChildPageRecIDs(out);

        assertTrue(out.size() > 0);


        
        // first, confirm that each of the pages actually exists
        for (Iterator iter = out.iterator(); iter.hasNext();) {
            Long recidO = (Long) iter.next();
            Object fetchedRecord = recman.fetch(recidO.longValue(), ByteArraySerializer.INSTANCE);
            // in the current jdbm implementation, a fetch against an invalid
            // recid will return an empty byte array.  We will check for both a null
            // return and an empty byte array, just in case this behavior is every fixed.
            assertFalse(fetchedRecord == null || ((byte[])fetchedRecord).length == 0);
        }
        
        
        tree.delete();

        for (Iterator iter = out.iterator(); iter.hasNext();) {
            Long recidO = (Long) iter.next();
            Object fetchedRecord = recman.fetch(recidO.longValue(), ByteArraySerializer.INSTANCE);
            // in the current jdbm implementation, a fetch against an invalid
            // recid will return an empty byte array.  We will check for both a null
            // return and an empty byte array, just in case this behavior is every fixed.
            assertTrue(fetchedRecord == null || ((byte[])fetchedRecord).length == 0);
        }

        recman.close();

    }


    /**

     *  Test to insert, retrieve and remove a large amount of data. (cdaller)

     */

    public void testLargeDataAmount()

        throws IOException

    {



        RecordManager  recman;

        BTree          tree;



        if ( DEBUG )

            System.out.println("TestBTree.testLargeDataAmount");



        recman = RecordManagerFactory.createRecordManager( "test" );

        // recman = new jdbm.recman.BaseRecordManager( "test" );

        

        tree = BTree.createInstance( recman, new StringComparator() );

        // tree.setSplitPoint( 4 );



        int iterations = 10000;



        // insert data

        for ( int count = 0; count < iterations; count++ ) {

           try {

            assertEquals(null,tree.insert("num"+count,new Integer(count),false));

           } catch ( IOException except ) {

               except.printStackTrace();

               throw except;

           }

        }



           // find data

         for(int count = 0; count < iterations; count++)

         {

           assertEquals(new Integer(count), tree.find("num"+count));

         }



             // delete data

         for(int count = 0; count < iterations; count++)

         {

           assertEquals(new Integer(count),tree.remove("num"+count));

         }



         assertEquals(0,tree.entryCount());



         recman.close();

   }



/**

 * Test access from multiple threads. Assertions only work, when the

 * run() method is overridden and the exceptions of the threads are

 * added to the resultset of the TestCase. see run() and

 * handleException().

 */

  public void testMultithreadAccess()

    throws IOException

  {

        RecordManager  recman;

        BTree          tree;



        if ( DEBUG )

            System.out.println("TestBTree.testMultithreadAccess");



        recman = RecordManagerFactory.createRecordManager( "test" );

        tree = BTree.createInstance( recman, new StringComparator() );



        TestThread[] thread_pool = new TestThread[THREAD_NUMBER];

        String name;

        Map content;



        // create content for the tree, different content for different threads!

        for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {

            name = "thread"+thread_count;

            content = new TreeMap();

            for(int content_count = 0; content_count < THREAD_CONTENT_SIZE; content_count++) {

                // guarantee, that keys and values do not overleap,

                // otherwise one thread removes some keys/values of

                // other threads!

                content.put( name+"_"+content_count,

                             new Integer(thread_count*THREAD_CONTENT_SIZE+content_count) );

            }

            thread_pool[thread_count] = new TestThread(name,tree,content);

            thread_pool[thread_count].start();

        }



        try {

            Thread.sleep(THREAD_RUNTIME);

        } catch( InterruptedException ignore ) {

            ignore.printStackTrace();

        }



        // stop threads:

        for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {

            if ( DEBUG ) System.out.println("Stop threads");

            thread_pool[thread_count].setStop();

        }

        // wait until the threads really stop:

        try {

            for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {

                if ( DEBUG ) System.out.println("Join thread " + thread_count );

                thread_pool[thread_count].join();

                if ( DEBUG ) System.out.println("Joined thread " + thread_count );

            }

        } catch( InterruptedException ignore ) {

            ignore.printStackTrace();

        }

        recman.close();

    }





    /**

     *  Helper method to 'simulate' the methods of an entry set of the btree.

     */

    protected static boolean containsKey( Object key, BTree btree )

        throws IOException

    {

        return ( btree.find( key ) != null );

    }





    /**

     *  Helper method to 'simulate' the methods of an entry set of the btree.

     */

    protected static boolean containsValue( Object value, BTree btree )

        throws IOException

  {

    // we must synchronize on the BTree while browsing

    synchronized ( btree ) {

        TupleBrowser browser = btree.browse();

        Tuple tuple = new Tuple();

        while(browser.getNext(tuple)) {

          if(tuple.getValue().equals(value))

            return(true);

        }

    }

    //    System.out.println("Comparation of '"+value+"' with '"+ tuple.getValue()+"' FAILED");

    return(false);

  }



    /**

     *  Helper method to 'simulate' the methods of an entry set of the btree.

     */

    protected static boolean contains( Map.Entry entry, BTree btree )

        throws IOException

    {

        Object tree_obj = btree.find(entry.getKey());

        if ( tree_obj == null ) {

            // can't distuingish, if value is null or not found!!!!!!

            return ( entry.getValue() == null );

        }

        return ( tree_obj.equals( entry.getValue() ) );

    }



    /**

     *  Runs all tests in this class

     */

    public static void main( String[] args ) {

        junit.textui.TestRunner.run( new TestSuite( TestBTree.class ) );

    }



    /**

     * Inner class for testing puroposes only (multithreaded access)

     */

    class TestThread

        extends Thread

    {

        Map _content;

        BTree _btree;

        volatile boolean _continue = true;

        int THREAD_SLEEP_TIME = 50; // in ms

        String _name;



        TestThread( String name, BTree btree, Map content )

        {

            _content = content;

            _btree = btree;

            _name = name;

        }



        public void setStop()

        {

            _continue = false;

        }



        private void action()

            throws IOException

        {

            Iterator iterator = _content.entrySet().iterator();

            Map.Entry entry;

            if ( DEBUG ) {

                System.out.println("Thread "+_name+": fill btree.");

            }

            while( iterator.hasNext() ) {

                entry = (Map.Entry) iterator.next();

                assertEquals( null, _btree.insert( entry.getKey(), entry.getValue(), false ) );

            }



            // as other threads are filling the btree as well, the size

            // of the btree is unknown (but must be at least the size of

            // the content map)

            assertTrue( _content.size() <= _btree.entryCount() );



            iterator = _content.entrySet().iterator();

            if ( DEBUG ) {

                System.out.println( "Thread " + _name + ": iterates btree." );

            }

            while( iterator.hasNext() ) {

                entry = (Map.Entry) iterator.next();

                assertEquals( entry.getValue(), _btree.find( entry.getKey() ) );

                assertTrue( contains( entry, _btree ) );

                assertTrue( containsKey( entry.getKey(), _btree ) );

                assertTrue( containsValue( entry.getValue(), _btree ) );

            }



            iterator = _content.entrySet().iterator();

            Object key;

            if ( DEBUG ) {

                System.out.println( "Thread " + _name + ": removes his elements from the btree." );

            }

            while( iterator.hasNext() ) {

                key = ( (Map.Entry) iterator.next() ).getKey();

                _btree.remove( key );

                assertTrue( ! containsKey( key,_btree ) );

            }

        }



        public void run()

        {

          if(DEBUG)

            System.out.println("Thread "+_name+": started.");

          try {

            while(_continue) {

              action();

              try {

                Thread.sleep(THREAD_SLEEP_TIME);

              } catch( InterruptedException except ) {

                except.printStackTrace();

              }

            }

          } catch( Throwable t ) {

            if ( DEBUG ) {

              System.err.println("Thread "+_name+" threw an exception:");

              t.printStackTrace();

            }

            handleThreadException(t);

          }

          if(DEBUG)

            System.out.println("Thread "+_name+": stopped.");

        }

      } // end of class TestThread

}





/**

 * class for testing puroposes only (store as value in btree) not

 * implemented as inner class, as this prevents Serialization if

 * outer class is not Serializable.

 */

class TestObject

    implements Serializable

{


	private static final long serialVersionUID = 778155118788320118L;
	
	Object _content;


    private TestObject()

    {

        // empty

    }





    public TestObject( Object content )

    {

        _content = content;

    }





    Object getContent()

    {

        return _content;

    }





    public boolean equals( Object obj )

    {

        if ( ! ( obj instanceof TestObject ) ) {

            return false;

        }

        return _content.equals( ( (TestObject) obj ).getContent() );

    }



    public String toString()

    {

        return( "TestObject {content='" + _content + "'}" );

    }



} // TestObject



