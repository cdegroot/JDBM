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
 * $Id: TestExtensibleSerializer.java,v 1.2 2006/05/03 19:51:33 thompsonbry Exp $
 */
/*
 * Created on Sep 26, 2005
 */
package jdbm.recman;

import java.io.IOException;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;

import org.CognitiveWeb.extser.IExtensibleSerializer;

import jdbm.RecordManager;
import jdbm.RecordManagerOptions;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.ExtensibleSerializer;
import jdbm.helper.ExtensibleSerializerSingleton;
import jdbm.helper.ISerializationHandler;
import jdbm.helper.Serializer;
import jdbm.helper.StringComparator;
import jdbm.helper.StringSerializer;
import jdbm.strings.StringTable;

import junit.framework.TestCase;

/**
 * Integration test for {@link ExtensibleSerializer}used as the default
 * serialization handler for a store. This tests some persistent features of
 * jdbm but does NOT attempt to duplicate the entire jdbm test suite.
 * 
 * @todo Currently you MUST uncomment the relevant line in {@link Provider}in
 *       order to test jdbm in combination with the extensible serialization
 *       handler (otherwise the default serialization handler will be used).
 * 
 * @author thompsonbry
 */

public class TestExtensibleSerializer
    extends TestCase
{

    RecordManager recman = null;
    
    /**
     * Create record manager with an appropriate default serializer.
     * 
     * @throws IOException
     */
    
    protected void makeRecordManager()
    	throws IOException
    {

        Properties properties = new Properties();

        // Use the "extensible" serialization protocol.
        properties.setProperty
           ( RecordManagerOptions.SERIALIZER,
             RecordManagerOptions.SERIALIZER_EXTENSIBLE
             );
        
        // Open the record manager.
        recman = RecordManagerFactory.createRecordManager
    	    ( TestRecordFile.testFileName,
    	      properties
    	      );

    }

    public void setUp()
    	throws IOException
    {

        TestRecordFile.deleteTestFile();

        makeRecordManager();
        
    }

    public void tearDown()
    	throws IOException
    {

        recman.close();
        
        TestRecordFile.deleteTestFile();

    }

    public void reopenStore()
    	throws IOException
    {

        recman.commit();
        
        recman.close();

        // Open the record manager.
        recman = RecordManagerFactory.createRecordManager
    	    ( TestRecordFile.testFileName,
    	      new Properties()
    	      );
        
    }
    
    /**
     * Verify that the {@link ExtensibleSerializerSingleton}was configured on
     * the record manager, that it was assigned the reference to the record
     * manager, and that we can get the {@link ExtensibleSerializer} singleton
     * from it.
     * <p>
     */

    public void test_001()
    	throws IOException
    {

        ISerializationHandler ser = recman.getSerializationHandler();
        
        assertTrue( ser != null );
        
        assertTrue( ser instanceof ExtensibleSerializerSingleton );
                
        ExtensibleSerializerSingleton ss = (ExtensibleSerializerSingleton) ser;
        
        IExtensibleSerializer cids = ss.getSerializer( recman );
        
        assertTrue( cids != null );
        
        // And now reopen the store and verify that we can still get the
        // ExtensibleSerializerSingleton and that these various invariants are still
        // true.

        reopenStore();
        
        ser = recman.getSerializationHandler();
        
        assertTrue( ser != null );
        
        assertTrue( ser instanceof ExtensibleSerializerSingleton );
                
        ss = (ExtensibleSerializerSingleton) ser;
        
        cids = ss.getSerializer( recman );
        
        assertTrue( cids != null );

    }


    /**
     * Insert an {@link String}into the store, reopen the store, and then
     * deserialize the object.
     * 
     * @throws IOException
     */
    public void test_002()
	throws IOException
    {
	    
        final String expectedString = "Hello World!";
	    
        long recid = recman.insert( expectedString );
	    
        reopenStore();
	    
        assertEquals( expectedString, recman.fetch( recid ) );
	    
    }

    /**
     * Register a serializer for a class which (a) does NOT implement {@link
     * Serializable} and (b) does NOT have a default already registered.  We
     * then verify that we can insert an instance of that class into the store,
     * reopen the store, and get our instance back out.
     */
    
    public void test_003()
    	throws IOException
    {
        
        IExtensibleSerializer serializer = ((ExtensibleSerializerSingleton)recman.getSerializationHandler()).getSerializer( recman );
        
        serializer.registerSerializer( MyClass.class, MyClassSerializer.class );

        final String expectedState = "Hello World!";
        
        MyClass obj1 = new MyClass( expectedState );
        
        long recid = recman.insert( obj1 );
        obj1 = (MyClass) recman.fetch( recid );
        assertEquals( expectedState, obj1.getState() );

        reopenStore();
        
        MyClass obj2 = (MyClass) recman.fetch( recid );
        assertEquals( expectedState, obj2.getState() );
        
    }

    /**
     * Integration test using the {@link ExtensibleSerializer} to insert and
     * fetch objects for which it has special cased serialization and objects
     * that use Java serialization (either the {@link Serializable} or the
     * {@link Externalizable} interface).
     */
    public void test_004()
    	throws IOException
    {

        // Objects corresponding to Java primitives with special case serialization.
        final Boolean expectedBoolean = new Boolean(true);
        final Byte expectedByte = new Byte((byte)-4);
        final Character expectedCharacter = new Character( 'a' );
        final Short expectedShort = new Short((short)-32999);
        final Integer expectedInteger = new Integer(-999999);
        final Long expectedLong = new Long(-12L);
        final Float expectedFloat = new Float(-13.1331f);
        final Double expectedDouble = new Double(-13.1331d);
        final String expectedString = "Hello World!";

        // Serializable
        final List expectedList = Arrays.asList( new Object[]{"A", new Integer(4), new Double(12d)} );

        long recidBoolean = recman.insert( expectedBoolean );
        long recidByte = recman.insert( expectedByte );
        long recidCharacter = recman.insert( expectedCharacter );
        long recidShort = recman.insert( expectedShort );
        long recidInteger = recman.insert( expectedInteger );
        long recidLong = recman.insert( expectedLong );
        long recidFloat = recman.insert( expectedFloat );
        long recidDouble = recman.insert( expectedDouble );
        long recidString = recman.insert( expectedString );

        long recidList = recman.insert( expectedList );
        
        reopenStore();

        assertEquals( expectedBoolean, recman.fetch( recidBoolean ) );
        assertEquals( expectedByte, recman.fetch( recidByte ) );
        assertEquals( expectedCharacter, recman.fetch( recidCharacter ) );
        assertEquals( expectedShort, recman.fetch( recidShort ) );
        assertEquals( expectedInteger, recman.fetch( recidInteger ) );
        assertEquals( expectedLong, recman.fetch( recidLong ) );
        assertEquals( expectedFloat, recman.fetch( recidFloat ) );
        assertEquals( expectedDouble, recman.fetch( recidDouble ) );
        assertEquals( expectedString, recman.fetch( recidString ) );
        
     	List actualList = (List) recman.fetch( recidList );
     	assertEquals( expectedList.size(), actualList.size() );
     	for( int i=0; i<expectedList.size(); i++ ) {
     	    assertEquals( expectedList.get(i), actualList.get(i) );
     	}
     	
    }
    
    /**
     * Integration test using {@link BTree}.
     */

    public void test_005()
    	throws IOException
    {
        
        BTree btree = BTree.createInstance( recman, StringComparator.INSTANCE );
        long btreeId = btree.getRecid();
        btree.insert( "c", "C", false );
        btree.insert( "a", "A", false );
        btree.insert( "b", "B", false );
        
        reopenStore();
        
        btree = BTree.load( recman, btreeId );
        
        assertEquals( "A", btree.find("a") );
        assertEquals( "B", btree.find("b") );
        assertEquals( "C", btree.find("c") );
        
    }
    
    /**
     * Integration test using {@link StringTable}.
     */

    public void test_006()
    	throws IOException
    {
        
        StringTable stbl = StringTable.createInstance( recman );
        long recid = recman.insert( stbl );
        
        int stringId1 = stbl.intern( "a", true );
        int stringId2 = stbl.intern( "b", true );
        int stringId3 = stbl.intern( "c", true );
        
        reopenStore();
        
        stbl = StringTable.load( recman, recid );
        
        assertEquals( "a", stbl.disintern( stringId1 ) );
        assertEquals( "b", stbl.disintern( stringId2 ) );
        assertEquals( "c", stbl.disintern( stringId3 ) );
        
    }
    
    /**
     * Integration test registers a custom serializer with the {@link 
     * ExtensibleSerializer} and demonstrates that instances of an object
     * which does NOT implement {@link Serializable} may be correctly
     * inserted and fetched from the store.
     */

    public void test_007()
    	throws IOException
    {

        // Get the ExtensibleSerializer singleton.
        IExtensibleSerializer cids
            = ((ExtensibleSerializerSingleton)recman.getSerializationHandler()).getSerializer
              ( recman
                );
        
        // Register a serializer for MyClass.
        cids.registerSerializer( MyClass.class, MyClassSerializer.class );

        final String expectedState = "Hello World!";

        MyClass obj1 = new MyClass( expectedState );

        assertEquals( expectedState, obj1.getState() );
        
        long recid = recman.insert( obj1 ); // insert using the default serializer.
        
        reopenStore();
        
        MyClass obj2 = (MyClass) recman.fetch( recid );
        
        assertEquals( expectedState, obj2.getState() );
        
    }
    
    /**
     * Note: Does NOT implement {@link Serializable}.
     * 
     * @see MyClassSerializer
     */
    public static class MyClass
    {
        String m_state;
        public MyClass(){}
        public MyClass( String s ) {m_state = s;}
        public void setState( String s ) {m_state = s;}
        public String getState() {return m_state;}
    }
    
    /**
     * Knows how to (de-)serialize {@link MyClass}.
     */
    public static class MyClassSerializer
    	implements Serializer
    {
        public byte[] serialize(Object obj) throws IOException {
            return new StringSerializer().serialize( ((MyClass)obj).getState() );
        }
        public Object deserialize(byte[] serialized) throws IOException {
            MyClass obj = new MyClass();
            obj.setState( (String) new StringSerializer().deserialize( serialized ) );
            return obj;
        }
        
    }
  
}
