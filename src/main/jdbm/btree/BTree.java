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
 */

package jdbm.btree;

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.CognitiveWeb.extser.DataInput;
import org.CognitiveWeb.extser.DataOutput;
import org.CognitiveWeb.extser.IStreamSerializer;
import org.CognitiveWeb.extser.Stateless;

import jdbm.RecordManager;
import jdbm.helper.ExtensibleSerializerSingleton;
import jdbm.helper.Serializer;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.helper.compression.CompressionProvider;


/**
 * B+Tree persistent indexing data structure.  B+Trees are optimized for
 * block-based, random I/O storage because they store multiple keys on
 * one tree node (called <code>BPage</code>).  In addition, the leaf nodes
 * directly contain (inline) the values associated with the keys, allowing a
 * single (or sequential) disk read of all the values on the page.
 * <p>
 * B+Trees are n-airy, yeilding log(N) search cost.  They are self-balancing,
 * preventing search performance degradation when the size of the tree grows.
 * <p>
 * Keys and associated values must be <code>Serializable</code> objects. The
 * user is responsible to supply a serializable <code>Comparator</code> object
 * to be used for the ordering of entries, which are also called <code>Tuple</code>.
 * The B+Tree allows traversing the keys in forward and reverse order using a
 * TupleBrowser obtained from the browse() methods.
 * <p>
 * This implementation does not directly support duplicate keys, but it is
 * possible to handle duplicates by inlining or referencing an object collection
 * as a value.
 * <p>
 * There is no limit on key size or value size, but it is recommended to keep
 * both as small as possible to reduce disk I/O.   This is especially true for
 * the key size, which impacts all non-leaf <code>BPage</code> objects.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: BTree.java,v 1.18 2008/02/14 17:43:20 trumpetinc Exp $
 */
public class BTree
    implements Externalizable
{

    private static final boolean DEBUG = false;

    /**
     * Version id for serialization.
     */
    final static long serialVersionUID = 1L;


    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 16;


    /**
     * Page manager used to persist changes in BPages
     */
    private transient RecordManager _recman;


    /**
     * This BTree's record ID in the PageManager.
     */
    private transient long _recid;


    /**
     * Comparator used to index entries.
     */
    protected Comparator _comparator;


    /**
     * Serializer used to serialize index keys (optional)
     */
    protected Serializer _keySerializer;


    /**
     * Serializer used to serialize index values (optional)
     */
    protected Serializer _valueSerializer;


    /**
     * Height of the B+Tree.  This is the number of BPages you have to traverse
     * to get to a leaf BPage, starting from the root.
     */
    private int _height;


    /**
     * Recid of the root BPage
     */
    private transient long _root;


    /**
     * Number of entries in each BPage.
     */
    protected int _pageSize;


    /**
     * Total number of entries in the BTree
     */
    protected long _entries;

    
    /**
    * Provides compressor and decompressor for the keys stored in the BTree
    */
    protected CompressionProvider _keyCompressionProvider;

    /**
     * Serializer used for the root BPage of this tree.
     */
    private transient BPage _bpageSerializer;

    
    /**
     * No-argument constructor used by serialization.
     */
    public BTree()
    {
        // empty
    }


    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     */
    public static BTree createInstance( RecordManager recman,
                                        Comparator comparator )
        throws IOException
    {
        return createInstance( recman, comparator, null, null, DEFAULT_SIZE );
    }


    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param keySerializer Serializer used to serialize index keys (optional)
     * @param valueSerializer Serializer used to serialize index values (optional)
     * @param comparator Comparator used to order index entries
     */
    public static BTree createInstance( RecordManager recman,
                                        Comparator comparator,
                                        Serializer keySerializer,
                                        Serializer valueSerializer )
        throws IOException
    {
        return createInstance( recman, comparator, keySerializer, 
                               valueSerializer, DEFAULT_SIZE );
    }


    /**
     * Create a new persistent BTree with the given number of entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     * @param keySerializer Serializer used to serialize index keys (optional)
     * @param valueSerializer Serializer used to serialize index values (optional)
     * @param pageSize Number of entries per page (must be even).
     */
    public static BTree createInstance( RecordManager recman,
                                        Comparator comparator,
                                        Serializer keySerializer,
                                        Serializer valueSerializer,
                                        int pageSize )
        throws IOException
    {
        BTree btree;

        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }

        if ( comparator == null ) {
            throw new IllegalArgumentException( "Argument 'comparator' is null" );
        }

        if ( ! ( comparator instanceof Serializable ) ) {
            throw new IllegalArgumentException( "Argument 'comparator' must be serializable" );
        }

        if ( keySerializer != null && ! ( keySerializer instanceof Serializable ) ) {
            throw new IllegalArgumentException( "Argument 'keySerializer' must be serializable" );
        }

        if ( valueSerializer != null && ! ( valueSerializer instanceof Serializable ) ) {
            throw new IllegalArgumentException( "Argument 'valueSerializer' must be serializable" );
        }

        // make sure there's an even number of entries per BPage
        if ( ( pageSize & 1 ) != 0 ) {
            throw new IllegalArgumentException( "Argument 'pageSize' must be even" );
        }

        btree = new BTree();
        btree._recman = recman;
        btree._comparator = comparator;
        btree._keySerializer = keySerializer;
        btree._valueSerializer = valueSerializer;
        btree._pageSize = pageSize;
        btree._bpageSerializer = new BPage();
        btree._bpageSerializer._btree = btree;
        btree._recid = recman.insert( btree ); // insert into store.
        btree._bpageSerializer._btreeId = btree.getRecid();
        btree._keyCompressionProvider = null;
        return btree;
    }


    /**
     * Load a persistent BTree.
     *
     * @param recman RecordManager used to store the persistent btree
     * @param recid Record id of the BTree
     */
    public static BTree load( RecordManager recman, long recid )
        throws IOException
    {
        BTree btree = (BTree) recman.fetch( recid );
        btree._recid = recid;
        btree._recman = recman;
        btree._bpageSerializer = new BPage();
        btree._bpageSerializer._btree = btree;
        btree._bpageSerializer._btreeId = recid;
        return btree;
    }

    public void setKeyCompressionProvider(CompressionProvider provider){
    	if (_entries != 0) 
    		throw new IllegalArgumentException( "You can't change the key compression provider once the BTree is populated" );
    	
    	_keyCompressionProvider = provider;
    }

    public CompressionProvider getKeyCompressionProvider()
    {
        return _keyCompressionProvider;
    }
    
    /**
     * Insert an entry in the BTree.
     * <p>
     * The BTree cannot store duplicate entries.  An existing entry can be
     * replaced using the <code>replace</code> flag.   If an entry with the
     * same key already exists in the BTree, its value is returned.
     *
     * @param key Insert key
     * @param value Insert value
     * @param replace Set to true to replace an existing key-value pair.
     * @return Existing value, if any.
     */
    public synchronized Object insert( Object key, Object value,
                                       boolean replace )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }

        BPage rootPage = getRoot();

        if ( rootPage == null ) {
            // BTree is currently empty, create a new root BPage
            if (DEBUG) {
                System.out.println( "BTree.insert() new root BPage" );
            }
            rootPage = new BPage( this, key, value );
            _root = rootPage._recid;
            _height = 1;
            _entries = 1;
            _recman.update( _recid, this );
            return null;
        } else {
            BPage.InsertResult insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "BTree.insert() replace root BPage due to overflow" );
                }
                rootPage = new BPage( this, rootPage, insert._overflow );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _entries++;
                dirty = true;
            }
            if ( dirty ) {
                _recman.update( _recid, this );
            }
            // insert might have returned an existing value
            return insert._existing;
        }
    }


    /**
     * Remove an entry with the given key from the BTree.
     *
     * @param key Removal key
     * @return Value associated with the key, or null if no entry with given
     *         key existed in the BTree.
     */
    public synchronized Object remove( Object key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }
        boolean dirty = false;
        BPage.RemoveResult remove = rootPage.remove( _height, key );
        if ( remove._underflow && rootPage.isEmpty() ) {
            _height -= 1;
            dirty = true;

            _recman.delete(_root);
            if ( _height == 0 ) {
                _root = 0;
            } else {
                _root = rootPage.childBPage( _pageSize-1 )._recid;
            }
        }
        if ( remove._value != null ) {
            _entries--;
            dirty = true;
        }
        if ( dirty ) {
            _recman.update( _recid, this );
        }
        return remove._value;
    }


    /**
     * Find the value associated with the given key.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or null if not found.
     */
    public synchronized Object find( Object key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }

        Tuple tuple = new Tuple( null, null );
        TupleBrowser browser = rootPage.find( _height, key );

        if ( browser.getNext( tuple ) ) {
            // find returns the matching key or the next ordered key, so we must
            // check if we have an exact match
            if ( _comparator.compare( key, tuple.getKey() ) != 0 ) {
                return null;
            } else {
                return tuple.getValue();
            }
        } else {
            return null;
        }
    }


    /**
     * Find the value associated with the given key, or the entry immediately
     * following this key in the ordered BTree.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or a greater entry, or null if no
     *         greater entry was found.
     */
    public synchronized Tuple findGreaterOrEqual( Object key )
        throws IOException
    {
        Tuple         tuple;
        TupleBrowser  browser;

        if ( key == null ) {
            // there can't be a key greater than or equal to "null"
            // because null is considered an infinite key.
            return null;
        }

        tuple = new Tuple( null, null );
        browser = browse( key );
        if ( browser.getNext( tuple ) ) {
            return tuple;
        } else {
            return null;
        }
    }


    /**
     * Get a browser initially positioned at the beginning of the BTree.
     * <p><b>
     * WARNING: If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @return Browser positionned at the beginning of the BTree.
     */
    public synchronized TupleBrowser browse()
        throws IOException
    {
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.INSTANCE;
        }
        TupleBrowser browser = rootPage.findFirst();
        return browser;
    }


    /**
     * Get a browser initially positioned just before the given key.
     * <p><b>
     * WARNING: If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @param key Key used to position the browser.  If null, the browser
     *            will be positionned after the last entry of the BTree.
     *            (Null is considered to be an "infinite" key)
     * @return Browser positionned just before the given key.
     */
    public synchronized TupleBrowser browse( Object key )
        throws IOException
    {
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.INSTANCE;
        }
        TupleBrowser browser = rootPage.find( _height, key );
        return browser;
    }

    /** 
     * Deletes all BPages in this BTree, then deletes the tree from the record manager
     */
    public synchronized void delete()
        throws IOException
    {
        BPage rootPage = getRoot();
        if (rootPage != null)
            rootPage.delete();
        _recman.delete(_recid);
    }

    /**
     * Return the number of entries (size) of the BTree.
     * 
     * @deprecated Use {@link #entryCount()} which correctly
     * reports the size when it exceeds a Java <cod>int</code>.
     */
    public synchronized int size()
    {
 
        if( _entries > Integer.MAX_VALUE ) {
            
            throw new RuntimeException
            	( "Size exceeds Integer."
            	  );
            
        }
        
        return (int) _entries;
        
    }

    /**
     * Return the number of entries (size) of the BTree.
     */
    
    public synchronized long entryCount()
    {
        
        return _entries;
        
    }

    /**
     * Return the persistent record identifier of the BTree.
     */
    public long getRecid()
    {
        return _recid;
    }

    /**
     * Return the size of a node in the btree.
     */
    public int getPageSize()
    {
        return _pageSize;
    }
    
    /**
     * Height of the B+Tree.  This is the number of BPages you have to traverse
     * to get to a leaf BPage, starting from the root.
     */
    public int getHeight()
    {
        return _height;
    }
    
    /**
     * Return the {@link Comparator} in use by the {@link BTree}.
     */
    public Comparator getComparator()
    {
        
        return _comparator;
        
    }

    /**
     * Return the {@link Serializer} in use by the {@link BTree}
     * to (de-)serialize its keys.
     */
    public Serializer getKeySerializer()
    {
        
        return _keySerializer;
        
    }
    
    /**
     * Return the {@link Serializer} in use by the {@link BTree}
     * to (de-)serialize its values.
     */
    public Serializer getValueSerializer()
    {
    
        return _valueSerializer;
        
    }
    
    /**
     * Return the root BPage, or null if it doesn't exist.
     */
    protected BPage getRoot()
        throws IOException
    {
        if ( _root == 0 ) {
            return null;
        }
//        BPage root = (BPage) _recman.fetch( _root, _bpageSerializer );
        BPage root = _fetch( _root, _bpageSerializer );
        root._recid = _root;
        root._btree = this;
        root._btreeId = getRecid();
        return root;
    }

    /**
     * Implement Externalizable interface.
     */
    public void readExternal( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        _comparator = (Comparator) in.readObject();
        _keySerializer = (Serializer) in.readObject();
        _valueSerializer = (Serializer) in.readObject();
        _height = in.readInt();
        _root = in.readLong();
        _pageSize = in.readInt();
        int tmp = in.readInt();
        if( tmp == -1 ) {
            _entries = in.readLong();
        } else {
            _entries = tmp;
        }
        try{
        	_keyCompressionProvider = (CompressionProvider) in.readObject();
        } catch (EOFException e){
        	// this BTree was stored before we added key compression support
        	// I'd prefer to do this with in.available(), but that doesn't work reliably (available() is not the number of bytes left in the input- it is the number of bytes that can be read without blocking...)
        	_keyCompressionProvider = null;
        }

    }


    /**
     * Implement Externalizable interface.
     */
    public void writeExternal( ObjectOutput out )
        throws IOException
    {
        out.writeObject( _comparator );
        out.writeObject( _keySerializer );
        out.writeObject( _valueSerializer );
        out.writeInt( _height );
        out.writeLong( _root );
        out.writeInt( _pageSize );
        
        if( _entries < Integer.MAX_VALUE ) {

            out.writeInt( (int)_entries );

        } else {

            out.writeInt( -1 );
            out.writeLong( _entries );
            
        }
        
        out.writeObject( _keyCompressionProvider );
    }

    /**
     * Stream-based serialization supporting transparent versioning.
     * 
     * @author thompsonbry
     */
    
    public static class Serializer0 implements IStreamSerializer, Stateless
    {

        public void serialize(DataOutput out, Object obj) throws IOException {
            
            BTree tmp = (BTree) obj;

            out.writePackedInt ( tmp._height );
            out.writePackedInt ( tmp._pageSize );
            
            out.writePackedLong( tmp._root );
            out.writePackedLong( tmp._entries );
            
            out.serialize( tmp._comparator );
            out.serialize( tmp._keySerializer );
            out.serialize( tmp._valueSerializer );
            out.serialize( tmp._keyCompressionProvider );
            
        }

        public Object deserialize(DataInput in, Object obj) throws IOException {
            
            BTree tmp = (BTree) obj;
            
            tmp._height   = in.readPackedInt();
            tmp._pageSize = in.readPackedInt();
            
            tmp._root     = in.readPackedLong();
            tmp._entries  = in.readPackedLong();
            
            tmp._comparator = (Comparator) in.deserialize();
            tmp._keySerializer = (Serializer) in.deserialize();
            tmp._valueSerializer = (Serializer) in.deserialize();
            tmp._keyCompressionProvider = (CompressionProvider) in.deserialize();
            
            return tmp;
            
        }
        
    }

    /*
    public void assert() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.assertRecursive( _height );
        }
    }
    */


    public void dump( PrintStream out ) throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
        	root.dump( out, 0 );
            root.dumpRecursive( out, _height, 0 );
        }
    }

    /**
     * Used for debugging and testing only.  Populates the 'out' list with
     * the recids of all child pages in the BTree.
     * @param out
     * @throws IOException
     */
    void dumpChildPageRecIDs(List out) throws IOException{
        BPage root = getRoot();
        if ( root != null ) {
            out.add(new Long(root._recid));
            root.dumpChildPageRecIDs( out, _height);
        }
    }

    /** PRIVATE INNER CLASS
     *  Browser returning no element.
     */
    static class EmptyBrowser
        extends TupleBrowser
    {

        static TupleBrowser INSTANCE = new EmptyBrowser();

        public boolean getNext( Tuple tuple )
        {
            return false;
        }

        public boolean getPrevious( Tuple tuple )
        {
            return false;
        }
    }

    //
    // CRUD interface used to bridge the historical Serializer and
    // the new IStreamSerializer for BPage.
    //

    /**
     * True iff the extensible serialization handler is being used for
     * the store.
     */
    protected boolean isExtensibleSerializer()
    {
        
        return _recman.getSerializationHandler() instanceof ExtensibleSerializerSingleton;
        
    }

    /**
     * Insert using either a custom serializer or a serializer registered with
     * the extensible serialization handler.
     * 
     * @param bpage
     *            The page to be inserted into the store.
     * 
     * @param ser
     *            The custom serializer, which is ignored if the extensible
     *            serialization handler was configured for the store.
     * 
     * @return The logical row id for the bpage.
     * 
     * @throws IOException
     */

    long _insert( BPage bpage, Serializer ser )
    	throws IOException
    {

        return ( isExtensibleSerializer()
                ? _recman.insert( bpage )
                : _recman.insert( bpage, ser )
                );
        
    }

    /**
     * Fetch a {@link BPage}from the store using either a custom serializer or
     * a serializer registered with the extensible serialization handler.
     * 
     * @param recid
     *            The logical row id for the {@link BPage}.
     * 
     * @param ser
     *            The custom serializer, which is ignored if the extensible
     *            serialization handler was configured for the store.
     * 
     * @return The {@link BPage}.
     * 
     * @throws IOException
     */

    BPage _fetch( long recid, Serializer ser )
    	throws IOException
    {
    
        return (BPage) ( isExtensibleSerializer()
                	 ? _recman.fetch( recid )
                	 : _recman.fetch( recid, ser )
                	 );
        
    }
    
    /**
     * Update a {@link BPage} in the store using either a custom serializer or a
     * serializer registered with the extensible serialization handler.
     * 
     * @param recid
     *            The logical row id for the {@link BPage}.
     * 
     * @param bpage
     *            The {@link BPage} that is being updated.
     * 
     * @param ser
     *            The custom serializer, which is ignored if the extensible
     *            serialization handler was configured for the store.
     * 
     * @return The {@link BPage}.
     * 
     * @throws IOException
     */
    
    void _update( long recid, BPage bpage, Serializer ser )
    	throws IOException
    {

        if( isExtensibleSerializer() ) {

            _recman.update( recid, bpage );
            
        } else {
            
            _recman.update( recid, bpage, ser );
            
        }
        
    }

    /**
     * Delete a {@link BPage} from the store.
     * 
     * @param recid The recid of the {@link BPage}.
     * 
     * @throws IOException
     */
    
    void _delete( long recid )
    	throws IOException
    {

        _recman.delete( recid );
        
    }
    
}
