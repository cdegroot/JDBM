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
 * $Id: BaseRecordManager.java,v 1.12 2006/06/01 13:13:15 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import jdbm.RecordManager;
import jdbm.RecordManagerOptions;
import jdbm.helper.DefaultSerializationHandler;
import jdbm.helper.ISerializationHandler;
import jdbm.helper.Serializer;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.compessor.DefaultRecordCompressor;
import jdbm.helper.compessor.IRecordCompressor;

/**
 *  This class manages records, which are uninterpreted blobs of data. The
 *  set of operations is simple and straightforward: you communicate with
 *  the class using long "rowids" and byte[] data blocks. Rowids are returned
 *  on inserts and you can stash them away someplace safe to be able to get
 *  back to them. Data blocks can be as long as you wish, and may have
 *  lengths different from the original when updating.
 *  <p>
 *  Operations are synchronized, so that only one of them will happen
 *  concurrently even if you hammer away from multiple threads. Operations
 *  are made atomic by keeping a transaction log which is recovered after
 *  a crash, so the operations specified by this interface all have ACID
 *  properties.
 *  <p>
 *  You identify a file by just the name. The package attaches <tt>.db</tt>
 *  for the database file, and <tt>.lg</tt> for the transaction log. The
 *  transaction log is synchronized regularly and then restarted, so don't
 *  worry if you see the size going up and down.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: BaseRecordManager.java,v 1.12 2006/06/01 13:13:15 thompsonbry Exp $
 */
public /*final*/ class BaseRecordManager
    implements RecordManager
{

    /**
     * Underlying record file.
     */
    /*private*/ RecordFile _file;


    /**
     * Physical row identifier manager.
     */
    /*private*/ PhysicalRowIdManager _physMgr;


    /**
     * Logigal to Physical row identifier manager.
     */
    /*private*/ LogicalRowIdManager _logMgr;


    /**
     * Page manager.
     */
    /*private*/ PageManager _pageman;


    /**
	 * When non-null, record installs will be buffered and batched.
	 * 
	 * @see RecordManagerOptions#BUFFERED_INSTALLS
	 */
    BufferedRecordInstallManager _bufMgr;
    
    /**
     * Reserved slot for name directory.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;

    /**
     * Reserved slot for sticky options.  This root object identifies
     * a persistent Properties record containing sticky configuration
     * options specified when the store was first created.
     */
    public static final int STICKY_OPTIONS_ROOT = NAME_DIRECTORY_ROOT + 1;

    /**
     * Reserved slot for a persistent (stateful) default serializer
     * instance.  To avoid bootstrapping problems, this object is
     * persisted using {@link DefaultSerializer#INSTANCE}.
     */
    public static final int DEFAULT_SERIALIZER_ROOT = STICKY_OPTIONS_ROOT + 1;

    /**
     * The first slot in the root object table that is available
     * for application objects.  Note that the value of this
     * constant MAY change with subsequent releases of jdbm.
     */
    public static final int FIRST_FREE_ROOT = DEFAULT_SERIALIZER_ROOT + 1;

    /**
     * The configured serialization handler.
     */
    /*private*/ ISerializationHandler _serializer = new DefaultSerializationHandler();
    
    /**
     * The elapsed time during serialization in milliseconds.  This time
     * does NOT include disk access since serialization results in a byte[]
     * that is later transferred to disk.
     */
    transient private long m_serializationElapsed = 0L;

    /**
     * The elapsed time during deserialization in milliseconds. This time does
     * NOT include disk access since deserialization begins once the byte[] of
     * the record is already in hand.
     */
    transient private long m_deserializationElapsed = 0L;

    /**
     * Record compressor (optional).
     * 
     * @see Provider
     * @see RecordManagerOptions#COMPRESSOR
     */
    /*protected*/ IRecordCompressor _compressor = new DefaultRecordCompressor();

    public ISerializationHandler getSerializationHandler()
    {
        
        checkIfClosed();
        
        return _serializer;
        
    }
    
    /**
     * Static debugging flag
     */
    public static final boolean DEBUG = false;

    /**
     * Directory of named records.  This is a persistent directory,
     * stored as a Hashtable.  It can be retrived by using {@link
     * #NAME_DIRECTORY_ROOT}.
     */
    private Map _nameDirectory;

    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public BaseRecordManager( String filename )
        throws IOException
    {
        _file = new RecordFile( filename );
        _pageman = new PageManager( _file );
        _physMgr = new PhysicalRowIdManager( _file, _pageman );
        _logMgr = new LogicalRowIdManager( _file, _pageman );
        _bufMgr = null; // See Provider, RecordManagerOptions#BUFFERED_INSTALLS
    }

    /**
     * Writes some debugging informatiom.
     */
    protected void finalize() throws Throwable
    {
        super.finalize();
        System.err.println( "  serialization time: "+m_serializationElapsed+"ms");
        System.err.println( "deserialization time: "+m_deserializationElapsed+"ms");
    }
    
    /**
     * @return <i>this</i> unless the record manager is closed, in
     * which case it returns <code>null</code>.
     */
    
    public RecordManager getRecordManager()
    {
        
        if( _file == null ) {
            
            return null;
            
        }
        
        return this;
        
    }

    public RecordManager getBaseRecordManager()
    {
        
        return getRecordManager();
        
    }
    
//    public RecordManager getOuterRecordManager()
//    {
//        
//        return _outerRecordManager;
//        
//    }
//
//    public void setOuterRecordManager( RecordManager recman )
//    {
//
//        // The outer record manager may be changed, but only to
//        // a record manager whose base record manager is this
//        // object.  This makes it possible for people to layer
//        // on other recman after {@link Provider}.
//
//        if( _outerRecordManager != null && _outerRecordManager.getBaseRecordManager() != this ) {
//            
//            throw new IllegalArgumentException();
//            
//        }
//        
//        _outerRecordManager = recman;
//        
//    }
    
    /**
     *  Get the underlying Transaction Manager
     */
    public synchronized TransactionManager getTransactionManager()
    {
        checkIfClosed();

        return _file.txnMgr;
    }


    /**
     *  Switches off transactioning for the record manager. This means
     *  that a) a transaction log is not kept, and b) writes aren't
     *  synch'ed after every update. This is useful when batch inserting
     *  into a new database.
     *  <p>
     *  Only call this method directly after opening the file, otherwise
     *  the results will be undefined.
     */
    public synchronized void disableTransactions(){
        checkIfClosed();
        _file.disableTransactions();
    }
    
    /**
     *  Switches off transactioning for the record manager. This means
     *  that a) a transaction log is not kept, and b) writes aren't
     *  synch'ed after every update. This is useful when batch inserting
     *  into a new database.
     *  <p>
     *  Only call this method directly after opening the file, otherwise
     *  the results will be undefined.
     *  
     * @param autoCommitInterval Specifies the maximum size of the dirty page pool before an auto-commit is issued
     * @param syncOnClose Sets whether the underlying record file should sync when it closes if
     * transactions are disabled.
     */
    public synchronized void disableTransactions(int autoCommitInterval, boolean syncOnClose)
    {
        checkIfClosed();
        _file.disableTransactions(autoCommitInterval, syncOnClose);
    }

    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close()
        throws IOException
    {
        checkIfClosed();

        if( _bufMgr != null ) {
        	
        	_bufMgr.commit();
        	_bufMgr = null;
        	
        }
        
        _pageman.close();
        _pageman = null;

        _file.close();
        _file = null;
    }


    /**
     *  Inserts a new record using standard java object serialization.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        return insert( obj, null );
    }

    
    /**
     *  <p>
     *  Inserts a new record using a custom serializer.
     *  </p>
     *  <p>
     *  Note: This method is never invoked if caching is used and the "lazy
     *  insert" feature is specified.
     *  </p>
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     *  @see RecordManagerOptions#LAZY_INSERT
     */
    public synchronized long insert( Object obj, Serializer serializer )
        throws IOException
    {
        byte[]    data;
        long      recid;
        
        checkIfClosed();

        long beginTime = System.currentTimeMillis();
        if( serializer == null ) {
            data = _serializer.serialize( this, 0L, obj );
        } else {
            data = serializer.serialize( obj );
        }
        m_serializationElapsed += System.currentTimeMillis() - beginTime;
        
        data = _compressor.compress( data );

        if( _bufMgr != null ) {

        	/*
			 * Defer assignment of the physical row since the insert operation
			 * will likely be buffered. In this case the generated logical row
			 * identifier is not mapped onto a physical row identifier. That
			 * will happen eventually when the buffered record is flushed onto a
			 * page.
			 */
        	Location physRowId = new Location( 0L, (short) 0 );
            recid = _logMgr.insert( physRowId ).toLong();
            Location logRowId = new Location( recid );
        	_bufMgr.update(logRowId, data);

        } else {
        
        	/*
			 * Allocate a physical row and copy the data into that row. Then
			 * generate a logical row identifier entry that is mapped to that
			 * physical row and return it to the caller.
			 */
        	Location physRowId = _physMgr.insert( data, 0, data.length );
            recid = _logMgr.insert( physRowId ).toLong();

        }
        
        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.insert() recid " + recid + " length " + data.length ) ;
        }
        return recid;
    }

    /**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete( long recid )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }

        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.delete() recid " + recid ) ;
        }

        Location logRowId = new Location( recid );
        Location physRowId = _logMgr.fetch( logRowId );
        if( physRowId.getBlock() != 0L ) {
            // Delete the physical row.  (Not done until the physical row has
            // been allocated).
            _physMgr.delete( physRowId );
        }
        if( _bufMgr != null ) {
        	/*
			 * If we are using buffered updates, then also make sure that
			 * the buffered record is deleted.
			 */
        	_bufMgr.delete( logRowId );
        }
        _logMgr.delete( logRowId );
    }


    /**
     *  Updates a record using standard java object serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        update( recid, obj, null );
    }

    
    /**
     *  Updates a record using a custom serializer.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void update( long recid, Object obj, Serializer serializer )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }

        Location logRecid = new Location( recid );
        Location physRecid = _logMgr.fetch( logRecid );
        
        long beginTime = System.currentTimeMillis();
        byte[] data;
        if( serializer == null ) {
            data = _serializer.serialize( this, recid, obj );
        } else {
            data = serializer.serialize( obj );
        }
        m_serializationElapsed += System.currentTimeMillis() - beginTime;
        
        data = _compressor.compress( data );
        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.update() recid " + recid + " length " + data.length ) ;
        }

        if( _bufMgr != null ) {
        	
        	/*
			 * If we are buffering updates, the we hand the update off and
			 * return immediately. If the record gets buffered then it will not
			 * be installed on a page until later (in response to continuing
			 * cache evictions or a commit operation).
			 */
        	
        	_bufMgr.update(logRecid, data);

        	return;
        	
        }
        
        /*
         * Modified algorithm detects a non-existing physical row from an insert
         * and allocates a physical row. If the physical row exists, then it
         * will be reused if it has sufficient capacity and otherwise
         * reallocated.
         */
        final Location newRecid;
        if( physRecid.getBlock() == 0L ) {
            // physical row does not exist (insert as performed by the cache layer defers
            // allocation of the physical record).
            newRecid = _physMgr.insert( data, 0, data.length );
        } else {
            // physical row exists (record was either inserted by base recman or already
            // updated).
            newRecid = _physMgr.update( physRecid, data, 0, data.length );
        }
        if ( ! newRecid.equals( physRecid ) ) {
            _logMgr.update( logRecid, newRecid );
        }
    }
    
    /**
     *  Fetches a record using standard java object serialization.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetch( long recid )
        throws IOException
    {
        return fetch( recid, null);
    }


    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized Object fetch( long recid, Serializer serializer )
        throws IOException
    {
        byte[] data = null;

        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
        /*
		 * The logical row identifier (identifies a slot in the translation
		 * table).
		 */
        Location logRowId = new Location( recid );
        if( _bufMgr != null ) {
        	/*
        	 * If we are buffering records, then test the buffer first.  This
        	 * test requires the _logical_ record identifier.
        	 */
        	data = _bufMgr.fetch( logRowId );
        }
        if( data == null ) {
        	/*
			 * Either the record is not buffered or we are not buffering
			 * records. Translate the logical record identifier to the physical
			 * record identifier and then fetch the record from the page (and
			 * the page from the store if necessary).
			 */
        	Location physRowId = _logMgr.fetch( logRowId );
        	data = _physMgr.fetch( physRowId );
        }
        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.fetch() recid " + recid + " length " + data.length ) ;
        }
        if( data == null ) { // data.length == 0 ) {
            // If you delete a record and then do a fetch, the physMgr identifies
            // a zero length byte[] based on the record header.  The physMgr has 
            // been modified to return null instead of data[] so that we can detect
            // a deleted record and return null to the application.
            //
            // Note: The way in which deleted records are detected may be changed
            // to permit zero length records, but this test should still be valid.
            return null;
        }
        data = _compressor.decompress( data );
        long beginTime = System.currentTimeMillis();
        Object obj;
        if( serializer == null ) {
            obj = _serializer.deserialize( this, recid, data );
        } else {
            obj = serializer.deserialize( data );
        }
        m_deserializationElapsed += System.currentTimeMillis() - beginTime; 
        return obj;
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount()
    {
        return FileHeader.NROOTS;
    }

    /**
     *  Returns the indicated root rowid.
     * 
     *  @param id The root rowid identifier.  The value {@link
     *  #NAME_DIRECTORY_ROOT} is reserved.  The #of root ids available
     *  is reported by #getRootCount().
     *
     *  @see #getRootCount()
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _pageman.getFileHeader().getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount()
     *  @see #getRoot( int id )
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _pageman.getFileHeader().setRoot( id, rowid );
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        Map nameDirectory = getNameDirectory();
        Long recid = (Long) nameDirectory.get( name );
        if ( recid == null ) {
            return 0;
        }
        return recid.longValue();
    }

    /**
     * Set the record id of a named object.
     * 
     * @param name The name to associate with the recid in the dictionary.
     * 
     * @param recid The recid of the named object.  When recid == 0L the
     * name is removed from the dictionary.
     */
    public void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        Map nameDirectory = getNameDirectory();
        if ( recid == 0 ) {
            // remove from hashtable
            nameDirectory.remove( name );
        } else {
            nameDirectory.put( name, new Long( recid ) );
        }
        saveNameDirectory( nameDirectory );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();

        if( _bufMgr != null ) {
        	
        	_bufMgr.commit();
        	
        }
        
        _pageman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();
        
        if( _bufMgr != null ) {
        	
        	_bufMgr.abort();
        	
        }

        _pageman.rollback();
    }


    /**
     * Load name directory
     */
    private Map getNameDirectory()
        throws IOException
    {
        // retrieve directory of named hashtable
        long nameDirectory_recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( nameDirectory_recid == 0 ) {
            _nameDirectory = new HashMap();
            nameDirectory_recid = insert( _nameDirectory, DefaultSerializer.INSTANCE );
            setRoot( NAME_DIRECTORY_ROOT, nameDirectory_recid );
        } else {
            _nameDirectory = (Map) fetch( nameDirectory_recid, DefaultSerializer.INSTANCE );
        }
        return _nameDirectory;
    }


    private void saveNameDirectory( Map directory )
        throws IOException
    {
        long recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( recid == 0 ) {
            throw new IOException( "Name directory must exist" );
        }
        update( recid, _nameDirectory, DefaultSerializer.INSTANCE );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException.
     */
    /*private*/ void checkIfClosed()
        throws IllegalStateException
    {
        if ( _file == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }

}
