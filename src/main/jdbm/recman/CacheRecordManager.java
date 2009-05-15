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
 * $Id: CacheRecordManager.java,v 1.15 2006/06/03 18:22:46 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.util.Enumeration;

import jdbm.RecordManager;
import jdbm.helper.CacheEvictionException;
import jdbm.helper.CachePolicy;
import jdbm.helper.CachePolicyListener;
import jdbm.helper.ICacheEntry;
import jdbm.helper.ISerializationHandler;
import jdbm.helper.Serializer;
import jdbm.helper.WrappedRuntimeException;

/**
 *  A RecordManager wrapping and caching another RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @author <a href="thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: CacheRecordManager.java,v 1.15 2006/06/03 18:22:46 thompsonbry Exp $
 */
public class CacheRecordManager
    implements RecordManager
{

    /**
     * Wrapped RecordManager
     */
    protected RecordManager _recman;


    /**
     * Cache for underlying RecordManager
     */
    protected CachePolicy _cache;

    /**
     * When true, insert operations are lazy and do not immediately allocate
     * the physical row.
     * 
     * @see jdbm.RecordManagerOptions#LAZY_INSERT
     */
    protected boolean _lazyInsert = false;

    /**
     * Construct a CacheRecordManager wrapping another RecordManager and
     * using a given cache policy.
     *
     * @param recman Wrapped RecordManager
     * @param cache Cache policy
     */
    public CacheRecordManager( RecordManager recman, CachePolicy cache )
    {
        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }
        if ( cache == null ) {
            throw new IllegalArgumentException( "Argument 'cache' is null" );
        }
        _recman = recman;
        _cache = cache;
        
        _cache.addListener( new CacheListener() );
    }
    
    /**
     * Get the underlying Record Manager.
     *
     * @return underlying RecordManager or null if CacheRecordManager has
     *         been closed. 
     */
    public RecordManager getRecordManager()
    {
        return _recman;
    }

    public RecordManager getBaseRecordManager()
    {
    
        return _recman.getBaseRecordManager();
        
    }

//    public RecordManager getOuterRecordManager()
//    {
//        
//        return getBaseRecordManager().getOuterRecordManager();
//        
//    }
//    
//    public void setOuterRecordManager( RecordManager recman )
//    {
//        
//        getBaseRecordManager().setOuterRecordManager( recman );
//        
//    }
    
    public ISerializationHandler getSerializationHandler()
    {
        
        checkIfClosed();
        
        return _recman.getSerializationHandler();
        
    }

//    public void setDefaultSerializer( Serializer ser )
//    {
//        
//        checkIfClosed();
//        
//        _recman.setDefaultSerializer( ser );
//        
//    }
    
    /**
     * Get the underlying cache policy
     *
     * @return underlying CachePolicy or null if CacheRecordManager has
     *         been closed. 
     */
    public CachePolicy getCachePolicy()
    {
        return _cache;
    }

    
    /**
     *  Inserts a new record using the default serialization handler.
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
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized long insert( Object obj, Serializer serializer )
        throws IOException
    {
        checkIfClosed();
        long recid;
        boolean isDirty;
        
        if( _lazyInsert ) {
            /*
             * Alternative strategy defers the physical record insertion until
             * the object is evicted from the cache. The goal of this strategy
             * is to avoid "eager" serialization of the object and "eager"
             * allocation of a physical row. This approach is beneficial if you
             * expect to update the object before a commit.
             * 
             * The insert operation only assigns a logical row id. The physical
             * row id is zero. The cache entry is marked as "dirty" since we
             * have not actually serialized the object onto a physical row. We
             * test for a zero physical row id during update in
             * {@link BaseRecordManager)and insert the physical row if theblockId
             * is zero.
             */
            Location physRowId = new Location( 0L, (short) 0 );
            recid = ((BaseRecordManager)_recman)._logMgr.insert( physRowId ).toLong();
            isDirty = true; // since we are not serializing the obj into a physical record.
        } else {
            /*
             * Original strategy.  Eagerly allocates the physical row and immediately
             * serializes the object onto that row.
             */
            recid = _recman.insert( obj, serializer );
            isDirty = false;
        }
        
        try {
            _cache.put( new Long( recid ), obj, isDirty, serializer );
        } catch ( CacheEvictionException except ) {
            throw new WrappedRuntimeException( except );
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

        _recman.delete( recid );
        _cache.remove( new Long( recid ) );
    }


    /**
     *  Updates a record using the default serialization handler.
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
    public synchronized void update( long recid, Object obj, 
                                     Serializer serializer )
        throws IOException
    {
//        CacheEntry  entry;
        Long        id;
        
        checkIfClosed();

        id = new Long( recid );
        try {
//            entry = (CacheEntry) _cache.get( id );
//            if ( entry != null ) {
//                // reuse existing cache entry
//                entry._obj = obj;
//                entry._serializer = serializer;
//                entry._isDirty = true;
//            } else {
                _cache.put( id, obj, true, serializer );
//            }
        } catch ( CacheEvictionException except ) {
            throw new IOException( except.getMessage() );
        }
    }


    /**
     *  Fetches a record using the default serialization handler.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetch( long recid )
        throws IOException
    {
        return fetch( recid, null );
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
        checkIfClosed();

        Long id = new Long( recid );
        Object obj = _cache.get( id );
        if ( obj == null ) {
            obj = _recman.fetch( recid, serializer );
            if( obj != null ) {
                try {
                    _cache.put( id, obj, false, serializer );
                } catch ( CacheEvictionException except ) {
                    throw new WrappedRuntimeException( except );
                }
            }
        }
        return obj;
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

        updateCacheEntries();
        _recman.close();
        _recman = null;
        _cache = null;
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public synchronized int getRootCount()
    {
        checkIfClosed();

        return _recman.getRootCount();
    }


    /**
     *  Returns the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _recman.getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _recman.setRoot( id, rowid );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        updateCacheEntries();
        _recman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();

        _recman.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        _cache.removeAll();
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public synchronized long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        return _recman.getNamedObject( name );
    }


    /**
     * Set the record id of a named object.
     */
    public synchronized void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.setNamedObject( name, recid );
    }

    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _recman == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }

    
    /**
     * Update all dirty cache objects to the underlying RecordManager.
     */
    protected void updateCacheEntries()
        throws IOException
    {
        /*
         * Scan cache, update()ing dirty objects on the BaseRecordManager.
         */
        Enumeration enume = _cache.entries();
        while ( enume.hasMoreElements() ) {
            ICacheEntry entry = (ICacheEntry) enume.nextElement();
            if ( entry.isDirty() ) {
            	long recid = ((Long)entry.getKey()).longValue();
            	Object value = entry.getValue();
                _recman.update( recid, value, entry.getSerializer() );
                entry.setDirty( false );
            }
        }
    }

    /**
	 * Installs dirty objects evicted from the object cache onto the delegate
	 * {@link RecordManager}.
	 */
    final private class CacheListener
        implements CachePolicyListener
    {
        
        /**
         * Notification that cache is evicting an object. The code handles this
         * event by using the delegate record manager to install dirty objects
         * onto the database. Since the object is being evicted from the cache
         * and its cache entry will be recycled we do not have to clear the
         * dirty flag.
         * 
         * @param key
         *            the object identifier
         * @param obj
         *            the object evicted from the cache
         * @param dirty
         *            true iff the object is dirty and needs to be installed on
         *            the database.
         * @param ser
         *            The serializer to be used to install the object onto the
         *            database.
         */
        public void cacheObjectEvicted( Object key, Object obj, boolean dirty, Serializer ser ) 
            throws CacheEvictionException
        {
//            CacheEntry entry = (CacheEntry) obj;
//            if ( entry._isDirty ) {
            if( dirty ) {
                try {
//                    _recman.update( entry._recid, entry._obj, entry._serializer );
                    _recman.update( ((Long)key).longValue(), obj, ser );
                } catch ( IOException except ) {
                    throw new CacheEvictionException( except );
                }
            }
        }
        
    }
}
