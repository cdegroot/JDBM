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
 * $Id: MRU.java,v 1.13 2006/06/03 18:22:46 thompsonbry Exp $
 */

package jdbm.helper;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 *  MRU - Most Recently Used cache policy.
 *
 *  Methods are *not* synchronized, so no concurrent access is allowed.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: MRU.java,v 1.13 2006/06/03 18:22:46 thompsonbry Exp $
 */
final public class MRU implements CachePolicy {

    /** Cached object hashtable */
    Hashtable _hash = new Hashtable();

    /**
     * Maximum number of objects in the cache.
     */
    int _max;
    
    /**
     * Load factor for the cache.
     */
    float _loadFactor;

    /**
     * Beginning of linked-list of cache elements.  First entry is element
     * which has been used least recently (LRU position).
     */
    CacheEntry _first;

    /**
     * End of linked-list of cache elements.  Last entry is element
     * which has been used most recently (MRU position).
     */
    CacheEntry _last;


    /**
     * Cache eviction listeners
     */
    Vector listeners = new Vector();


    /**
     * Construct an MRU with a given maximum number of objects.
     */
    public MRU(int max) {
        this( max, 0.75f );
    }
    
    public MRU(int max, float loadFactor ) {
        if (max <= 0) {
            throw new IllegalArgumentException("MRU cache must contain at least one entry");
        }
        _max = max;
        _loadFactor = loadFactor;
    }

    /**
     * The capacity of the cache.
     */
    public int capacity() {
        return _max;
    }

    /**
	 * <p>
	 * Place an object in the cache.
	 * </p>
	 * <p>
	 * Cache evictions are only performed at or <i>over</i> capacity, but not
	 * for reentrant invocations. If a cache eviction causes a nested
	 * {@link #put(Object, Object, boolean, Serializer))} cache enters a
	 * temporary <em>over capacity</em> condition. The nested eviction is
	 * effectively deferred and a new cache entry is created for the incoming
	 * object rather than recycling the LRU cache entry. This temporary over
	 * capacity state exists until the primary eviction event has been handled,
	 * at which point entries are purged from the cache until it has one free
	 * entry. That free entry is then used to cache the incoming object which
	 * triggered the outer eviction event.
	 * </p>
	 * <p>
	 * This is not the only coherent manner in which nested eviction events
	 * could be handled, but it is perhaps the simplest. This technique MUST NOT
	 * be used with an open array hash table since the temporary over capacity
	 * condition would not be supported.
	 * </p>
	 */
    public void put(Object key, Object value, boolean dirty, Serializer ser) throws CacheEvictionException {
    	reentrantPutCounter++;
    	try {
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            entry._value = value;
            entry._dirty = dirty;
            entry._ser = ser;
            touchEntry(entry);
        } else {

            if (_hash.size() >= _max && reentrantPutCounter == 1 ) {
            	while( _hash.size() >= _max ) {
                 /*
                  * Purge entries until just under capacity and then
                  * recycle the last purged entry.
                  */
                entry = purgeEntry();
            	}
                entry._key = key;
                entry._value = value;
                entry._dirty = dirty;
                entry._ser = ser;
            } else {
            	/*
				 * Used when the cache is under capacity or during reentrant
				 * invocations of put().
				 */
                entry = new CacheEntry(key, value, dirty, ser);
            }
            addEntry(entry);
            _hash.put(entry._key, entry);
        }
    	} finally {
    		reentrantPutCounter--;
    	}
    }
    
    /**
	 * Used to track and handle reentrant calls to put(). The value of the
	 * counter is the number of times that put() occurs in the stack frame.
	 * E.g., the counter value will be zero when put() is not in the stack
	 * frame, a non-recursive invocation will show a counter value of 1; and a
	 * counter value of 2 or more indicates a reentrant invocation is in
	 * progress.
	 * 
	 * @see #put(Object, Object, boolean, Serializer))
	 */
    private int reentrantPutCounter = 0;

    /**
     * Obtain an object in the cache
     */
    public Object get(Object key) {
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            touchEntry(entry);
            return entry._value;
        } else {
            return null;
        }
    }


    /**
     * Remove an object from the cache
     */
    public void remove(Object key) {
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            removeEntry(entry);
            _hash.remove(entry._key);
        }
    }


    /**
     * Remove all objects from the cache and set the cache to its
     * capacity.
     */
    public void removeAll() {
        _hash = new Hashtable( _max, _loadFactor );
        _first = null;
        _last = null;
    }


    /**
     * Enumerate elements' values in the cache (from LRU to MRU).
     */
    public Enumeration elements() {
//        return new MRUEnumeration(_hash.elements(), true);
        return new MRUEnumeration(this, true);
    }

    /**
     * Enumerate entries in the cache (from LRU to MRU).
     */
    public Enumeration entries() {
//        return new MRUEnumeration(_hash.elements(), false);
        return new MRUEnumeration(this, false);
    }

    /**
     * Add a listener to this cache policy
     *
     * @param listener Listener to add to this policy
     */
    public void addListener(CachePolicyListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Cannot add null listener.");
        }
        if ( ! listeners.contains(listener)) {
            listeners.addElement(listener);
        }
    }

    /**
     * Remove a listener from this cache policy
     *
     * @param listener Listener to remove from this policy
     */
    public void removeListener(CachePolicyListener listener) {
        listeners.removeElement(listener);
    }

    /**
     * Add a CacheEntry.  Entry goes at the end of the list (MRU position).
     */
    protected void addEntry(CacheEntry entry) {
        if (_first == null) {
            _first = entry;
            _last = entry;
        } else {
            _last._next = entry;
            entry._previous = _last;
            _last = entry;
        }
    }


    /**
     * Remove a CacheEntry from linked list.  This is used together with addEntry
     * to update an entry so that it occurs in the MRU position.
     */
    protected void removeEntry(CacheEntry entry) {
    	if( _cacheOrderChangeListeners != null ) {
    		fireCacheOrderChangeEvent(true, entry);
    	}
        CacheEntry previous = entry._previous;
        CacheEntry next = entry._next;
        if (entry == _first) {
            _first = next;
        }
        if (_last == entry) {
            _last = previous;
        }
        if (previous != null) {
            previous._next = next;
        }
        if (next != null) {
            next._previous = previous;
        }
        entry._previous = null;
        entry._next = null; // does not clear: _dirty, _key, _value, _ser.
    }

    /**
     * Place entry at the end of linked list -- Most Recently Used
     */
    protected void touchEntry(CacheEntry entry) {
        if (_last == entry) {
            return;
        }
        removeEntry(entry);
        addEntry(entry);
    }

    /**
     * Purge least recently used object from the cache
     *
     * @return recyclable CacheEntry
     */
    protected CacheEntry purgeEntry() throws CacheEvictionException {
        CacheEntry entry = _first;

        // Notify policy listeners first. if any of them throw an
        // eviction exception, then the internal data structure
        // remains untouched.
        final int n = listeners.size();
        CachePolicyListener listener;
        for (int i=0; i<n; i++) {
            listener = (CachePolicyListener)listeners.elementAt(i);
            listener.cacheObjectEvicted(entry._key, entry._value, entry._dirty, entry._ser);
        }

        removeEntry(entry); // clears: _previous, _next.
        _hash.remove(entry._key);

        entry._value = null; // not cleared: _dirty, _key, _ser.
        return entry;
    }

    /**
	 * Registers a listener for removeEntry events. This is used by the
	 * {@link LRUIterator} to handle concurrent modifications of the cache
	 * ordering during traversal.
	 */
    
    synchronized protected void addCacheOrderChangeListener( ICacheOrderChangeListener l ) {
    	if( _cacheOrderChangeListeners == null ) {
    		_cacheOrderChangeListeners = new Vector();
    	}
    	_cacheOrderChangeListeners.add(l);
    }
    
    /**
	 * Unregister the listener. This is safe to invoke when the listener is not
	 * registered.
	 * 
	 * @param l
	 *            The listener.
	 */
    synchronized protected void removeCacheOrderChangeListener( ICacheOrderChangeListener l ) {
    	if (_cacheOrderChangeListeners == null)
			return;
    	_cacheOrderChangeListeners.remove(l);
    	if( _cacheOrderChangeListeners.size() == 0 ) {
    		_cacheOrderChangeListeners = null;
    	}
    }
    
    private void fireCacheOrderChangeEvent( boolean removed, ICacheEntry entry ) {
    	if( _cacheOrderChangeListeners.size() == 0 ) return; 
    	ICacheOrderChangeListener[] listeners = (ICacheOrderChangeListener[]) _cacheOrderChangeListeners
				.toArray(new ICacheOrderChangeListener[] {});
    	for( int i=0; i<listeners.length; i++ ) {
    		ICacheOrderChangeListener l = listeners[ i ];
    		if( removed ) {
    			l.willRemove( entry );
    		} else {
    			throw new UnsupportedOperationException(); // feature is not implemented.
    		}
    	}
	}

    /**
     * Lazily allocated and eagerly freed.
     */
    private Vector _cacheOrderChangeListeners = null;
    
    protected static interface ICacheOrderChangeListener {
    	public void willRemove( ICacheEntry entry );
//    	public void didAdd(ICacheEntryEntry);
    }

}

/**
 * State information for cache entries.
 * <p>
 * 
 * Note: In the name of performance the fields on this class have been exposed
 * to the package and both this class and and the cache implementation have been
 * marked as <em>final</em>. The public accessor methods have been retained
 * for extra-package use.
 * <p>
 */
final class CacheEntry implements ICacheEntry {
    /*private*/ Object _key;
    /*private*/ Object _value;
    boolean _dirty;
    Serializer _ser;

    /*private*/ CacheEntry _previous;
    /*private*/ CacheEntry _next;

    CacheEntry(Object key, Object value, boolean dirty, Serializer ser) {
        _key = key;
        _value = value;
        _dirty = dirty;
        _ser = ser;
    }

    public Object getKey() {
        return _key;
    }

    void setKey(Object obj) {
        _key = obj;
    }

    public Object getValue() {
        return _value;
    }

    void setValue(Object obj) {
        _value = obj;
    }

    CacheEntry getPrevious() {
        return _previous;
    }

    void setPrevious(CacheEntry entry) {
        _previous = entry;
    }

    CacheEntry getNext() {
        return _next;
    }

    void setNext(CacheEntry entry) {
        _next = entry;
    }
    
    public boolean isDirty() {
        return _dirty;
    }
    
    public void setDirty(boolean dirty) {
        _dirty = dirty;
    }
    
    public Serializer getSerializer() {
        return _ser;
    }
}

/**
 * <p>
 * Enumeration wrapper optionally returns actual user objects instead of
 * CacheEntries. Visitation is in order from LRU to MRU. Concurrent
 * modifications of the cache order are handled gracefully.
 * </p>
 * <p>
 * This class provide fast visitation from LRU to MRU by chasing references. In
 * order to support concurrent modification of the cache order during traversal,
 * an instance uses a protocol by it is informed of changes in the cache order.
 * There are two basic operations that effect the cache order: addEntry and
 * removeEntry. addEntry always inserts the entry in the MRU position, so it can
 * not effect the visitation order. However, removeEntry could unlink the entry
 * that the iterator will use to reach the next entry (via its next reference).
 * The iterator must therefore receive notice when a cache entry is about to be
 * removed. If the entry is the same entry that the iterator would visit next in
 * the LRU to MRU ordering, then the iterator advances its state to the next
 * entry that it would visit. When removeEntry then removes the entry from the
 * cache ordering, the iterator correctly visits the next cache entry in the new
 * ordering.
 * </p>
 * 
 */
class MRUEnumeration implements Enumeration, jdbm.helper.MRU.ICacheOrderChangeListener {
//    private final Enumeration _enum;
//    private final boolean _resolve;
//
//    MRUEnumeration(Enumeration enume, boolean resolve) {
//        _enum = enume;
//        _resolve = resolve;
//    }
//
//    public boolean hasMoreElements() {
//        return _enum.hasMoreElements();
//    }
//
//    public Object nextElement() {
//        CacheEntry entry = (CacheEntry)_enum.nextElement();
//        if( _resolve ) {
//            return entry._value;
//        } else {
//            return entry;
//        }
//    }

    private final MRU cache;
    private final boolean resolveObjects;
    private CacheEntry next;
    private CacheEntry lastVisited = null;
    
    /**
	 * Traverses the {@link MRU} in the its natural ordering.
	 * 
	 * @param cache
	 *            The {@link MRU}.
	 * @param resolveObjects
	 *            When true, the enumeration will visit the application objects
	 *            in the cache. When false it will visit the {@link ICacheEntry}
	 *            entries themselves.
	 */
    MRUEnumeration( MRU cache, boolean resolveObjects )
    {
        this.cache = cache;
        this.next = cache._first;
        this.resolveObjects = resolveObjects;
        cache.addCacheOrderChangeListener(this);
    }

    public boolean hasMoreElements() {
        return next != null;
    }

    public Object nextElement() {
        if( next == null ) {
        	removeListener();
            throw new NoSuchElementException();
        }
        /* Optionally resolve the application object vs the entry for that
         * object.
         */
        Object ret = (resolveObjects ? next._value : next);
        /*
         * Advance the internal state of the enumeration.
         */
        lastVisited = next;
        next = next._next;
        if( next == null ) {
        	removeListener();
        }
        /*
         * Return either the cache entry or the resolve application object.
         */
        return ret;
    }

	public void willRemove(ICacheEntry entry) {
		if( entry == next ) {
			next = next._next;
		}
	}

	/**
	 * Unregister the iterator as a cache order change listener.  This is
	 * safe to invoke multiple times.
	 */
	private void removeListener() {
		cache.removeCacheOrderChangeListener(this);
	}

	//    /**
//     * Removes the last visited entry from the cache.
//     * 
//     * @exception IllegalStateException
//     *                if no entry has been visited yet.
//     */
//    public void remove() {
//        if( lastVisited == null ) {
//            throw new IllegalStateException();
//        }
//        cache.map.remove( new Long( lastVisited.key ) );
//        cache.removeEntry( lastVisited );
//    }
    
}
