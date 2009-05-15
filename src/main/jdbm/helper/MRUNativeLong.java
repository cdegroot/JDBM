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
 * $Id: MRUNativeLong.java,v 1.3 2006/05/03 16:01:18 thompsonbry Exp $
 */

package jdbm.helper;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import jdbm.helper.maps.LongKeyMap;
import jdbm.helper.maps.LongKeyOpenHashMap;

/**
 * Alternative MRU (Most Recently Used) cache policy based on the
 * {@link LongKeyOpenHashMap}.
 *
 * Methods are *not* synchronized, so no concurrent access is allowed.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class MRUNativeLong implements CachePolicy {

    /**
     * Cached object hashtable.
     */
    LongKeyMap _hash;

    /**
     * Maximum number of objects in the cache.
     */
    final int _max;
    
    /**
     * Load factor for the hash map.
     */
    final double _loadFactor;

    /**
     * Beginning of linked-list of cache elements.  First entry is element
     * which has been used least recently.
     */
    CacheEntry _first;

    /**
     * End of linked-list of cache elements.  Last entry is element
     * which has been used most recently.
     */
    CacheEntry _last;


    /**
     * Cache eviction listeners
     */
    Vector listeners = new Vector();


    /**
     * Construct an MRU with a given maximum number of objects.
     */
    public MRUNativeLong(int max) {
        this( max, LongKeyOpenHashMap.DEFAULT_LOAD_FACTOR );
    }

    public MRUNativeLong( int max, double loadFactor ) {
        if (max <= 0) {
            throw new IllegalArgumentException("MRU cache must contain at least one entry");
        }
        _max = max;
        _loadFactor = loadFactor;
        removeAll(); // initializes cache to its capacity.
    }

    /**
     * Return #of entries in the cache.
     */
    public int size() {
        return _hash.size();
    }

    /**
     * Place an object in the cache.
     */
    public void put(Object _key, Object value, boolean dirty, Serializer ser) throws CacheEvictionException {
        long key = ((Long)_key).longValue();
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            entry._value = value;
            entry._dirty = dirty;
            entry._ser = ser;
            touchEntry(entry);
        } else {

            if (_hash.size() == _max) {
                // purge and recycle entry
                entry = purgeEntry();
                entry._key = _key;
                entry._value = value;
                entry._dirty = dirty;
                entry._ser = ser;
            } else {
                entry = new CacheEntry( _key, value, dirty, ser );
            }
            addEntry(entry);
            _hash.put(key, entry);
        }
    }


    /**
     * Obtain an object in the cache
     */
    public Object get(Object _key) {
        long key = ((Long)_key).longValue();
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            touchEntry(entry);
            return entry.getValue();
        } else {
            return null;
        }
    }


    /**
     * Remove an object from the cache
     */
    public void remove(Object _key) {
        long key = ((Long)_key).longValue();
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            removeEntry(entry);
            _hash.remove(key);
        }
    }


    /**
     * Remove all objects from the cache and set the cache to its
     * capacity.
     */
    public void removeAll() {
        _hash = new LongKeyOpenHashMap(_max,_loadFactor);
        _first = null;
        _last = null;
    }


    /**
     * Enumerate elements' values in the cache
     */
    public Enumeration elements() {
        return new MRUEnumeration(_hash.values().iterator(),true);
    }

    /**
     * Enumerate entries in the cache
     */
    public Enumeration entries() {
        return new MRUEnumeration(_hash.values().iterator(),false);
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
     * Add a CacheEntry.  Entry goes at the end of the list.
     */
    protected void addEntry(CacheEntry entry) {
        if (_first == null) {
            _first = entry;
            _last = entry;
        } else {
            _last.setNext(entry);
            entry.setPrevious(_last);
            _last = entry;
        }
    }


    /**
     * Remove a CacheEntry from linked list
     */
    protected void removeEntry(CacheEntry entry) {
        if (entry == _first) {
            _first = entry.getNext();
        }
        if (_last == entry) {
            _last = entry.getPrevious();
        }
        CacheEntry previous = entry.getPrevious();
        CacheEntry next = entry.getNext();
        if (previous != null) {
            previous.setNext(next);
        }
        if (next != null) {
            next.setPrevious(previous);
        }
        entry.setPrevious(null);
        entry.setNext(null);
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
        CachePolicyListener listener;
        for (int i=0; i<listeners.size(); i++) {
            listener = (CachePolicyListener)listeners.elementAt(i);
            listener.cacheObjectEvicted(entry._key, entry._value, entry._dirty, entry._ser);
        }

        removeEntry(entry);
        _hash.remove(((Long)entry.getKey()).longValue());

        entry._value = null;
        return entry;
    }

/**
 * State information for cache entries.
 */
static class CacheEntry implements ICacheEntry {
    Object _key;
    Object _value;
    boolean _dirty;
    Serializer _ser;

    private CacheEntry _previous;
    private CacheEntry _next;

    CacheEntry(Object key, Object value, boolean dirty, Serializer ser ) {
        _key = key;
        _value = value;
        _dirty = dirty;
        _ser = ser;
    }

    public Object getKey() {
        return _key;
    }

    public Object getValue() {
        return _value;
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
 * Enumeration wrapper to return actual user objects instead of
 * CacheEntries.
 */
static class MRUEnumeration implements Enumeration {
    private Iterator _itr;
    private boolean _resolve;

    MRUEnumeration(Iterator itr, boolean resolve) {
        _itr = itr;
        _resolve = resolve;
    }

    public boolean hasMoreElements() {
        return _itr.hasNext();
    }

    public Object nextElement() {
        CacheEntry entry = (CacheEntry)_itr.next();
        if( _resolve ) {
            return entry.getValue();
        } else {
            return entry;
        }
    }
}

}
