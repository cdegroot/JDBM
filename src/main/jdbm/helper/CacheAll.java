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
 * $Id: CacheAll.java,v 1.4 2006/05/03 16:01:18 thompsonbry Exp $
 */

package jdbm.helper;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;


/**
 * Caches all items using hard references. This is not intended for general use.
 * However it is useful for profiling jdbm since it causes all serialization and
 * allocation of physical rows to be deferred until the commit. The cache is NOT
 * reset at the commit, so the size of the cache will never decrease.
 * <p>
 * 
 * Methods are *not* synchronized, so no concurrent access is allowed.
 * <p>
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert </a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

final public class CacheAll implements CachePolicy {

    /** Cached object hashtable */
    Hashtable _hash = new Hashtable();

    /**
     * Cache eviction listeners
     */
    Vector listeners = new Vector();

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
     * Construct a cache that will retain all objects using hard references.
     */
    public CacheAll() {
        removeAll();
    }

    /**
     * Writes some debug information.
     */
    protected void finalize() throws Throwable {
        super.finalize();
        System.err.println( "CacheAll: size="+_hash.size());
    }

    /**
     * Place an object in the cache.
     */
    public void put(Object key, Object value, boolean dirty, Serializer ser ) throws CacheEvictionException {
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            entry._value = value;
            entry._dirty = dirty;
            entry._ser = ser;
            touchEntry(entry);
        } else {
            entry = new CacheEntry(key, value, dirty, ser);
            addEntry(entry);
            _hash.put(entry._key, entry);
        }
    }


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
     * Remove an object from the cache.
     * <p>
     * 
     * Note: This gets invoked when a record is deleted, so it makes
     * sense to remove it from the cache at that time.
     * <p>
     */
    public void remove(Object key) {
//        throw new UnsupportedOperationException();
        CacheEntry entry = (CacheEntry)_hash.get(key);
        if (entry != null) {
            removeEntry(entry);
            _hash.remove(entry.getKey());
        }
    }


    /**
     * Remove all objects from the cache and set the cache to its
     * capacity.
     */
    public void removeAll() {
        _hash = new Hashtable();
    }


    /**
     * Enumerate elements' values in the cache
     */
    public Enumeration elements() {
        return new CacheAllEnumeration(_hash.elements(), true);
    }

    /**
     * Enumerate entries in the cache.
     */
    public Enumeration entries() {
        return new CacheAllEnumeration(_hash.elements(), false);
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
            _last._next = entry;
            entry._previous = _last;
            _last = entry;
        }
    }


    /**
     * Remove a CacheEntry from linked list
     */
    protected void removeEntry(CacheEntry entry) {
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
        entry._next = null;
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

//    /**
//     * Purge least recently used object from the cache
//     *
//     * @return recyclable CacheEntry
//     */
//    protected CacheEntry purgeEntry() throws CacheEvictionException {
//        CacheEntry entry = _first;
//
//        // Notify policy listeners first. if any of them throw an
//        // eviction exception, then the internal data structure
//        // remains untouched.
//        CachePolicyListener listener;
//        for (int i=0; i<listeners.size(); i++) {
//            listener = (CachePolicyListener)listeners.elementAt(i);
//            listener.cacheObjectEvicted(entry.getValue());
//        }
//
//        removeEntry(entry);
//        _hash.remove(entry.getKey());
//
//        entry.setValue(null);
//        return entry;
//    }

}

/**
 * Enumeration wrapper optionallly returns actual user objects instead of
 * CacheEntries.
 */
class CacheAllEnumeration implements Enumeration {
    final private Enumeration _enum;
    final private boolean _resolve;

    /**
     * 
     * @param enume 
     * @param resolve When true resolve the entry to the application object
     * associated with that entry.
     */
    CacheAllEnumeration(Enumeration enume, boolean resolve) {
        _enum = enume;
        _resolve = resolve;
    }

    public boolean hasMoreElements() {
        return _enum.hasMoreElements();
    }

    public Object nextElement() {
        CacheEntry entry = (CacheEntry)_enum.nextElement();
        if( _resolve ) {
            return entry._value;
        } else {
            return entry;
        }
    }
}
