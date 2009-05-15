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
 * $Id
 */
package jdbm.helper;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;

/**
 * Wraps a deterministic cache policy with a <q>Level-2</q> cache based on
 * J2SE's {@link WeakReference weak references}. Weak references allow
 * this cache to keep references to objects until the memory they occupy
 * is required elsewhere.
 * <p>
 * Since the {@link CachePolicy} interface requires an event be fired
 * when an object is evicted, and the event contains the actual object,
 * this class cannot be a stand-alone implementation of
 * <code>CachePolicy</code>. This limitation arises because Java References
 * does not support notification before references are cleared; nor do
 * they support reaching soft referents. Therefore, this wrapper cache
 * aggressively notifies evictions: events are fired when the objects are
 * evicted from the internal cache. Consequently, the soft cache may return
 * a non-null object when <code>get( )</code> is called, even if that
 * object was said to have been evicted.
 * <p>
 * The current implementation uses a hash structure for its internal key
 * to value mappings.
 * <p>
 * Note: this component's publicly exposed methods are not threadsafe;
 * potentially concurrent code should synchronize on the cache instance.
 * <p>
 * 
 * This implementation was derived from {@link SoftCache}.  Any updates should
 * probably be applied to both {@link WeakCache} and {@link SoftCache} since
 * they are basically the same code.
 * <p>
 *
 * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: WeakCache.java,v 1.4 2006/06/03 18:22:46 thompsonbry Exp $
 */
public class WeakCache implements CachePolicy {
    
    /**
     * Default capacity of the L1 (internal) MRU cache.
     */
    public static final int L1_DEFAULT_CAPACITY = 128;

    /**
     * Default initial capacity of the L2 (soft reference) cache.
     */
    public static final int L2_INITIAL_CAPACITY = 128;

    /**
     * Default load factor for the L2 soft reference cache.
     */
    public static final float L2_DEFAULT_LOAD_FACTOR = 1.5f;

    private final ReferenceQueue _clearQueue = new ReferenceQueue();
    private final CachePolicy _internal;
    private final Map _cacheMap;
    /**
     * Initial capacity (from constructor).
     */
    private int _capacity;
    /**
     * Load factor (from constructor).
     */
    private float _loadFactor;
    /**
     * High tide (largest size achieved by the soft reference hash structure).
     */
    private int _highTide = 0;
    /**
     * #of objects inserted into the cache.
     */
    private int _ninserts = 0;
    
    /**
     * Creates a soft-reference based L2 cache with a {@link MRU} cache as
     * the internal (L1) cache. The soft reference cache uses the
     * default load capacity of 1.5f, which is intended to sacrifice some
     * performance for space. This compromise is reasonable, since all
     * {@link #get(Object) get( )s} first try the L1 cache anyway. The
     * internal MRU is given a capacity of 128 elements.
     */
    public WeakCache() {
        this(new MRU(L1_DEFAULT_CAPACITY));
    }

    /**
     * Creates a soft-reference based L2 cache wrapping the specified
     * L1 cache.
     *
     * @param internal non null internal cache.
     * @throws NullPointerException if the internal cache is null.
     */
    public WeakCache(CachePolicy internal) throws NullPointerException {
        this( L2_INITIAL_CAPACITY, L2_DEFAULT_LOAD_FACTOR, internal);
    }

    public WeakCache(float loadFactor, CachePolicy internal) throws IllegalArgumentException, NullPointerException {
        this( L2_INITIAL_CAPACITY, loadFactor, internal );
    }

    /**
     * Creates a soft-reference based L2 cache wrapping the specified
     * L1 cache. This constructor is somewhat implementation-specific,
     * so users are encouraged to use {@link #WeakCache(CachePolicy)}
     * instead.
     *
     * @param capacity initial capacity of the soft cache's hash structure.
     * @param loadFactor load factor that the soft cache's hash structure
     *        should use.
     * @param internal non null internal cache.
     * @throws IllegalArgumentException if the load factor is nonpositive.
     * @throws NullPointerException if the internal cache is null.
     */
    public WeakCache(int capacity, float loadFactor, CachePolicy internal) throws IllegalArgumentException, NullPointerException {
        if (internal == null) {
            throw new NullPointerException("Internal cache cannot be null.");
        }
        _internal = internal;
        _capacity = capacity;
        _loadFactor = loadFactor;
        _cacheMap = new HashMap( capacity, loadFactor);
    }

    /**
     * Writes some debug information.
     */
    protected void finalize() throws Throwable {
        super.finalize();
        System.err.println( "WeakCache: initialCapacity="+_capacity+", loadFactor="+_loadFactor+", highTide="+_highTide+", ninserts="+_ninserts);
    }

    /**
     * The inner hard reference cache policy.
     */
    public CachePolicy getDelegate() {
        return _internal;
    }
    
    /**
     * Adds the specified value to the cache under the specified key. Note
     * that the object is added to both this and the internal cache.
     * @param key the (non-null) key to store the object under
     * @param value the (non-null) object to place in the cache
     * @throws CacheEvictionException exception that the internal cache
     *         would have experienced while evicting an object it currently
     *         cached.
     */
    public void put(Object key, Object value, boolean dirty, Serializer ser) throws CacheEvictionException {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        } else if (value == null) {
            throw new IllegalArgumentException("value cannot be null.");
        }
        _internal.put(key, value, dirty, ser );
        removeClearedEntries();
        _cacheMap.put(key, new Entry(key, value, /*dirty, ser,*/ _clearQueue));
        _ninserts++;
        
        int size = _cacheMap.size();
        if( size > _highTide ) {
            _highTide = size;
        }
    }

    /**
     * Gets the object cached under the specified key.
     * <p>
     * The cache is looked up in the following manner:
     * <ol>
     * <li>The internal (L1) cache is checked. If the object is found, it is
     *     returned.</li>
     * <li>This (L2) cache is checked. If the object is not found, then
     *     the caller is informed that the object is inaccessible.</li>
     * <li>Since the object exists in L2, but not in L1, the object is
     *     readded to L1 using {@link CachePolicy#put(Object, Object)}.</li>
     * <li>If the readding succeeds, the value is returned to caller.</li>
     * <li>If a cache eviction exception is encountered instead, we
     *     remove the object from L2 and behave as if the object was
     *     inaccessible.</li>
     * </ol>
     * @param key the key that the object was stored under.
     * @return the object stored under the key specified; null if the
     *         object is not (nolonger) accessible via this cache.
     */
    public Object get(Object key) {
        // first try the internal cache.
        Object value = _internal.get(key);
        if (value != null) {
            return value;
        }
        // poll and remove cleared references.
        removeClearedEntries();
        Entry entry = (Entry)_cacheMap.get(key);
        if (entry == null) { // object is not in cache.
            return null;
        }
        value = entry.getValue();
        if (value == null) { // object was in cache, but it was cleared.
            return null;
        }
        /*
         * We have the object. so we try to re-insert it into internal cache.
         * 
         * Note: The object is NOT dirty at this point since eviction from the
         * internal cache always proceeds clearing an entry from the outer
         * cache.  Since the object is not dirty we can also set the serializer
         * safely to null.  If the object becomes dirty then the application is
         * required to declare the serializer to use.  This all happens in the
         * CacheRecordManager class.
         */
        try {
            _internal.put(key, value, false, null);
        } catch (CacheEvictionException e) {
            // if the internal cache causes a fuss, we kick the object out.
            _cacheMap.remove(key);
            return null;
        }
        return value;
    }

    /**
     * Removes any object stored under the key specified. Note that the
     * object is removed from both this (L2) and the internal (L1)
     * cache.
     * @param key the key whose object should be removed
     */
    public void remove(Object key) {
        _cacheMap.remove(key);
        _internal.remove(key);
    }

    /**
     * Removes all objects in this (L2) and its internal (L1) cache.
     */
    public void removeAll() {
        _cacheMap.clear();
        _internal.removeAll();
    }

    /**
     * Gets all the objects stored by the internal (L1) cache.
     * @return an enumeration of objects in internal cache.
     */
    public Enumeration elements() {
        return _internal.elements();
    }

    /**
     * Gets all the entries stored by the internal (L1) cache.
     * @return an enumeration of entries in internal cache.
     */
    public Enumeration entries() {
        return _internal.entries();
    }

    /**
     * Adds the specified listener to this cache. Note that the events
     * fired by this correspond to the <em>internal</em> cache's events.
     * @param listener the (non-null) listener to add to this policy
     * @throws IllegalArgumentException if listener is null.
     */
    public void addListener(CachePolicyListener listener)
            throws IllegalArgumentException {
        _internal.addListener(listener);
    }

    /**
     * Removes a listener that was added earlier.
     * @param listener the listener to remove.
     */
    public void removeListener(CachePolicyListener listener) {
        _internal.removeListener(listener);
    }

    /**
     * Cleans the mapping structure of any obsolete entries. This is usually
     * called before insertions and lookups on the mapping structure. The
     * runtime of this is usually very small, but it can be as expensive as
     * n * log(n) if a large number of soft references were recently cleared.
     */
    private final void removeClearedEntries() {
        for (Reference r = _clearQueue.poll(); r != null; r = _clearQueue.poll()) {
            Object key = ((Entry)r).getKey();
            _cacheMap.remove(key);
        }
    }

    /**
     * <p>
     * Value objects we keep in the internal map. This contains the key in
     * addition to the value, because polling for cleared references returns
     * these instances, and having access to their corresponding keys
     * drastically improves the performance of removing the pair from the map
     * (see {@link WeakCache#removeClearedEntries()}.)
     * </p>
     * <p>
     * Note: Since dirty objects are installed when they are evicted from the
     * hard reference cache, it is impossible for there to be a dirty object on
     * the weak reference cache that is not also present in the hard reference
     * cache. Therefore the dirty flag and serializer are <em>only</em>
     * maintained by the backing hard reference cache.
     * </p>
     */
    private final static class Entry extends WeakReference implements ICacheEntry {
        private final Object _key;
//        boolean _dirty;
//        Serializer _ser;

        /**
         * Constructor that uses <code>value</code> as the soft
         * reference's referent.
         */
        public Entry(Object key, Object value, /*boolean dirty, Serializer ser,*/ ReferenceQueue queue) {
            super(value, queue);
            _key = key;
//            _dirty = dirty;
//            _ser = ser;
        }

        /**
         * Gets the key
         * @return the key associated with this value.
         */
        public Object getKey() {
            return _key;
        }

        /**
         * Gets the value
         * @return the value; null if it is no longer accessible
         */
        public Object getValue() {
            return this.get();
        }
        
        /**
         * @exception UnsupportedOperationException always.
         */
        public boolean isDirty() {
            throw new UnsupportedOperationException();
//            return _dirty;
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public void setDirty(boolean dirty) {
            throw new UnsupportedOperationException();
//            _dirty = dirty;
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public Serializer getSerializer() {
            throw new UnsupportedOperationException();
//            return _ser;
        }
    
    }
    
}
