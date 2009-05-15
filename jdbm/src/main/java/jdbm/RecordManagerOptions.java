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
 * $Id: RecordManagerOptions.java,v 1.7 2006/06/01 13:13:15 thompsonbry Exp $
 */

package jdbm;

import jdbm.recman.BufferedRecordInstallManager;
import jdbm.recman.RecordFile;

/**
 * Standard options for RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerOptions.java,v 1.7 2006/06/01 13:13:15 thompsonbry Exp $
 */
public class RecordManagerOptions
{

    /**
     * Option to create a thread-safe record manager.
     */
    public static final String PROVIDER_FACTORY = "jdbm.provider";


    /**
     * Option to create a thread-safe record manager.
     */
    public static final String THREAD_SAFE = "jdbm.threadSafe";


    /**
     * Option to automatically commit data after each operation.
     */
    public static final String AUTO_COMMIT = "jdbm.autoCommit";


    /**
     * Option to disable transaction (to increase performance at the cost of
     * potential data loss).
     */
    public static final String DISABLE_TRANSACTIONS = "jdbm.disableTransactions";

    /**
     * Option to specify how frequently changes should be automatically committed.  This option
     * only applies if transactions are disabled.
     */
    public static final String DISABLE_TRANSACTIONS_AUTOCOMMITINTERVAL = "jdbm.disableTransactions.autoCommitInterval";
	
    /**
     * The default is 10000 pages, which is 80MB since each page is 8KB.
     * 
     * @see #DISABLE_TRANSACTIONS_AUTOCOMMITINTERVAL
     */
    public static final String DISABLE_TRANSACTIONS_AUTOCOMMITINTERVAL_DEFAULT = "10000";
	
    /**
     * Option to specify whether the record manager's contents should be synced to disk when it is closed.
     * Normally, the transaction manager takes care of syncing to disk, but if transactions are disabled,
     * an explicit sync() is required to absolutely guarantee that data is written to the physical disk
     * Otherwise, it could be stored in the OS's file cache and lost if the OS crashes, or a disk becomes
     * unavailable (i.e. the infamous "Delayed write failure" in Windows).
     * 
     * The default is 'true' - i.e. perform the sync
     */
    public static final String DISABLE_TRANSACTIONS_PERFORMSYNCONCLOSE = "jdbm.disableTransactions.syncOnClose";

    //
    // Object cache options.
    //
    
    /**
     * Cache type.
     * 
     * @see #NORMAL_CACHE
     * @see #SOFT_REF_CACHE
     * @see #WEAK_REF_CACHE
     * @see #NO_CACHE
     */
    public static final String CACHE_TYPE = "jdbm.cache.type";


    /**
     * Use normal (strong) object references for the record cache.<p>
     * 
     * The size of the cache is configured by the {@link #CACHE_SIZE} property
     * and defaults to <code>1000</code>.<p> 
     */
    public static final String NORMAL_CACHE = "normal";


    /**
     * Use soft references {@link java.lang.ref.SoftReference} for the record
     * cache instead of the default normal object references. The soft reference
     * cache uses an L1 (internal) MRU cache and wraps it with an L2 soft
     * reference cache. Since objects are no longer available once they have
     * been finalized, objects are serialized and updated against a physical row
     * when they are ejected from the L1 MRU cache even though they might still
     * be softly reachable. Soft references are cleared at the discretion of the
     * garbage collector in response to memory demand.
     * <p>
     * 
     * The size of the L1 (internal) MRU cache is configured by the
     * {@link #CACHE_SIZE} property and defaults to <code>128</code>. The
     * load factor may be specified using {@link #CACHE_LOAD_FACTOR}.
     * <p>
     * 
     * The initial capacity of the L2 (soft reference) cache is configured by the
     * {@link #L2_CACHE_SIZE} property.  The load factor is configured by the {@link
     * #L2_CACHE_LOAD_FACTOR} property.  When the load factor is not explicitly set
     * it defaults to <code>1.5</code>, which trades off some performance against
     * space (but note that cache hits are first tested against the internal MRU
     * cache).
     * <p>
     * 
     * @see jdbm.helper.SoftCache
     */
    public static final String SOFT_REF_CACHE = "soft";

    /**
     * L1 cache size (when applicable).
     */
    public static final String CACHE_SIZE = "jdbm.cache.size";

    /**
     * L2 cache size (when applicable).
     */
    public static final String L2_CACHE_SIZE = "jdbm.cache.l2.size";

    /**
     * L1 cache load factor (when applicable).
     */
    public static final String CACHE_LOAD_FACTOR = "jdbm.cache.loadFactor";

    /**
     * L2 cache load factor (when applicable).
     */
    public static final String L2_CACHE_LOAD_FACTOR = "jdbm.cache.l2.loadFactor";

    /**
     * Use weak references {@link java.lang.ref.WeakReference} for the record
     * cache instead of the default normal object references.
     * <p>
     * Weak references do not prevent their referents from being made
     * finalizable, finalized, and then reclaimed.
     */
    public static final String WEAK_REF_CACHE = "weak";


    /**
     * Disable the record cache entirely (used for testing only)
     */
    public static final String NO_CACHE = "none";

    //
    // Serialization options.
    //
    
    /**
     * Sticky option sets the serialization handler for the record
     * manager.
     *
     * @see #SERIALIZER_DEFAULT
     * @see #SERIALIZER_EXTENSIBLE
     * @see RecordManager#getSerializationHandler()
     */
    public static final String SERIALIZER = "jdbm.serializer";
    
    /**
     * This option specifies the use of Java serialization by
     * default (when custom serializers are not explicitly used
     * in the code).  This option is the default value for {@link
     * #SERIALIZER} for backward compatibility with existing stores.
     * 
     * @see #SERIALIZER
     * @see jdbm.helper.DefaultSerializer
     */
    public static final String SERIALIZER_DEFAULT = "default";

    /**
     * This option specifies the use an extensible serialization
     * handler for the store.  This is NOT backward compatible
     * with existing stores.
     * 
     * @see #SERIALIZER
     * @see jdbm.helper.ExtensibleSerializer
     * 
     * @see #PROFILE_SERIALIZATION
     */
    public static final String SERIALIZER_EXTENSIBLE = "extensible";

    /**
     * Boolean option may be used to turn on profiling for the serializer. This
     * option defaults to false and is only supported by the extensible
     * serialization handler.
     */
    public static final String PROFILE_SERIALIZATION = "jdbm.serializer.profile";
    
    //
    // Compression options.
    //
    
    /**
     * Sticky option sets the {@link IRecordCompressor} for the store. When in
     * effect all jdbm records are compressed immediately after serialization
     * and decompressed immediately before deserialization.
     */
    public static final String COMPRESSOR = "jdbm.compressor";

    /**
     * The default value for the {@link #COMPRESSOR} option specifies
     * that records will not be compressed.  This option is the fastest
     * and is also backward compatible for existing stores.
     */
    public static final String COMPRESSOR_NONE = "none";

    /**
     * Record compression strategy that emphasizes speed over compression.
     */
    public static final String COMPRESSOR_BEST_SPEED = "bestSpeed";
    
    /**
     * Record compression strategy that emphasizes compression over speed.
     */
    public static final String COMPRESSOR_BEST_COMPRESSION = "bestCompression";

    //
    // Dump utility options.
    //
    
    /**
     * Option opens an existing store in a read only model and provides utility
     * methods for examining the various page lists and profiling the storage
     * used by the store.
     * 
     * @see jdbm.recman.DumpUtility
     */
    public static final String DUMP = "dump";

    //
    // Options in this section interact with the record allocation scheme.
    //
    
    /**
     * Boolean option to defer the allocation of the physical row in the
     * database until the record is evicted from the cache or a commit is
     * performed. This option can provide a substantial performance gain if you
     * update records several times before they are committed since
     * serialization is deferred and fewer physical row allocations are
     * performed.  The option only effect the runtime behavior of the store and
     * may be changed each time the store file is opened.
     */
    
    public static final String LAZY_INSERT = "jdbm.lazyInsert";
    
    /**
     * The default is <code>true</code> since this should be a backward
     * compatibile runtime option.
     */
    
    public static final String LAZY_INSERT_DEFAULT = "true";

    /**
	 * Boolean option to buffer installs of records onto pages. When the value
	 * of this option is true, then record installs are buffered and batched a
	 * page at a time. When the value of this option is false, record installs
	 * are performed as they are requested (record at a time).
	 * 
	 * @see BufferedRecordInstallManager
	 * 
	 * @todo This is a prototype feature. I find that it works for our
	 *       application in combination with the weak reference cache. There are
	 *       still some remaining bugs that need to be ironed out, including one
	 *       in the historical record at a time allocation (allocNew), where the
	 *       scan of records on a page can run past the end of the page. I also
	 *       plan to develop a record clustering feature based on this.
	 */
    public static final String BUFFERED_INSTALLS = "jdbm.bufferedInstalls";
    
    /**
	 * An integer option whose non-negative value is the waste margin (in bytes)
	 * for the {@link BufferedRecordInstallManager}. Once there are fewer that
	 * this many bytes available in a bucket, all records assigned to that
	 * bucket are installed onto a page.
	 */
    public static final String BUFFERED_INSTALLS_WASTE_MARGIN = "jdbm.bufferedInstalls.wasteMargin";
    public static final String BUFFERED_INSTALLS_WASTE_MARGIN_DEFAULT = "128";

    /**
	 * <p>
	 * An integer option whose non-negative value is the waste margin in bytes
	 * that will be accepted when a partly filled bucket MUST be installed due
	 * to a commit. If a bucket is filled to within this margin of its capacity
	 * then it will be installed onto a page. Otherwise it will be installed
	 * using the standard record at a time strategy.
	 * </p>
	 * <p>
	 * While using the standard physical row (re-)allocation techniques is
	 * slower, it has the following advantages: (a) it consumes free physical
	 * rows in the database; and (b) it does not leave as much empty space on
	 * the page. Buckets which are only "partly" full therefore get installed
	 * using the standard record at a time mechanisms. The heuristic for
	 * "partly" controlled by this option. Note that clustering is NOT possible
	 * when using the record at a time strategy!
	 * </p>
	 */
    
    public static final String BUFFERED_INSTALLS_WASTE_MARGIN2 = "jdbm.bufferedInstalls.wasteMargin2";
    public static final String BUFFERED_INSTALLS_WASTE_MARGIN2_DEFAULT = "256";

//    /**
//	 * An integer option whose value specifies the maximum waste that is
//	 * acceptable to the allocator when scanning free physical rows in a "first
//	 * fit" strategy. You can set the waste margin to ZERO (0) to disable the
//	 * waste margin and accept the first fit regardless of the waste. Waste is
//	 * measured as the available physical row capacity that is not used by the
//	 * new record.
//	 * 
//	 * @todo Integrate this option into the code.  This is difficult since the code
//	 * assumes that there is no configurable state for this and there is not really
//	 * any great place to attach the information.  Right now you have to edit the
//	 * value assigned to {@link FreePhysicalRowIdPage#wasteMargin2}.
//	 */
//    public static final String ALLOCATOR_WASTE_MARGIN1 = "jdbm.allocator.wasteMargin1";
//    public static final String ALLOCATOR_WASTE_MARGIN1_DEFAULT = "128";
//
//    /**
//	 * An integer option whose value specifies a secondary maximum waste that is
//	 * accepted after scanning an entire page of free physical row slots. The
//	 * best fit is tracked during the page scan. If the best fit does not exceed
//	 * this waste threshold then the best fit from that page is used. Otherwise
//	 * the next page of the free physical row list will be scanned. Once the
//	 * free physical row list has been exhausted, the allocator will examine the
//	 * last allocated in use page and then grab a new page.
//	 * 
//	 * @todo Integrate this option into the code.  This is difficult since the code
//	 * assumes that there is no configurable state for this and there is not really
//	 * any great place to attach the information.  Right now you have to edit the
//	 * value assigned to {@link FreePhysicalRowIdPage#wasteMargin2}.
//	 */
//    public static final String ALLOCATOR_WASTE_MARGIN2 = "jdbm.allocator.wasteMargin2";
//    public static final String ALLOCATOR_WASTE_MARGIN2_DEFAULT = ""+RecordFile.BLOCK_SIZE/4;

}
