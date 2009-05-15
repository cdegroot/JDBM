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
 * $Id: Provider.java,v 1.11 2006/06/03 18:22:46 thompsonbry Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.util.Properties;

import jdbm.RecordManager;
import jdbm.RecordManagerOptions;
import jdbm.RecordManagerProvider;

import jdbm.helper.CacheAll;
import jdbm.helper.CachePolicy;
import jdbm.helper.DefaultSerializationHandler;
import jdbm.helper.ExtensibleSerializer;
import jdbm.helper.ExtensibleSerializerSingleton;
import jdbm.helper.ISerializationHandler;
import jdbm.helper.MRU;
import jdbm.helper.MRUNativeLong;
import jdbm.helper.SoftCache;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.WeakCache;
import jdbm.helper.compessor.BestCompressionRecordCompressor;
import jdbm.helper.compessor.BestSpeedRecordCompressor;
import jdbm.helper.compessor.DefaultRecordCompressor;
import jdbm.helper.compessor.IRecordCompressor;
import jdbm.helper.maps.LongKeyOpenHashMap;

/**
 * Provider of the default RecordManager implementation.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: Provider.java,v 1.11 2006/06/03 18:22:46 thompsonbry Exp $
 */
public final class Provider
    implements RecordManagerProvider
{

    /**
     * Create a default implementation record manager.
     *
     * @param name Name of the record file.
     * @param options Record manager options.
     * @throws IOException if an I/O related exception occurs while creating
     *                    or opening the record manager.
     * @throws UnsupportedOperationException if some options are not supported by the
     *                                      implementation.
     * @throws IllegalArgumentException if some options are invalid.
     */
    public RecordManager createRecordManager( String name,
                                              Properties options )
        throws IOException
    {
        BaseRecordManager  baserecman; 
        RecordManager      recman; // returned to caller.
        String             value;

        value = options.getProperty( RecordManagerOptions.DUMP, "false" );
        if( value.equalsIgnoreCase( "TRUE") ) {
            recman = baserecman = new DumpUtility( name );
        } else {
            recman = baserecman = new BaseRecordManager( name );
        }

        // Some RecordFile options.
        value = options.getProperty( "jdbm.RecordFile.cleanMRUCapacity" );
        if( value != null ) {
            int cleanMRUCapacity = Integer.parseInt( value );
            baserecman._file.setCleanMRUCapacity( cleanMRUCapacity );
        }
        value = options.getProperty( "jdbm.RecordFile.freeListCapacity" );
        if( value != null ) {
            int freeListCapacity = Integer.parseInt( value );
            baserecman._file.setFreeListCapacity( freeListCapacity );
        }
        value = options.getProperty( "jdbm.RecordFile.counterDisplayRate" );
        if( value != null ) {
            int counterDisplayRate = Integer.parseInt( value );
            baserecman._file.setCounterDisplayRate( counterDisplayRate );
        }
        value = options.getProperty( RecordManagerOptions.DISABLE_TRANSACTIONS, "false" );
        if ( value.equalsIgnoreCase( "TRUE" ) ) {
            value = options.getProperty
                ( RecordManagerOptions.DISABLE_TRANSACTIONS_AUTOCOMMITINTERVAL,
                  RecordManagerOptions.DISABLE_TRANSACTIONS_AUTOCOMMITINTERVAL_DEFAULT
                  );
            int autoCommitInterval = Integer.parseInt( value );

            value = options.getProperty( RecordManagerOptions.DISABLE_TRANSACTIONS_PERFORMSYNCONCLOSE, "true" );
            boolean syncOnClose = value.equalsIgnoreCase("TRUE");
            baserecman.disableTransactions( autoCommitInterval, syncOnClose );
            
        }
        
        value = options.getProperty( RecordManagerOptions.CACHE_TYPE,
                                     RecordManagerOptions.NORMAL_CACHE );
        if ( value.equalsIgnoreCase( RecordManagerOptions.NORMAL_CACHE ) ) {
            // "normal" cache.
            value = options.getProperty( RecordManagerOptions.CACHE_SIZE, "1000" );
            int cacheSize = Integer.parseInt( value );
            MRU cache = new MRU( cacheSize );
            recman = new CacheRecordManager( recman, cache );
        } else if (value.equalsIgnoreCase(RecordManagerOptions.NO_CACHE)){
            // "no" cache.
        } else if ( value.equalsIgnoreCase( RecordManagerOptions.SOFT_REF_CACHE ) ) {
            // "soft" cache.
            //
            // Note: The default values for the "soft" cache are from SoftCache.java.
            // However I have seen drammatically improved performance with cacheSize
            // set to 500 or 1000.  This makes sense since it reduces the chance that
            // a record your application may need again will have fallen off of the 
            // internal MRU.
            //
            // L1 is the inner deterministic cache policy.
            boolean nativeLong = false;
            CachePolicy l1;
            value = options.getProperty( RecordManagerOptions.CACHE_SIZE, ""+SoftCache.L1_DEFAULT_CAPACITY );
            int cacheSizeL1 = Integer.parseInt( value );
            if( nativeLong ) {
                // Note: I see 1% improvement using MRUNativeLong vs MRU.  We might do better
                // if we remove the use of Long keys and used only the [long] primitive
                // in the CachePolicy interface.
                value = options.getProperty( RecordManagerOptions.CACHE_LOAD_FACTOR, ""+LongKeyOpenHashMap.DEFAULT_LOAD_FACTOR );
                double loadFactorL1 = Double.parseDouble( value );
                l1 = new MRUNativeLong( cacheSizeL1, loadFactorL1 );
            } else {
                value = options.getProperty( RecordManagerOptions.CACHE_LOAD_FACTOR, "0.75" );
                float loadFactorL1 = Float.parseFloat( value );
                l1 = new MRU( cacheSizeL1, loadFactorL1 );
            }
            // L2 is the soft reference cache.
            value = options.getProperty( RecordManagerOptions.L2_CACHE_SIZE, ""+SoftCache.L2_INITIAL_CAPACITY );
            int cacheSizeL2 = Integer.parseInt( value );
            value = options.getProperty( RecordManagerOptions.L2_CACHE_LOAD_FACTOR, ""+SoftCache.L2_DEFAULT_LOAD_FACTOR );
            float loadFactorL2 = Float.parseFloat( value );
            CachePolicy l2 = new SoftCache( cacheSizeL2, loadFactorL2, l1 ); // the soft cache.
            // Set the cache policy on the record manager.
            recman = new CacheRecordManager( recman, l2 ); 
        } else if ( value.equalsIgnoreCase( RecordManagerOptions.WEAK_REF_CACHE ) ) {
            // "weak" cache
            //
            // Note: The default values for the "soft" cache are from SoftCache.java.
            // However I have seen drammatically improved performance with cacheSize
            // set to 500 or 1000.  This makes sense since it reduces the chance that
            // a record your application may need again will have fallen off of the 
            // internal MRU.
            //
            // L1 is the inner deterministic cache policy.
            boolean nativeLong = false;
            CachePolicy l1;
            value = options.getProperty( RecordManagerOptions.CACHE_SIZE, ""+WeakCache.L1_DEFAULT_CAPACITY );
            int cacheSizeL1 = Integer.parseInt( value );
            if( nativeLong ) {
                // Note: I see 1% improvement using MRUNativeLong vs MRU.  We might do better
                // if we remove the use of Long keys and used only the [long] primitive
                // in the CachePolicy interface.
                value = options.getProperty( RecordManagerOptions.CACHE_LOAD_FACTOR, ""+LongKeyOpenHashMap.DEFAULT_LOAD_FACTOR );
                double loadFactorL1 = Double.parseDouble( value );
                l1 = new MRUNativeLong( cacheSizeL1, loadFactorL1 );
            } else {
                value = options.getProperty( RecordManagerOptions.CACHE_LOAD_FACTOR, "0.75" );
                float loadFactorL1 = Float.parseFloat( value );
                l1 = new MRU( cacheSizeL1, loadFactorL1 );
            }
            // L2 is the soft reference cache.
            value = options.getProperty( RecordManagerOptions.L2_CACHE_SIZE, ""+WeakCache.L2_INITIAL_CAPACITY );
            int cacheSizeL2 = Integer.parseInt( value );
            value = options.getProperty( RecordManagerOptions.L2_CACHE_LOAD_FACTOR, ""+WeakCache.L2_DEFAULT_LOAD_FACTOR );
            float loadFactorL2 = Float.parseFloat( value );
            CachePolicy l2 = new WeakCache( cacheSizeL2, loadFactorL2, l1 ); // the soft cache.
            // Set the cache policy on the record manager.
            recman = new CacheRecordManager( recman, l2 ); 
        } else if ( value.equalsIgnoreCase( "all" ) ) {
            /*
             * "all" items are retained in the cache. This is not intended for
             * general use. However it is useful for profiling jdbm since it
             * causes all serialization and allocation of physical rows to be
             * deferred until the commit. The cache is NOT reset at the commit,
             * so the size of the cache will only grow with use.
             */
            recman = new CacheRecordManager( recman, new CacheAll() );
        } else {
            throw new IllegalArgumentException( "Invalid cache type: " + value );
        }

        if( recman instanceof CacheRecordManager ) {
            value = options.getProperty( RecordManagerOptions.LAZY_INSERT,
                    RecordManagerOptions.LAZY_INSERT_DEFAULT );
            if( value.equalsIgnoreCase("true")) {
                ((CacheRecordManager)recman)._lazyInsert = true;
            }
        }

        // Setup the default serializer (this is a sticky option).
        value = getStickyOption
            ( recman,
              RecordManagerOptions.SERIALIZER,
              null
              );
        if( value == null ) {
            value = options.getProperty
            	( RecordManagerOptions.SERIALIZER,
                  RecordManagerOptions.SERIALIZER_DEFAULT
                  );
        } else {
        	String desiredValue = options.getProperty(
					RecordManagerOptions.SERIALIZER,
					RecordManagerOptions.SERIALIZER_DEFAULT);
			if (! value.equals(desiredValue)) {
				System.err.println("WARN: desired serializer=" + desiredValue
						+ ", but sticky option already set to: " + value);
			}
        }
        final String serializerValue = value; // remember.
        setStickyOption( recman, RecordManagerOptions.SERIALIZER, value );

        // Experimental support for global record compression.
        value = getStickyOption
            ( recman,
              RecordManagerOptions.COMPRESSOR,
              null
              );
        if( value == null ) {
            value = options.getProperty
            	( RecordManagerOptions.COMPRESSOR,
            	  RecordManagerOptions.COMPRESSOR_NONE
            	  );
        }
        IRecordCompressor compressor;
        if( RecordManagerOptions.COMPRESSOR_NONE.equals(value)) {
            compressor = new DefaultRecordCompressor();
        } else if( RecordManagerOptions.COMPRESSOR_BEST_SPEED.equals(value)) {
            compressor = new BestSpeedRecordCompressor();
        } else if( RecordManagerOptions.COMPRESSOR_BEST_COMPRESSION.equals(value)) {
            compressor = new BestCompressionRecordCompressor();
        } else {
            throw new IllegalArgumentException
               ( "option="+RecordManagerOptions.COMPRESSOR+", value="+value
                 );
        }
        // set as a sticky option.
        setStickyOption( recman, RecordManagerOptions.COMPRESSOR, value );
        
        // Set the compressor.  After this all records are compressed if a
        // compressor was selected.  This means that we must set the compressor
        // after we have setup all sticky properties and before we setup the
        // serializer (or anything else whose state can be updated later once
        // the compressor is in effect).  This means that compression would
        // interfer with the subsequent access to sticky properties, e.g., by the
        // application or another recman layer.  That could be handled by adding
        // a Compressor option to the recman API, just like the Serializer option.

        baserecman._compressor = compressor;

        // Set the serialization handler now that the compressor is in place.
        setupSerializationHandler( options, recman, serializerValue );

        /*
         * Optionally setup the page at a time record installer.
         */
        value = options.getProperty( RecordManagerOptions.BUFFERED_INSTALLS, "false");
        // @release comment out next line except for testing.
//		 value = "true";
		if (value.equalsIgnoreCase("true")) {
			int wasteMargin = Integer
					.parseInt(options
							.getProperty(
									RecordManagerOptions.BUFFERED_INSTALLS_WASTE_MARGIN,
									RecordManagerOptions.BUFFERED_INSTALLS_WASTE_MARGIN_DEFAULT));
			int wasteMargin2 = Integer
					.parseInt(options
							.getProperty(
									RecordManagerOptions.BUFFERED_INSTALLS_WASTE_MARGIN2,
									RecordManagerOptions.BUFFERED_INSTALLS_WASTE_MARGIN2_DEFAULT));
			baserecman._bufMgr = new BufferedRecordInstallManager(
					baserecman._file, baserecman._logMgr, baserecman._physMgr,
					baserecman._pageman, wasteMargin, wasteMargin2);
		}
		
        return recman;
    }

    private Properties _stickyOptions;
    private long _stickyOptionsId;

    /**
     * Initializes {@link #_stickyOptions}.  The sticky options are stored as a
     * root object.  If the root object does not exist, then it is created and
     * inserted into the store.
     * 
     * @param recman
     * 
     * @throws IOException
     */

    protected void initStickyOptions( RecordManager recman )
    	throws IOException
    {
        if( _stickyOptions == null ) {
            _stickyOptionsId = recman.getRoot
                ( BaseRecordManager.STICKY_OPTIONS_ROOT
                  );
            if( _stickyOptionsId == 0L ) {
                _stickyOptions = new Properties();
                _stickyOptionsId = recman.insert
                    ( _stickyOptions,
                      DefaultSerializer.INSTANCE
                      );
                recman.setRoot
                    ( BaseRecordManager.STICKY_OPTIONS_ROOT,
                      _stickyOptionsId
                      );
            } else {
                _stickyOptions = (Properties)recman.fetch
                    ( _stickyOptionsId,
                      DefaultSerializer.INSTANCE
                      );
            }
        }
    }

    /**
     * Sets a sticky option.  A sticky option may be set once and is thereafter immutable.
     * They are used for configuration properties for the store that must be remembered so
     * that you do not have to have the correct properties file on hand when you try to 
     * open an existing store.
     * 
     * @param recman The record manager.
     * @param name The name of the sticky option.
     * @param value The value for that sticky option (may not be null).
     * @exception IllegalStateException if the option was already set and has a
     * different value.
     * @exception IOException
     */
    
    protected void setStickyOption( RecordManager recman, String name, String value )
    	throws IOException
    {
        if( recman == null || name == null || name.length()==0 || value == null ) {
            throw new IllegalArgumentException();
        }
        initStickyOptions( recman );
        final String existingValue = _stickyOptions.getProperty( name );
        if( existingValue != null && ! existingValue.equals(value) ) {
            // The value of a sticky option may not be changed.
            throw new IllegalStateException
            	( "Sticky option already set: name="+name+", value="+existingValue
            	  );
        }
        if( ! value.equals( existingValue ) ) {
            // Update the sticky option.
            System.err.println( "stickyOption:: "+name+"="+value );
            _stickyOptions.setProperty( name, value );
            // Update the sticky options on the store.
            recman.update
                ( _stickyOptionsId,
                  _stickyOptions,
                  DefaultSerializer.INSTANCE
                  );
            // Force commit so that we can't loose sticky options.
            recman.commit();
        }
    }

    /**
     * If the named option has been set as a sticky option, then that value
     * is returned.  Otherwise the specified default value is returned.<p>
     * 
     * @param recman The record manager.
     * 
     * @param name The name of the option.
     * 
     * @param defval The default value for the option (optional).
     * 
     * @return The value of the option and <code>null</code> iff there is no
     * value for that sticky option and <i>defval == null</i>.
     * 
     * @throws IOException
     */
    
    protected String getStickyOption( RecordManager recman, String name, String defval )
    	throws IOException
    {
        initStickyOptions( recman );
        String value = _stickyOptions.getProperty( name, defval );
        return value;
    }
    
    /**
     * Check for existing serialization handler (it is a root object). If there
     * is an existing serialization handler, then we always recover it. (This is
     * a sticky option.) Otherwise we insert a serialization handler into the
     * store, mark it as a root object, and initialize it.
     * 
     * @param recman
     *            The record manager. This MUST be the outer record manager
     *            established by the Provider so that the serialization handler
     *            is configured to use the caching layer.
     * 
     * @param value
     *            The Class of the serialization handler. This is ignored if
     *            there is already a root object identifying the serialization
     *            handler for the store.
     * 
     * @see RecordManagerOptions#SERIALIZER
     * @see ExtensibleSerializer
     * @see ExtensibleSerializerSingleton
     */
    
    private void setupSerializationHandler( Properties properties, RecordManager recman, String value )
    	throws IOException
    {
        
        // @release comment out next line except for testing.
//        value = RecordManagerOptions.SERIALIZER_EXTENSIBLE;

        ISerializationHandler ser;
        
        if( value.equalsIgnoreCase(RecordManagerOptions.SERIALIZER_DEFAULT)) {
            
            // "default"
            
            ser = new DefaultSerializationHandler();
            
	} else if( value.equalsIgnoreCase(RecordManagerOptions.SERIALIZER_EXTENSIBLE)) {

	    // "extensible"
	    
	    long recid = recman.getRoot
	    	( BaseRecordManager.DEFAULT_SERIALIZER_ROOT
	    	  );
	    
	    ExtensibleSerializer extser;
	    
	    if( recid == 0L ) {
	
	        // Create a new ExtensibleSerializer (it is inserted into the store).
	        extser = ExtensibleSerializer.createInstance
	            ( recman
	              );
	        
	        // Save as root object.
	        recman.setRoot
	            ( BaseRecordManager.DEFAULT_SERIALIZER_ROOT,
		      extser.getRecid()
		      );
	        
	    } else {
	        
	        // Load an existing instance from the store.
	        extser = ExtensibleSerializer.load( recman, recid );
	        
	    }

	    // Optionally turn on the profiler for the extensible serialization support.
	    if (properties.getProperty(
                    RecordManagerOptions.PROFILE_SERIALIZATION, "false")
                    .equalsIgnoreCase("TRUE")) {
	     	extser.getProfiler().enable( true );
	    }

	    //
	    // Initilize the singleton that proxies for the extensible serializer.
	    //
		        
	    // Create a singleton (proxies for the extensible serializer).
	    ExtensibleSerializerSingleton proxy = new ExtensibleSerializerSingleton();

	    // Register the extensible serializer with the singleton.
	    proxy.setSerializer( recman, extser );
		        
	    // We use the singleton as the default serializer in order to avoid
	    // having multiple instances of the ExtensibleSerializer persisted
	    // in the store.

	    ser = proxy;

	} else {
	    
	    throw new IllegalArgumentException
	    	( "Invalid value: option="+RecordManagerOptions.SERIALIZER+", value="+value
	    	  );

	}

	// Set default serializer on recman.
	((BaseRecordManager)recman.getBaseRecordManager())._serializer = ser;
                
    }

}
