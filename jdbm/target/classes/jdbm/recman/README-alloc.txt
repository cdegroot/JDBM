This document describes the memory allocation structures and
algorithms used by jdbm.  It is based on a thread in the
jdbm-developers mailing list.

- A block is a fixed length of bytes.  Also known as a page.
- A row is a variable length of bytes.  Also known as a record.
- A slot is a fixed length entry in a given block/page.
- A page list is a linked list of pages.  The head and tail of each
  page list is maintained in the file header.

Jdbm knows about a few page lists which are pre-defined in Magic,
e.g., Magic.USED_PAGE.  The FREE, USED, TRANSLATION, FREELOGIDS, and
FREEPHYSIDS page lists are used by the jdbm memory allocation policy
and are described below.

The translation list consists of a bunch of slots that can be
available (free) or unavailable (allocated).  If a slot is available,
then it contains junk data (it is available to map the logical row id
associated with that slot to some physical row id).  If it is
unavailable, then it contains the block id and offset of the header of
a valid (non-deleted) record.  "Available" for the translation list
is marked by a zero block id for that slot.

The free logical row id list consists of a set of pages that contain
slots.  Each slot is either available (free) or unavailable
(allocated).  If it is unavailable, then it contains a reference to
the location of the available slot in the translation list.  If it is
available, then it contains junk data. "Available" slots are marked by
a zero block id.  A count is maintained of the #of available slots
(free row ids) on the page.

As you free a logical row id, you change it's slot in the translation
list from unavailable to available, and then *add* entries to the free
logical row list.  Adding entries to the free logical row list is done
by finding an available slot in the free logical row list and
replacing the junk data in that slot with the location of the now
available slot in the translation list.  A count is maintained of the
#of available slots (free row ids) on the page.
 
Whew... now we've freed a logical row id.  But what about the physical
row id?
 
Well, the free physical row id list consists of a set of pages that
contain slots.  Each slot is either available (free) or unavailable
(allocated).  If it is unavailable, then it contains a reference to
the location of the newly freed row's header in the data page.  If it
is available, then it contains junk data.  "Available" slots are
marked by a zero block id.  A count is maintained of the #of available
slots (free row ids) on the page. (Sound familiar?)
 
As you free a physical row id, you change it's header in the data page
from inuse to free (by zeroing the size field of the record header),
and then *add* an entry to the free physical row list.  Adding entries
to the free physical row list consists of finding an available slot,
and replacing the junk data in that slot with the location of the
newly freed row's header in the data page.

The translation list is used for translating in-use logical row ids
to in-use physical row ids.  When a physical row id is freed, it is
removed from the translation list and added to the free physical row
id list.
 
This allows a complete decoupling of the logical row id from the
physical row id, which makes it super easy to do some of the fiddling
I'm talking about the coallescing and splitting records.
 
If you want to get a list of the free records, just enumerate the
unavailable entries in the free physical row id list.  You don't even
need to look up the record header because the length of the record is
also stored in the free physical row id list.  As you enumerate the
list, be sure to not include slots that are available (in the current
incarnation of jdbm, I believe the available length is set to 0 to
indicate available - we'll be changing that some time soon here, I'm
sure).
