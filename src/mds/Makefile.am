libmds_types_la_SOURCES = \
	mds/events.cc \
	mds/locks.c \
	mds/mdstypes.cc \
	mds/snap.cc \
	mds/Capability.cc \
	mds/InoTable.cc \
	mds/LogEvent.cc \
	mds/MDSAuthCaps.cc \
	mds/Session.cc
noinst_LTLIBRARIES += libmds_types.la

libmds_la_SOURCES = \
	mds/journal.cc \
	mds/Beacon.cc \
	mds/CDentry.cc \
	mds/CDir.cc \
	mds/CInode.cc \
	mds/JournalPointer.cc \
	mds/Locker.cc \
	mds/Migrator.cc \
	mds/MDBalancer.cc \
	mds/MDCache.cc \
	mds/MDLog.cc \
	mds/MDS.cc \
	mds/MDSContext.cc \
	mds/MDSTable.cc \
	mds/MDSTableClient.cc \
	mds/MDSTableServer.cc \
	mds/Mutation.cc \
	mds/RecoveryQueue.cc \
	mds/Server.cc \
	mds/SessionMap.cc \
	mds/SnapRealm.cc \
	mds/SnapServer.cc \
	common/TrackedOp.cc
libmds_la_LIBADD = $(LIBOSDC) $(LIBMDS_TYPES)
noinst_LTLIBRARIES += libmds.la

noinst_HEADERS += \
	mds/inode_backtrace.h \
	mds/flock.h \
	mds/locks.c \
	mds/locks.h \
	mds/CDentry.h \
	mds/CDir.h \
	mds/CInode.h \
	mds/Capability.h \
	mds/InoTable.h \
	mds/JournalPointer.h \
	mds/LocalLock.h \
	mds/Locker.h \
	mds/LogEvent.h \
	mds/LogSegment.h \
	mds/MDBalancer.h \
	mds/MDCache.h \
	mds/RecoveryQueue.h \
	mds/MDLog.h \
	mds/MDS.h \
	mds/Beacon.h \
	mds/MDSContext.h \
	mds/MDSAuthCaps.h \
	mds/MDSMap.h \
	mds/MDSTable.h \
	mds/MDSTableServer.h \
	mds/MDSTableClient.h \
	mds/Mutation.h \
	mds/Migrator.h \
	mds/ScatterLock.h \
	mds/Server.h \
	mds/Session.h \
	mds/SessionMap.h \
	mds/SimpleLock.h \
	mds/SnapClient.h \
	mds/SnapRealm.h \
	mds/SnapServer.h \
	mds/inode_backtrace.h \
	mds/mds_table_types.h \
	mds/mdstypes.h \
	mds/snap.h \
	mds/MDSContinuation.h

noinst_HEADERS += \
	mds/events/ECommitted.h \
	mds/events/EExport.h \
	mds/events/EFragment.h \
	mds/events/EImportFinish.h \
	mds/events/EImportStart.h \
	mds/events/EMetaBlob.h \
	mds/events/ENoOp.h \
	mds/events/EOpen.h \
	mds/events/EResetJournal.h \
	mds/events/ESession.h \
	mds/events/ESessions.h \
	mds/events/ESlaveUpdate.h \
	mds/events/ESubtreeMap.h \
	mds/events/ETableClient.h \
	mds/events/ETableServer.h \
	mds/events/EUpdate.h


