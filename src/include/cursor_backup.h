/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_backup and related definitions. */

struct __wt_cursor_backup {
    WT_CURSOR iface;

    size_t next;     /* Cursor position */
    WT_FSTREAM *bfs; /* Backup file stream */

#define WT_CURSOR_BACKUP_ID(cursor) (((WT_CURSOR_BACKUP *)(cursor))->maxid)
    uint32_t maxid; /* Maximum log file ID seen */

    char **list; /* List of files to be copied. */
    size_t list_allocated;
    size_t list_next;

    /* File offset-based incremental backup. */
    WT_BLKINCR *incr_src; /* Incremental backup source */
    char *incr_file;      /* File name */

    WT_CURSOR *incr_cursor; /* File cursor */

    WT_ITEM bitstring;    /* List of modified blocks */
    uint64_t nbits;       /* Number of bits in bitstring */
    uint64_t offset;      /* Zero bit offset in bitstring */
    uint64_t bit_offset;  /* Current offset */
    uint64_t granularity; /* Length, transfer size */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURBACKUP_CKPT_FAKE 0x0001u   /* Object has fake checkpoint */
#define WT_CURBACKUP_COMPRESSED 0x0002u  /* Object uses compression */
#define WT_CURBACKUP_CONSOLIDATE 0x0004u /* Consolidate returned info on this object */
#define WT_CURBACKUP_DUP 0x0008u         /* Duplicated backup cursor */
#define WT_CURBACKUP_EXPORT 0x0010u      /* Special backup cursor for export operation */
#define WT_CURBACKUP_FORCE_FULL 0x0020u  /* Force full file copy for this cursor */
#define WT_CURBACKUP_FORCE_STOP 0x0040u  /* Force stop incremental backup */
#define WT_CURBACKUP_HAS_CB_INFO 0x0080u /* Object has checkpoint backup info */
#define WT_CURBACKUP_INCR 0x0100u        /* Incremental backup cursor */
#define WT_CURBACKUP_INCR_INIT 0x0200u   /* Cursor traversal initialized */
#define WT_CURBACKUP_LOCKER 0x0400u      /* Hot-backup started */
#define WT_CURBACKUP_QUERYID 0x0800u     /* Backup cursor for incremental ids */
#define WT_CURBACKUP_RENAME 0x1000u      /* Object had a rename */
                                         /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
    uint32_t flags;
};
