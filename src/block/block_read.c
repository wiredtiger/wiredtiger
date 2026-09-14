/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static void __fs_free_space_dump(WT_SESSION_IMPL *session, WT_BLOCK *block);
/*
 * __wt_bm_read --
 *     Map or read address cookie referenced block into a buffer.
 */
int
__wt_bm_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta,
  const uint8_t *addr, size_t addr_size)
{
    WT_BLOCK *block;
    WT_DECL_RET;
    wt_off_t offset;
    uint32_t checksum, objectid, size;
    bool last_release;

    WT_UNUSED(block_meta);

    block = bm->block;

    /* Crack the cookie. */
    WT_RET(__wt_block_addr_unpack(
      session, block, addr, addr_size, &objectid, &offset, &size, &checksum));

    if (bm->is_multi_handle)
        /* Lookup the block handle */
        WT_RET(__wt_blkcache_get_handle(session, bm, objectid, true, &block));

#ifdef HAVE_DIAGNOSTIC
    /*
     * In diagnostic mode, verify the block we're about to read isn't on the available list, or for
     * the writable objects, the discard list.
     */
    WT_ERR(__wti_block_misplaced(session, block, "read", offset, size,
      bm->is_live && block == bm->block, __PRETTY_FUNCTION__, __LINE__));
#endif

    /* Read the block. */
    WT_ERR(__wti_block_read_off(session, block, buf, objectid, offset, size, checksum));

    /* Optionally discard blocks from the system's buffer cache. */
    WT_ERR(__wti_block_discard(session, block, (size_t)size));

err:
    if (bm->is_multi_handle) {
        last_release = false;
        __wt_blkcache_release_handle(session, block, &last_release);
        if (last_release && __wt_block_eligible_for_sweep(bm, block))
            WT_TRET(__wt_bm_sweep_handles(session, bm));
    }

    return (ret);
}

/*
 * __wt_bm_corrupt --
 *     Report a block has been corrupted, external API.
 */
int
__wt_bm_corrupt(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    wt_off_t offset;
    uint32_t checksum, objectid, size;

    /* Read the block. */
    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(__wt_bm_read(bm, session, tmp, NULL, addr, addr_size));

    /* Crack the cookie, dump the block. */
    WT_ERR(__wt_block_addr_unpack(
      session, bm->block, addr, addr_size, &objectid, &offset, &size, &checksum));
    __wt_log_data_dump(session, tmp->data, tmp->size,
      "corrupt dump: {%" PRIu32 ": %" PRIuMAX ", %" PRIu32 ", %#" PRIx32 "}", objectid,
      (uintmax_t)offset, size, checksum);

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __block_header_report --
 *     Decode the headers of a block that failed its checksum into a diagnostic string. The size the
 *     block declares for itself is the discriminator: a block agreeing with the address cookie is
 *     the block we asked for with damaged bytes, while a block disagreeing with it is a
 *     structurally valid block of a different size sitting at that offset, which implicates the
 *     storage stack rather than the media and needs an entirely different remediation.
 */
static void
__block_header_report(
  const void *image, const WT_BLOCK_HEADER *blk, uint32_t size, char *dst, size_t dst_size)
{
    WT_PAGE_HEADER dsk;

    /*
     * The page header is only byte-swapped once a block passes its checksum, so decode a copy of
     * it. The block header is the caller's byte-swapped copy: the one in the image may already have
     * had its checksum cleared.
     */
    memcpy(&dsk, image, sizeof(dsk));
    __wt_page_header_byteswap(&dsk);

    /*
     * None of these fields is trusted enough to size or index anything with, so every one of them
     * is formatted as a fixed-width integer or a bounded string.
     */
    WT_IGNORE_RET(__wt_snprintf(dst, dst_size,
      "%s%s: block header disk_size %" PRIu32 ", requested size %" PRIu32 ", flags %#" PRIx8
      "; page header mem_size %" PRIu32 ", write_gen %" PRIu64 ", entries %" PRIu32 ", type %" PRIu8
      " (%s), flags %#" PRIx8 ", version %" PRIu8,
      blk->disk_size == size ? "HEADER_SIZE_MATCH" : "HEADER_SIZE_MISMATCH",
      F_ISSET(&dsk, WT_PAGE_COMPRESSED) && dsk.mem_size <= blk->disk_size ?
        " IMPLAUSIBLE_MEM_SIZE" :
        "",
      blk->disk_size, size, blk->flags, dsk.mem_size, dsk.write_gen, dsk.u.entries, dsk.type,
      __wt_page_type_str(dsk.type), dsk.flags, dsk.version));
}

/*
 * __block_checksum_bitflip_detect --
 *     Check whether the checksum stored in the block header differs from the expected checksum by
 *     exactly one bit. Scanning the block cannot answer this, because a scan assumes the stored
 *     checksum is the one the write path computed.
 */
static bool
__block_checksum_bitflip_detect(uint32_t stored, uint32_t expected, size_t *bit_position)
{
    size_t bit;
    uint32_t diff;

    diff = stored ^ expected;
    if (diff == 0 || (diff & (diff - 1)) != 0)
        return (false);

    for (bit = 0; (diff & (1U << bit)) == 0; ++bit)
        ;
    *bit_position = bit;
    return (true);
}

/*
 * __block_bitflip_detect --
 *     Check if flipping a single bit in the data would match the expected checksum. This helps
 *     diagnose single-bit memory corruption. Skip check for blocks larger than a defined size to
 *     avoid excessive CPU usage.
 */
static bool
__block_bitflip_detect(
  void *data, size_t check_size, uint32_t expected_checksum, size_t *bit_position)
{
    size_t byte_index, bit_index;
    uint8_t *bytes;

    if (check_size > WT_BITFLIP_MAX_SIZE)
        return (false);

    bytes = (uint8_t *)data;

    /* Try flipping each bit in the data. */
    for (byte_index = 0; byte_index < check_size; ++byte_index) {
        for (bit_index = 0; bit_index < 8; ++bit_index) {
            /* Flip the bit. */
            bytes[byte_index] ^= (1U << bit_index);

            /* Check if it matches the expected checksum. */
            if (__wt_checksum_match(data, check_size, expected_checksum)) {
                /* Found a single bit flip that would produce the expected checksum. */
                *bit_position = byte_index * 8 + bit_index;
                /* Flip the bit back before returning. */
                bytes[byte_index] ^= (1U << bit_index);
                return (true);
            }

            /* Flip the bit back. */
            bytes[byte_index] ^= (1U << bit_index);
        }
    }

    return (false);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __wt_block_read_off_blind --
 *     Read the block at an offset, return the size and checksum, debugging only.
 */
int
__wt_block_read_off_blind(
  WT_SESSION_IMPL *session, WT_BLOCK *block, wt_off_t offset, uint32_t *sizep, uint32_t *checksump)
{
    WT_BLOCK_HEADER *blk;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;

    *sizep = 0;
    *checksump = 0;

    /*
     * Make sure the buffer is large enough for the header and read the first allocation-size block.
     */
    WT_RET(__wt_scr_alloc(session, block->allocsize, &tmp));
    WT_ERR(__wt_read(session, block->fh, offset, (size_t)block->allocsize, tmp->mem));
    blk = WT_BLOCK_HEADER_REF(tmp->mem);
    __wt_block_header_byteswap(blk);

    *sizep = blk->disk_size;
    *checksump = blk->checksum;

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}
#endif

/*
 * __wti_block_read_off --
 *     Read an addr/size pair referenced block into a buffer.
 */
int
__wti_block_read_off(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf, uint32_t objectid,
  wt_off_t offset, uint32_t size, uint32_t checksum)
{
    WT_BLOCK_HEADER *blk, swap;
    size_t bit_position, bufsize, check_size;
    uint64_t time_start, time_stop;
    bool full_checksum_mismatch;

    time_start = __wt_clock(session);

    full_checksum_mismatch = false;
    bufsize = size;
    __wt_verbose_debug2(session, WT_VERB_READ,
      "off %" PRIuMAX ", size %" PRIu32 ", checksum %#" PRIx32, (uintmax_t)offset, size, checksum);

    WT_STAT_CONN_INCR(session, block_read);
    WT_STAT_CONN_INCRV(session, block_byte_read, size);

    /*
     * Ensure we don't read information that isn't there. It shouldn't ever happen, but it's a cheap
     * test.
     */
    if (size < block->allocsize)
        WT_RET_MSG(session, EINVAL,
          "%s: impossibly small block size of %" PRIu32 "B, less than allocation size of %" PRIu32,
          block->name, size, block->allocsize);

    WT_RET(__wt_buf_init(session, buf, bufsize));
    buf->size = size;

    __wt_capacity_throttle(session, size, WT_THROTTLE_READ);
    WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));

    /*
     * We incrementally read through the structure before doing a checksum, do little- to big-endian
     * handling early on, and then select from the original or swapped structure as needed.
     */
    blk = WT_BLOCK_HEADER_REF(buf->mem);
    __wt_block_header_byteswap_copy(blk, &swap);
    check_size = F_ISSET(&swap, WT_BLOCK_DATA_CKSUM) ? size : WT_BLOCK_COMPRESS_SKIP;
    if (swap.checksum == checksum) {
        /*
         * Set block header checksum to 0 to allow the checksum to be computed, as its calculation
         * includes the block header. Not clearing it would result in the checksum being
         * miscalculated. blk->checksum remains cleared, as it will not be revisited during a B-tree
         * traversal.
         */
        blk->checksum = 0;
        if (__wt_checksum_match(buf->mem, check_size, checksum)) {
            time_stop = __wt_clock(session);
            __wt_stat_msecs_hist_incr_bmread(session, WT_CLOCKDIFF_MS(time_stop, time_start));

            /*
             * Swap the page-header as needed; this doesn't belong here, but it's the best place to
             * catch all callers.
             */
            __wt_page_header_byteswap(buf->mem);
            return (0);
        }
        full_checksum_mismatch = true;
    }

    if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE)) {
        char header_report[512];

        if (full_checksum_mismatch)
            __wt_errx_id(session, 1538000,
              "%s: potential hardware corruption, read checksum error for %" PRIu32
              "B block at offset %" PRIuMAX ": calculated block checksum of %#" PRIx32
              " doesn't match expected checksum of %#" PRIx32,
              block->name, size, (uintmax_t)offset, __wt_checksum(buf->mem, check_size), checksum);
        else
            __wt_errx_id(session, 1538001,
              "%s: potential hardware corruption, read checksum error for %" PRIu32
              "B block at offset %" PRIuMAX ": block header checksum of %#" PRIx32
              " doesn't match expected checksum of %#" PRIx32,
              block->name, size, (uintmax_t)offset, swap.checksum, checksum);

        /*
         * Report what the block says about itself. The classification this gives us is only
         * available while the block is in hand: these events happen once and cannot be repeated,
         * and the raw dump below is often unusable because it can hold user data.
         */
        __block_header_report(buf->mem, &swap, size, header_report, sizeof(header_report));
        __wt_errx_id(session, 1843300,
          "%s: read checksum error diagnostic for %" PRIu32 "B block at offset %" PRIuMAX ": %s",
          block->name, size, (uintmax_t)offset, header_report);

        /*
         * Dump the corrupted block for analysis prior to bitflip detection in case detection takes
         * too long.
         */
        __wt_log_data_dump(session, buf->data, buf->size,
          "corrupt dump: {%" PRIu32 ": %" PRIuMAX ", %" PRIu32 ", %#" PRIx32 "}", objectid,
          (uintmax_t)offset, size, checksum);

        /* Dump the free disk space. */
        __fs_free_space_dump(session, block);

        /*
         * Attempt to detect single-bit flips. On the full mismatch branch the stored checksum
         * agreed with the cookie, so any flip has to be in the block itself; on the block header
         * branch the stored checksum is what disagreed, so the only single-bit explanation is a
         * flip in that field, which no scan of the block can find.
         */
        bit_position = 0;
        if (full_checksum_mismatch) {
            if (__block_bitflip_detect(buf->mem, check_size, checksum, &bit_position))
                __wt_errx(session,
                  "%s: single-bit flip detected at bit position %" WT_SIZET_FMT
                  " (byte %" WT_SIZET_FMT ", bit %" WT_SIZET_FMT
                  ") would produce the expected checksum",
                  block->name, bit_position, bit_position / 8, bit_position % 8);
            else
                __wt_errx(session, "%s: bitflip detection performed but no single-bit flip found",
                  block->name);
        } else if (__block_checksum_bitflip_detect(swap.checksum, checksum, &bit_position)) {
            __wt_errx(session,
              "%s: single-bit flip detected in the stored block header checksum at bit "
              "position %" WT_SIZET_FMT,
              block->name, bit_position);
        } else {
            __wt_errx(session,
              "%s: the stored block header checksum is not a single-bit flip of the expected "
              "checksum",
              block->name);
        }
    }

    /* Panic if a checksum fails during an ordinary read. */
    F_SET_ATOMIC_32(S2C(session), WT_CONN_DATA_CORRUPTION);

    if (block->verify || WT_SESSION_READ_CORRUPT_OK(session))
        return (WT_ERROR);

    __wti_block_extlist_dump_all(session, block);

    WT_RET_PANIC(session, WT_ERROR, "%s: fatal read error", block->name);
}

/*
 * __fs_free_space_dump --
 *     Dump the free disk space on the main database directory and on the journal directory for both
 *     full or partial checksum mismatch.
 */
static void
__fs_free_space_dump(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *fs;
    WT_LOG_MANAGER *log_mgr;
    wt_off_t db_dir_free_space, journal_dir_free_space;
    const char *db_dir, *journal_dir;

    db_dir_free_space = journal_dir_free_space = 0;
    db_dir = S2C(session)->home;
    journal_dir = NULL;
    log_mgr = &S2C(session)->log_mgr;
    fs = __wt_fs_file_system(session);

    if (log_mgr->log_path != NULL && strlen(log_mgr->log_path) > 0)
        /*
         * If the journal directory is not the same as the main database directory path, set it.
         */
        journal_dir = !WT_STREQ(db_dir, log_mgr->log_path) ? log_mgr->log_path : NULL;

    /* Log free space on the main database directory, and the journal directory if different. */
    ret = fs->fs_free_space(fs, (WT_SESSION *)session, db_dir, &db_dir_free_space);
    /*
     * Using __wt_errx here is intentional: we're already in a corruption path (checksum mismatch).
     * We're being consistent with other corruption logs so free-space details are always recorded.
     */
    if (ret == 0) {
        __wt_errx(session,
          "%s: free disk space on main database directory (%s) is %" PRIdMAX " bytes", block->name,
          db_dir, (intmax_t)db_dir_free_space);
    } else
        __wt_err(session, ret,
          "%s: unable to determine free disk space on main database directory (%s)", block->name,
          db_dir);

    if (journal_dir != NULL) {
        ret = fs->fs_free_space(fs, (WT_SESSION *)session, journal_dir, &journal_dir_free_space);
        if (ret == 0) {
            __wt_errx(session,
              "%s: free disk space on journal directory (%s) is %" PRIdMAX " bytes", block->name,
              journal_dir, (intmax_t)journal_dir_free_space);
        } else
            __wt_err(session, ret,
              "%s: unable to determine free disk space on journal directory (%s)", block->name,
              journal_dir);
    }
}

#ifdef HAVE_UNITTEST
bool
__ut_block_bitflip_detect(
  void *data, size_t check_size, uint32_t expected_checksum, size_t *bit_position)
{
    return (__block_bitflip_detect(data, check_size, expected_checksum, bit_position));
}

void
__ut_block_header_report(
  const void *image, const WT_BLOCK_HEADER *blk, uint32_t size, char *dst, size_t dst_size)
{
    __block_header_report(image, blk, size, dst, dst_size);
}

bool
__ut_block_checksum_bitflip_detect(uint32_t stored, uint32_t expected, size_t *bit_position)
{
    return (__block_checksum_bitflip_detect(stored, expected, bit_position));
}
#endif
