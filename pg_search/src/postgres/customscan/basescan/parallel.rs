// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::cell::{Cell, RefCell};
use std::os::raw::c_void;

use crate::postgres::customscan::basescan::BaseScan;
use crate::postgres::customscan::builders::custom_state::CustomScanStateWrapper;
use crate::postgres::customscan::dsm::ParallelQueryCapable;
use crate::postgres::{ParallelScanState, PartitionEarlyTermState};

use pgrx::pg_sys::{self, shm_toc, ParallelContext, Size};

extern "C" {
    fn get_partition_parent(partition_oid: pg_sys::Oid, even_if_detached: bool) -> pg_sys::Oid;
}

thread_local! {
    /// The pcxt pointer for the current plan cycle. Used to detect stale state from prior queries.
    static EARLY_TERM_PCXT: Cell<usize> = const { Cell::new(0) };
    /// DSM offset to PartitionEarlyTermState, set by the first partition child during init.
    static EARLY_TERM_INFO: RefCell<Option<usize>> = const { RefCell::new(None) };
    /// Whether estimate_dsm has already added space for PartitionEarlyTermState in this cycle.
    static EARLY_TERM_ESTIMATED: Cell<bool> = const { Cell::new(false) };
}

/// Reset thread-local early termination tracking if we're in a new parallel context cycle.
fn maybe_reset_early_term_locals(pcxt: *mut ParallelContext) {
    let pcxt_val = pcxt as usize;
    if EARLY_TERM_PCXT.get() != pcxt_val {
        EARLY_TERM_PCXT.set(pcxt_val);
        EARLY_TERM_ESTIMATED.set(false);
        EARLY_TERM_INFO.with(|info| *info.borrow_mut() = None);
    }
}

/// Compute the sort rank of a partition child within its parent's partition list.
/// For ascending sorts, rank 0 = lowest-valued partition (first in oids list).
/// For descending sorts, rank 0 = highest-valued partition (last in oids list).
unsafe fn compute_partition_rank(child_oid: pg_sys::Oid, is_desc: bool) -> Option<usize> {
    let parent_oid = get_partition_parent(child_oid, false);
    if parent_oid == pg_sys::InvalidOid {
        return None;
    }
    let parent_rel = pg_sys::relation_open(parent_oid, pg_sys::AccessShareLock as _);
    let pdesc = pg_sys::RelationGetPartitionDesc(parent_rel, true);
    if pdesc.is_null() {
        pg_sys::relation_close(parent_rel, pg_sys::AccessShareLock as _);
        return None;
    }
    let nparts = (*pdesc).nparts as usize;
    let oids = std::slice::from_raw_parts((*pdesc).oids, nparts);
    let asc_rank = oids.iter().position(|&oid| oid == child_oid);
    pg_sys::relation_close(parent_rel, pg_sys::AccessShareLock as _);

    asc_rank.map(|rank| if is_desc { nparts - 1 - rank } else { rank })
}

/// Get the number of partitions for a parent relation given a child OID.
unsafe fn get_n_partitions(child_oid: pg_sys::Oid) -> u32 {
    let parent_oid = get_partition_parent(child_oid, false);
    if parent_oid == pg_sys::InvalidOid {
        return 0;
    }
    let parent_rel = pg_sys::relation_open(parent_oid, pg_sys::AccessShareLock as _);
    let pdesc = pg_sys::RelationGetPartitionDesc(parent_rel, true);
    let n = if pdesc.is_null() {
        0
    } else {
        (*pdesc).nparts as u32
    };
    pg_sys::relation_close(parent_rel, pg_sys::AccessShareLock as _);
    n
}

impl ParallelQueryCapable for BaseScan {
    fn estimate_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut ParallelContext,
    ) -> Size {
        if state.custom_state().search_reader.is_none() {
            BaseScan::init_search_reader(state);
        }

        let args = state.custom_state().parallel_scan_args();
        let mut size = ParallelScanState::size_of(
            args.segment_readers.len(),
            &args.query,
            args.with_aggregates,
        );

        // If this is the first eligible partition child in this plan cycle,
        // add space for the shared PartitionEarlyTermState.
        if state.custom_state().partition_early_term_eligible {
            maybe_reset_early_term_locals(pcxt);
            if !EARLY_TERM_ESTIMATED.get() {
                EARLY_TERM_ESTIMATED.set(true);
                size += PartitionEarlyTermState::size_of();
            }
        }

        size
    }

    fn initialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ) {
        let args = state.custom_state().parallel_scan_args();
        let pss_size = ParallelScanState::size_of(
            args.segment_readers.len(),
            &args.query,
            args.with_aggregates,
        );

        unsafe {
            let pscan_state = coordinate.cast::<ParallelScanState>();
            assert!(!pscan_state.is_null(), "coordinate is null");
            (*pscan_state).create_and_populate(args);
            state.custom_state_mut().parallel_state = Some(pscan_state);

            if state.custom_state().partition_early_term_eligible {
                maybe_reset_early_term_locals(pcxt);
                let existing_offset = EARLY_TERM_INFO.with(|info| *info.borrow());

                if let Some(et_offset) = existing_offset {
                    // Subsequent partition child: reuse existing PartitionEarlyTermState
                    (*pscan_state).early_term_offset = et_offset;

                    let seg = (*pcxt).seg;
                    let seg_base = pg_sys::dsm_segment_address(seg) as usize;
                    let et_ptr = (seg_base + et_offset) as *mut PartitionEarlyTermState;
                    state.custom_state_mut().early_term_state = Some(et_ptr);
                } else {
                    // First partition child: allocate and initialize PartitionEarlyTermState
                    // at the end of this child's coordinate area.
                    let et_ptr =
                        (coordinate as *mut u8).add(pss_size) as *mut PartitionEarlyTermState;

                    let child_oid = state.custom_state().heaprelid;
                    let n_partitions = get_n_partitions(child_oid);
                    let limit = state.custom_state().limit().unwrap_or(0) as u32;
                    (*et_ptr).init(limit, n_partitions);

                    // Compute the DSM offset from the segment base
                    let seg = (*pcxt).seg;
                    let seg_base = pg_sys::dsm_segment_address(seg) as usize;
                    let et_offset = et_ptr as usize - seg_base;

                    EARLY_TERM_INFO.with(|info| *info.borrow_mut() = Some(et_offset));
                    (*pscan_state).early_term_offset = et_offset;

                    state.custom_state_mut().early_term_state = Some(et_ptr);
                }

                // Compute partition sort rank
                let child_oid = state.custom_state().heaprelid;
                let is_desc = state.custom_state().partition_sort_desc;
                state.custom_state_mut().partition_sort_rank =
                    compute_partition_rank(child_oid, is_desc);
            }
        }
    }

    fn reinitialize_dsm_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        _pcxt: *mut ParallelContext,
        coordinate: *mut c_void,
    ) {
        let pscan_state = coordinate.cast::<ParallelScanState>();
        assert!(!pscan_state.is_null(), "coordinate is null");
        unsafe {
            (*pscan_state).reset();

            // Reset early termination counters for rescan
            if let Some(et_state) = state.custom_state().early_term_state {
                (*et_state).reset();
            }
        }
    }

    fn initialize_worker_custom_scan(
        state: &mut CustomScanStateWrapper<Self>,
        toc: *mut shm_toc,
        coordinate: *mut c_void,
    ) {
        let pscan_state = coordinate.cast::<ParallelScanState>();
        assert!(!pscan_state.is_null(), "coordinate is null");

        state.custom_state_mut().parallel_state = Some(pscan_state);
        unsafe {
            match (*pscan_state)
                .query()
                .expect("should be able to deserialize the query from the ParallelScanState")
            {
                Some(query) => state.custom_state_mut().set_base_search_query_input(query),
                None => panic!("no query in ParallelScanState"),
            }

            // Reconstruct early termination state pointer from DSM offset.
            // toc is at the base of the DSM segment, so toc + offset = ET state.
            let et_offset = (*pscan_state).early_term_offset;
            if et_offset != 0 {
                let et_ptr = (toc as *mut u8).add(et_offset) as *mut PartitionEarlyTermState;
                state.custom_state_mut().early_term_state = Some(et_ptr);

                // Compute partition sort rank
                let child_oid = state.custom_state().heaprelid;
                let is_desc = state.custom_state().partition_sort_desc;
                state.custom_state_mut().partition_sort_rank =
                    compute_partition_rank(child_oid, is_desc);
            }
        }
    }
}
