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

use std::os::raw::c_void;

use crate::parallel_worker::{estimate_chunk, estimate_keys};
use crate::postgres::customscan::basescan::BaseScan;
use crate::postgres::customscan::builders::custom_state::CustomScanStateWrapper;
use crate::postgres::customscan::dsm::ParallelQueryCapable;
use crate::postgres::{ParallelScanState, PartitionEarlyTermState};

use pgrx::pg_sys::{self, shm_toc, ParallelContext, Size};

extern "C" {
    fn get_partition_parent(partition_oid: pg_sys::Oid, even_if_detached: bool) -> pg_sys::Oid;
}

/// shm_toc key for the shared PartitionEarlyTermState.
/// Chosen to avoid collisions with PostgreSQL internal keys (0xE0...) and
/// our parallel_worker keys (1..3).
const EARLY_TERM_TOC_KEY: u64 = 0xB250_0000_0000_0001;

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
        let size = ParallelScanState::size_of(
            args.segment_readers.len(),
            &args.query,
            args.with_aggregates,
        );

        // Each eligible partition child adds a TOC estimate for the shared
        // PartitionEarlyTermState. Over-estimation is acceptable per the PG API;
        // only one child will actually allocate during initialize_dsm.
        if state.custom_state().partition_early_term_eligible {
            unsafe {
                estimate_keys(pcxt, 1);
                estimate_chunk(pcxt, PartitionEarlyTermState::size_of());
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

        unsafe {
            let pscan_state = coordinate.cast::<ParallelScanState>();
            assert!(!pscan_state.is_null(), "coordinate is null");
            (*pscan_state).create_and_populate(args);
            state.custom_state_mut().parallel_state = Some(pscan_state);

            if state.custom_state().partition_early_term_eligible {
                let toc = (*pcxt).toc;

                // Check if another partition child already allocated the shared state.
                let existing = pg_sys::shm_toc_lookup(toc, EARLY_TERM_TOC_KEY, true);

                if existing.is_null() {
                    // First partition child: allocate and initialize.
                    let et_ptr = pg_sys::shm_toc_allocate(toc, PartitionEarlyTermState::size_of())
                        as *mut PartitionEarlyTermState;

                    let child_oid = state.custom_state().heaprelid;
                    let n_partitions = get_n_partitions(child_oid);
                    let limit = state.custom_state().limit().unwrap_or(0) as u32;
                    (*et_ptr).init(limit, n_partitions);

                    pg_sys::shm_toc_insert(toc, EARLY_TERM_TOC_KEY, et_ptr as *mut c_void);

                    state.custom_state_mut().early_term_state = Some(et_ptr);
                } else {
                    // Subsequent partition child: reuse existing.
                    state.custom_state_mut().early_term_state =
                        Some(existing as *mut PartitionEarlyTermState);
                }

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

            // Look up the shared early termination state via TOC key.
            let et_ptr = pg_sys::shm_toc_lookup(toc, EARLY_TERM_TOC_KEY, true);
            if !et_ptr.is_null() {
                state.custom_state_mut().early_term_state =
                    Some(et_ptr as *mut PartitionEarlyTermState);

                let child_oid = state.custom_state().heaprelid;
                let is_desc = state.custom_state().partition_sort_desc;
                state.custom_state_mut().partition_sort_rank =
                    compute_partition_rank(child_oid, is_desc);
            }
        }
    }
}
