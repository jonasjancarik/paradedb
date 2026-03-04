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

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};

/// A memory pool that returns errors when the memory limit is exceeded.
///
/// This is used to enforce `work_mem` limits in `JoinScan` and prevent
/// DataFusion from attempting to spill to disk (which is not yet implemented safely).
///
/// Important: this pool must NOT panic on OOM. A panic during async DataFusion
/// execution partially unwinds futures, leaving the execution plan tree in an
/// inconsistent state. When PostgreSQL later drops `JoinScanState` during memory
/// context cleanup, the Drop of the partially-freed plan triggers a double-free
/// crash. Instead, `try_grow` returns `ResourcesExhausted`, which DataFusion
/// propagates cleanly through the stream as `Some(Err(...))`.
///
/// TODO: Instead of erroring, implement a `MemoryPool` that integrates with PostgreSQL's
/// temporary file management (BufFile/VFD) to allow DataFusion to spill to disk when
/// `work_mem` is exceeded.
#[derive(Debug)]
pub struct PanicOnOOMMemoryPool {
    pool: datafusion::execution::memory_pool::GreedyMemoryPool,
}

impl PanicOnOOMMemoryPool {
    pub fn new(limit: usize) -> Self {
        Self {
            pool: datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
        }
    }
}

impl MemoryPool for PanicOnOOMMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // grow() is for unconditional bookkeeping (e.g. after try_grow succeeded).
        // Don't check limits here — GreedyMemoryPool::grow() doesn't either.
        self.pool.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, returned: usize) {
        self.pool.shrink(reservation, returned);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        // Delegate to the inner GreedyMemoryPool which checks the limit and
        // returns Err(ResourcesExhausted) when exceeded.
        self.pool.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.pool.reserved()
    }
}
