use std::collections::{HashMap, LinkedList};

use super::buffer_pool_manager::FrameId;

#[derive(Debug)]
pub struct LRUKNode {
    /// History of last seen K timestamps of this page. Least recent timestamp
    /// stored in front.
    // Remove maybe_unused if you start using them. Feel free to change the member variables as you
    // want.
    history: LinkedList<u64>,
    k: usize,
    frame_id: FrameId,
    is_evictable: bool,
}

/// LRUKReplacer implements the LRU-k replacement policy.
///
/// The LRU-k algorithm evicts a frame whose backward k-distance is maximum
/// of all frames. Backward k-distance is computed as the difference in time
/// between current timestamp and the timestamp of kth previous access.
///
/// A frame with less than k historical references is given
/// +inf as its backward k-distance. When multiple frames have +inf backward
/// k-distance, classical LRU algorithm is used to choose victim.
#[derive(Debug)]
pub struct LRUKReplacer {
    // TODO(student): implement me! You can replace these member variables as you like.
    // Remove maybe_unused if you start using them.
    node_store: HashMap<FrameId, LRUKNode>,
    current_timestamp: u64,
    current_size: usize,
    replacer_size: usize,
    k: usize,
}

impl LRUKReplacer {
    /// TODO(P1): Add implementation
    ///
    /// @brief a new LRUKReplacer.
    /// @param num_frames the maximum number of frames the LRUReplacer will be
    /// required to store
    pub fn new(num_frames: usize, k: usize) -> Self {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Find the frame with largest backward k-distance and evict that
    /// frame. Only frames that are marked as 'evictable' are candidates for
    /// eviction.
    ///
    /// A frame with less than k historical references is given +inf as its
    /// backward k-distance. If multiple frames have inf backward
    /// k-distance, then evict frame with earliest timestamp based on LRU.
    ///
    /// Successful eviction of a frame should decrement the size of replacer and
    /// remove the frame's access history.
    ///
    /// @param[out] frame_id id of frame that is evicted.
    /// @return true if a frame is evicted successfully, false if no frames can
    /// be evicted.
    pub fn evict(&mut self) -> Option<FrameId> {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Record the event that the given frame id is accessed at current
    /// timestamp. Create a new entry for access history if frame id has not
    /// been seen before.
    ///
    /// If frame id is invalid (ie. larger than replacer_size_), throw an
    /// exception. You can also use BUSTUB_ASSERT to abort the process if
    /// frame id is invalid.
    ///
    /// @param frame_id id of frame that received a new access.
    /// @param access_type type of access that was received. This parameter is
    /// only needed for leaderboard tests.
    pub fn record_access(&mut self, frame_id: FrameId) {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Toggle whether a frame is evictable or non-evictable. This
    /// function also controls replacer's size. Note that size is equal to
    /// number of evictable entries.
    ///
    /// If a frame was previously evictable and is to be set to non-evictable,
    /// then size should decrement. If a frame was previously non-evictable
    /// and is to be set to evictable, then size should increment.
    ///
    /// If frame id is invalid, throw an exception or abort the process.
    ///
    /// For other scenarios, this function should terminate without modifying
    /// anything.
    ///
    /// @param frame_id id of frame whose 'evictable' status will be modified
    /// @param set_evictable whether the given frame is evictable or not
    pub fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Remove an evictable frame from replacer, along with its access
    /// history. This function should also decrement replacer's size if
    /// removal is successful.
    ///
    /// Note that this is different from evicting a frame, which always remove
    /// the frame with largest backward k-distance. This function removes
    /// specified frame id, no matter what its backward k-distance is.
    ///
    /// If Remove is called on a non-evictable frame, throw an exception or
    /// abort the process.
    ///
    /// If specified frame is not found, directly return from this function.
    ///
    /// @param frame_id id of frame to be removed
    pub fn remove(&mut self, frame_id: FrameId) {
        unimplemented!()
    }

    /// TODO(P1): Add implementation
    ///
    /// @brief Return replacer's size, which tracks the number of evictable
    /// frames.
    ///
    /// @return size_t
    pub fn size(&self) -> usize {
        unimplemented!()
    }
}

mod tests {
    use super::LRUKReplacer;

    #[test]
    pub fn test_lru_k_cmu_sample() {
        let mut lru_replacer = LRUKReplacer::new(7, 2);

        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is
        // non-evictable.
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.set_evictable(1, true);
        lru_replacer.set_evictable(2, true);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        lru_replacer.set_evictable(5, true);
        lru_replacer.set_evictable(6, false);
        assert_eq!(5, lru_replacer.size());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access
        // histories. All other frames have max backward k-dist. The order of
        // eviction is [2,3,4,5,1].
        lru_replacer.record_access(1);

        // Scenario: Evict three pages from the replacer. Elements with max k-distance
        // should be popped first based on LRU.
        let value = lru_replacer.evict();
        assert_eq!(Some(2), value);
        let value = lru_replacer.evict();
        assert_eq!(Some(3), value);
        let value = lru_replacer.evict();
        assert_eq!(Some(4), value);
        assert_eq!(lru_replacer.size(), 2);

        // Scenario: Now replacer has frames [5,1]. Insert new frames 3, 4, and update
        // access history for 5. We should end with [3,1,5,4]
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        assert_eq!(4, lru_replacer.size());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        let value = lru_replacer.evict();
        assert_eq!(Some(3), value);
        assert_eq!(3, lru_replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward
        // k-dist.
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.size());
        let value = lru_replacer.evict();
        assert_eq!(Some(6), value);
        assert_eq!(3, lru_replacer.size());

        // Now we have [1,5,4]. Continue looking for victims.
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.size());
        let value = lru_replacer.evict();
        assert_eq!(Some(5), value);
        assert_eq!(1, lru_replacer.size());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.size());
        let value = lru_replacer.evict();
        assert_eq!(Some(4), value);

        assert_eq!(1, lru_replacer.size());
        let value = lru_replacer.evict();
        assert_eq!(Some(1), value);
        assert_eq!(0, lru_replacer.size());

        // This operation should not modify size
        assert_eq!(None, lru_replacer.evict());
        assert_eq!(0, lru_replacer.size());
    }
}
