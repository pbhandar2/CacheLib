/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/memory/Malloc.h>
#include <folly/stats/QuantileHistogram.h>

#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {
template <typename AllocatorT>
class ObjectCache;

// Dynamically adjust the entriesLimit to limit the cache size for object-cache.
template <typename AllocatorT>
class ObjectCacheSizeController : public PeriodicWorker {
 public:
  using ObjectCache = ObjectCache<AllocatorT>;
  explicit ObjectCacheSizeController(
      ObjectCache& objCache, const util::Throttler::Config& throttlerConfig);
  size_t getCurrentEntriesLimit() const {
    return currentEntriesLimit_.load(std::memory_order_relaxed);
  }

  void getCounters(const util::CounterVisitor& visitor) const;

 private:
  void work() override final;

  void shrinkCacheByEntriesNum(size_t entries);
  void expandCacheByEntriesNum(size_t entries);

  std::pair<size_t, size_t> trackJemallocMemStats() const {
    size_t jemallocAllocatedBytes;
    size_t jemallocActiveBytes;
    size_t epoch = 1;
    size_t sz;
    sz = sizeof(size_t);
    mallctl("epoch", nullptr, nullptr, &epoch, sizeof(epoch));
    mallctl("stats.allocated", &jemallocAllocatedBytes, &sz, nullptr, 0);
    mallctl("stats.active", &jemallocActiveBytes, &sz, nullptr, 0);
    return {jemallocAllocatedBytes, jemallocActiveBytes};
  }

  void trackObjectSizeDistributionStats() {
    // scan the cache to get the object size
    for (auto itr = objCache_.l1Cache_->begin();
         itr != objCache_.l1Cache_->end();
         ++itr) {
      size_t objectSize = reinterpret_cast<const typename ObjectCache::Item*>(
                              itr.asHandle()->getMemory())
                              ->objectSize;
      objectSizeBytesHist_.addValue(objectSize);
    }
  }

  // threshold in percentage to determine whether the size-controller should do
  // the calculation
  const size_t kSizeControllerThresholdPct = 50;

  const util::Throttler::Config throttlerConfig_;

  // reference to the object cache
  ObjectCache& objCache_;

  // will be adjusted to control the cache size limit
  std::atomic<size_t> currentEntriesLimit_;

  folly::QuantileHistogram<> objectSizeBytesHist_{};
};

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

#include "cachelib/experimental/objcache2/ObjectCacheSizeController-inl.h"
