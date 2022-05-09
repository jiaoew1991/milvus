// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "exceptions/EasyAssert.h"
#include "ScalarIndex.h"

namespace milvus::segcore {
std::pair<std::shared_ptr<IdArray>, std::vector<SegOffset>>
ScalarIndexVector::do_search_ids(const IdArray& ids) const {
    auto builder = arrow::Int64Builder();
    //    auto res_ids = std::make_unique<IdArray>();
    // TODO: support string array
    auto src_ids = arrow::Int64Array(ids.data());
    std::vector<SegOffset> dst_offsets;

    // TODO: a possible optimization:
    // TODO: sort the input id array to make access cache friendly

    // assume no repeated key now
    // TODO: support repeated key
    for (auto id_iter = src_ids.begin(); id_iter < src_ids.end(); ++id_iter) {
        auto id = src_ids.Value(id_iter.index());
        using Pair = std::pair<T, SegOffset>;
        auto [iter_beg, iter_end] =
            std::equal_range(mapping_.begin(), mapping_.end(), std::make_pair(id, SegOffset(0)),
                             [](const Pair& left, const Pair& right) { return left.first < right.first; });

        for (auto& iter = iter_beg; iter != iter_end; iter++) {
            auto [entry_id, entry_offset] = *iter;
            builder.Append(entry_id);
            dst_offsets.push_back(entry_offset);
        }
    }
    return {std::move(builder.Finish().ValueOrDie()), std::move(dst_offsets)};
}

std::pair<std::vector<idx_t>, std::vector<SegOffset>>
ScalarIndexVector::do_search_ids(const std::vector<idx_t>& ids) const {
    std::vector<SegOffset> dst_offsets;
    std::vector<idx_t> dst_ids;

    for (auto id : ids) {
        using Pair = std::pair<T, SegOffset>;
        auto [iter_beg, iter_end] =
            std::equal_range(mapping_.begin(), mapping_.end(), std::make_pair(id, SegOffset(0)),
                             [](const Pair& left, const Pair& right) { return left.first < right.first; });

        for (auto& iter = iter_beg; iter != iter_end; iter++) {
            auto [entry_id, entry_offset] = *iter_beg;
            dst_ids.emplace_back(entry_id);
            dst_offsets.push_back(entry_offset);
        }
    }
    return {std::move(dst_ids), std::move(dst_offsets)};
}

void
ScalarIndexVector::append_data(const ScalarIndexVector::T* ids, int64_t count, SegOffset base) {
    for (int64_t i = 0; i < count; ++i) {
        auto offset = base + SegOffset(i);
        mapping_.emplace_back(ids[i], offset);
    }
}

void
ScalarIndexVector::build() {
    std::sort(mapping_.begin(), mapping_.end());
}
}  // namespace milvus::segcore
