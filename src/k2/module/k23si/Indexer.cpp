/*
MIT License

Copyright(c) 2022 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include "Indexer.h"

#include <k2/common/MapUtil.h>

#include <iterator>

namespace k2 {
// *********************** IndexerKey API
int IndexerKey::compare(const IndexerKey& o) const noexcept {
    auto pkcomp = partitionKey.compare(o.partitionKey);
    if (pkcomp == 0) {
        // if the partition keys are equal, return the comparison of the range keys
        return rangeKey.compare(o.rangeKey);
    }
    return pkcomp;
}

bool IndexerKey::operator<(const IndexerKey& o) const noexcept {
    return compare(o) < 0;
}
// *********************** end IndexerKey API

// *********************** Indexer API
seastar::future<> Indexer::start() {
    return seastar::make_ready_future();
}

seastar::future<> Indexer::stop() {
    return seastar::make_ready_future();
}

const Indexer::SchemaIndexer& Indexer::getSchemaIndexer() const {
    return _schemaIndexer;
}

void Indexer::createSchema(const dto::Schema& schema) {
    // create a default indexer for the schema if one doesn't exist
    auto [iter, success] = _schemaIndexer.try_emplace(schema.name);
    if (success) {
        K2LOG_D(log::skvsvr, "Created new schema indexer for {}", schema.name);
        // if we did create a new indexer, set the low/high watermarks to the time we created the main indexer
        iter->second.lastReadTimeLow = _createdTs;
        iter->second.lastReadTimeHigh = _createdTs;
    }
}

size_t Indexer::size() {
    // NB, this is not O(1) as we could make it, but in practice it may not matter much
    // We should also report key count per schema as a metric, which would mean iterating over
    // the schema indexes anyway.
    size_t sz = 0;
    for (auto&[_,idxr]: _schemaIndexer) {
        sz += idxr.impl.size();
    }
    return sz;
}

Indexer::Iterator Indexer::find(const dto::Key& key, bool reverse) {
    // if schema doesn't exist, it is an internal error - upon deployment of a new schema, we create an indexer for it
    auto it = _schemaIndexer.find(key.schemaName);
    if (it == _schemaIndexer.end()) {
        K2LOG_E(log::skvsvr, "Invalid schema in key: {}", key);
        throw std::runtime_error("Schema does not exist in schema indexer");
    }

    auto [before, found, after] = keyRange(IndexerKey{.partitionKey=key.partitionKey,.rangeKey=key.rangeKey}, it->second.impl);

    return Iterator(before, found, after, it->second, reverse, key.schemaName);
}
// *********************** end Indexer API

// *********************** Indexer::Iterator API
Indexer::Iterator::Iterator(KeyIndexer::iterator beforeIt, KeyIndexer::iterator foundIt, KeyIndexer::iterator afterIt, KeyIndexer& si, bool reverse, String schemaName):
    _beforeIt(beforeIt), _foundIt(foundIt), _afterIt(afterIt), _si(si), _reverse(reverse), _schemaName(schemaName) {
}

std::vector<dto::AccessRecord> Indexer::Iterator::getAllDataRecords() const {
    std::vector<dto::AccessRecord> result;
    if (_foundIt == _si.impl.end()) {
        return result;
    }
    result.reserve(_foundIt->second.size());

    for (auto& rec : _foundIt->second) {
        dto::AccessRecord copy{};
        copy.txnID = rec.TxnId;
        copy.accessType = rec.accessType;
        copy.requestID = rec.requestID;
        if (rec.data.has_value()) {
            dto::DataRecord data{
                .value = rec.value.share(),
                .timestamp = rec.timestamp,
                .isTombstone = rec.isTombstone};
            copy.data = std::move(data);
        }
        result.push_back(std::move(copy));
    }
    return result;
}

VersionsT* Indexer::Iterator::getAccessRecords() {
    if (hasData()) {
        return &(_foundIt->second);
    }

    return nullptr;
}

void Indexer::Iterator::addWI(const dto::Key& key, dto::DataRecord&& rec, uint64_t request_id) {
    // We need to create the key in the indexer if it didn't exist before
    if (_foundIt == _si.impl.end()) {
        auto lastRead = getLastReadTime();
        _foundIt = _si.impl.insert(_afterIt, std::make_pair(
            IndexerKey{.partitionKey=key.partitionKey, .rangeKey=key.rangeKey}, VersionSet{}));
        // mark the new entry as being observed at the same time as the neighbors.
        _foundIt->second.lastReadTime = lastRead;
        K2LOG_D(log::skvsvr, "Created new key {}", key);
    }
    else {
        const IndexerKey& ourKey = _foundIt->first;
        K2ASSERT(log::skvsvr, ourKey.partitionKey == key.partitionKey && ourKey.rangeKey == key.rangeKey, "Key mismatch while adding key: have={}, given={}", ourKey, key);
    }
    _foundIt->second.WI = dto::WriteIntent{.data=std::move(rec), .request_id=request_id};
}

void Indexer::Iterator::abortWI() {
    if (_foundIt != _si.impl.end()) {
        _foundIt->second.WI.reset();
        if (_foundIt->second.committed.empty()) {
            auto lastObservedAt = _foundIt->second.lastReadTime;
            // this entire entry can now be removed as it has no WI and no committed data
            K2LOG_D(log::skvsvr, "Removing empty entry for key={}, lastObserved=", _foundIt->first, lastObservedAt);
            _si.impl.erase(_foundIt);
            _foundIt = _si.impl.end();
            // update the neighbors with our timestamp
            observeAt(lastObservedAt);
        }
    }
}

void Indexer::Iterator::commitWI() {
    K2ASSERT(log::skvsvr, _foundIt != _si.impl.end() && _foundIt->second.WI.has_value(), "WI must have value to commit");
    _foundIt->second.committed.push_front(std::move(_foundIt->second.WI->data));
    _foundIt->second.WI.reset();
}

void Indexer::Iterator::observeAt(dto::Timestamp ts) {
    K2LOG_D(log::skvsvr, "Observing at ts={}, bit={}, fit={}, ait={}", ts,
            _beforeIt == _si.impl.end() ? IndexerKey{} : _beforeIt->first,
            _foundIt == _si.impl.end() ? IndexerKey{} : _foundIt->first,
            _afterIt == _si.impl.end() ? IndexerKey{} : _afterIt->first);
    if (_foundIt != _si.impl.end()) {
        _foundIt->second.lastReadTime.maxEq(ts);
    } else {
        _beforeIt != _si.impl.end() ? _beforeIt->second.lastReadTime.maxEq(ts) : _si.lastReadTimeLow.maxEq(ts);
        _afterIt  != _si.impl.end() ? _afterIt->second.lastReadTime.maxEq(ts)  : _si.lastReadTimeHigh.maxEq(ts);
    }
}

bool Indexer::Iterator::hasData() const {
    return _foundIt != _si.impl.end() && !_foundIt->second.empty();
}

void Indexer::Iterator::next() {
    if (atEnd()) {
        return;
    }
    auto end = _si.impl.end();
    if (!_reverse) {
        // forward direction
        if (_foundIt != end) {
            // only move our _beforeIt if _foundIt actually was pointing to an element.
            _beforeIt = _foundIt;
        }
        _foundIt = _afterIt;
        if (_afterIt != end) {
            ++_afterIt;
        }
    }
    else if (_reverse) {
        // reverse direction
        if (_foundIt != end) {
            // only move our _afterIt if _foundIt actually was pointing to an element.
            _afterIt = _foundIt;
        }
        _foundIt = _beforeIt;
        if (_beforeIt != _si.impl.begin() && _beforeIt != end) {
            // this is only safe to do if we are not at begin()
            --_beforeIt;
        } else {
            // we can't rewind begin. Set it to end() directly
            _beforeIt = end;
        }
    }
}

bool Indexer::Iterator::atEnd() const {
    auto end = _si.impl.end();
    return _foundIt == end &&
           ((_reverse && _beforeIt == end) || (!_reverse && _afterIt == end));
}

dto::Key Indexer::Iterator::getKey() const {
    dto::Key result{};
    if (_foundIt != _si.impl.end()) {
        result.partitionKey = _foundIt->first.partitionKey;
        result.rangeKey = _foundIt->first.rangeKey;
        result.schemaName = _schemaName;
    }
    return result;
}
// *********************** end Indexer::Iterator API

} // ns k2
