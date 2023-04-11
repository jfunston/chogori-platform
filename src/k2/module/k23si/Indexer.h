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

#pragma once

#include <map>
#include <unordered_map>
#include <deque>
#include <optional>

#if K2_MODULE_POOL_ALLOCATOR == 1
// this can only work on GCC > 4
#include <ext/pool_allocator.h>
#endif

#include <k2/common/Common.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/Timestamp.h>

#include "Log.h"

namespace k2 {

// the type holding multiple access records of a key
typedef std::deque<dto::AccessRecord> VersionsT;

// A key in the indexer. Since this is a schema-aware indexer, we only need to store the pkey and the rkey
struct IndexerKey {
    String partitionKey;
    String rangeKey;
    int compare(const IndexerKey& o) const noexcept;
    bool operator<(const IndexerKey& o) const noexcept;
    K2_DEF_FMT(IndexerKey, partitionKey, rangeKey);
};

// Sorted Key indexer used to map key->vset. It also provides the last observed times at its lowest and highest bounds
struct KeyIndexer {
    // the read intents on the lowest bound (a virtual key smaller than all other keys)
    VersionsT lowerBoundRIs;
    // the read intents on the upper bound (a virtual key bigger than all other keys)
    VersionsT upperBoundRIs;
    // the type holding versions for all keys, i.e. the implementation for this key indexer
    #if K2_MODULE_POOL_ALLOCATOR == 1
    typedef std::map<IndexerKey, VersionsT, std::less<IndexerKey>, __gnu_cxx::__pool_alloc<std::pair<IndexerKey, VersionsT>>> KeyIndexerT;
    #else
    typedef std::map<IndexerKey, VersionsT> KeyIndexerT;
    #endif
    // the implementation container for storing key->vset
    KeyIndexerT impl;
    // an iterator for our implementation container
    typedef KeyIndexerT::iterator iterator;
};

// The indexer for K2 records. It stores records, mapped as: IndexerKey --> dto::DataRecord
class Indexer {
public: // lifecycle
    seastar::future<> start();

    // The Indexer must be stopped before it is destroyed to allow for various state to be safely closed.
    seastar::future<> stop();

    // the type for the schema indexer - maps schemaName->KeyIndexer
    typedef std::unordered_map<String, KeyIndexer> SchemaIndexer;

public: // API
    // returns the number of all keys in the indexer
    size_t size();

    // forward declaration for clarity. See class definition further down
    class Iterator;

    // Returns an Iterator for the given key, preset to iterate in the given direction.
    // The iterator only iterates over keys in the same schema as the given key
    Iterator find(const dto::Key& key, bool reverse=false);

    // creates a new key indexer for the given schema.
    void createSchema(const dto::Schema& schema);

    // raw access to the underlying schema indexer, used by our debugging APIs
    const SchemaIndexer& getSchemaIndexer() const;

private:
    // the indexer, mapping schema_name -> indexer_for_schema
    SchemaIndexer _schemaIndexer;
}; // class KeyIndexer


// This class represents an iterator in the Indexer. Its main features are:
// - allows accessing specific keys
// - supports directional scan operations
// - tracks key or range observations to aid the K23SI transactional consistency
// Iterators are vended by the find() API of the indexer
class Indexer::Iterator {
public:
    // A new Iterator is created with iterators for the keys before, at, and after the key for
    // which we created the Iterator.
    // We also need to have a reference to the underlying KeyIndexer which is being iterated
    // as well as the direction of iteration.
    Iterator(KeyIndexer::iterator beforeIt, KeyIndexer::iterator foundIt, KeyIndexer::iterator afterIt, KeyIndexer& si, bool reverse, String schemaName);

public: // APIs
    // get a copy of all data records at the current Iterator position (Debug API)
    std::vector<dto::AccessRecord> getAllDataRecords() const;

    VersionsT* getAccessRecords();

    // use to determine if the current Iterator position contains any access records
    bool hasData() const;

    // move on to the next position
    void next();

    // determine if we're at the end of iteration - the current position is past the last element
    bool atEnd() const;

    // If there is data, returns the key for the current Iterator position.
    // Otherwise it returns a key with empty contents.
    dto::Key getKey() const;

private:
    // iterators pointing into the KeyIndexer for the keys:
    // These iterators are always in the forward direction. We may rewind them if we're asked
    // to iterate in reverse but they are always positioned forward.
    KeyIndexer::iterator _beforeIt; // before the current position
    KeyIndexer::iterator _foundIt; // at the current position
    KeyIndexer::iterator _afterIt; // the next position

    // this is the key indexer which is being iterated by this Iterator
    KeyIndexer& _si;

    // the direction in which we're iterating
    bool _reverse{false};

    // the name of the schema we're iterating
    String _schemaName;
}; // class Iterator

} // namespace k2
