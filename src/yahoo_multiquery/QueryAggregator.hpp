/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * Aggregate.hpp
 *
 *  Created on: 18, Dec, 2018
 *      Author: vinu.venugopal
 */

#ifndef OPERATOR_QueryAggregator_HPP_
#define OPERATOR_QueryAggregator_HPP_

#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>
#include "../dataflow/Vertex.hpp"

using namespace std;

typedef std::pair<int, long int> count_maxeventtime;
typedef unordered_map<long int, count_maxeventtime> InnerHMap;
typedef unordered_map<long int, unordered_map<long int, count_maxeventtime>> OuterHMap;
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;
// typedef unordered_map<long int, set<long int>> SlicePresenceMap;
// typedef unordered_map<std::pair<long int, int>, set<long int>> SlicePresenceMap;
struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &pair) const
    {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};
typedef unordered_map<std::pair<long int, int>, set<long int>, pair_hash> SlicePresenceMap;

// typedef unordered_map<long int, InnerHMap> WIDToIHM;
class QueryAggregator : public Vertex
{

public:
    OuterHMap WIDtoIHM;
    pthread_mutex_t WIDtoIHM_mutex;
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;
    pthread_mutex_t WIDtoWrapperUnit_mutex;
    SlicePresenceMap slicePresenceMap;
    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
    OuterHMap::iterator WIDtoIHM_it;
    InnerHMap::iterator CIDtoCountAndMaxEventTime_it;
    int queries;
    // std::unordered_map<long int, InnerHMap> WIDtoIHM;

    // WIDToIHM WIDtoIHM;

    QueryAggregator(int tag, int rank, int worldSize, int q);

    ~QueryAggregator();

    void batchProcess();

    void streamProcess(int channel);
};

#endif /* OPERATOR_QueryAggregator_HPP_ */
