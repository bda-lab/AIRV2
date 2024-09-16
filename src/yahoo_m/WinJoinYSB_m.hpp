
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
 * WinJoinYSB_m.hpp
 *
 *  Created on: 12, Aug, 2019
 *      Author: vinu.venugopal

 
 */


#ifndef OPERATOR_WINJOINYSBM_HPP_
#define OPERATOR_WINJOINYSBM_HPP_

#include <unordered_map>
#include <utility>
#include "../dataflow/Vertex.hpp"

using namespace std;

// Typedefs for handling event and window data
typedef std::pair<long int, long int> counts_click_view_pair;  // Pair of click and view counts
typedef unordered_map<long int, counts_click_view_pair> InnerHMapM;  // Inner hashmap for c_id -> (click_count, view_count)
typedef unordered_map<long int, InnerHMapM> OuterHMapM;  // Outer hashmap for WID -> c_id -> (click_count, view_count)
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;  // WID -> (completeness numerator, denominator) for window completeness

// Typedef for storing event times
typedef std::pair<long int, long int> EventTimePair;  // (clickEventTime, viewEventTime)

class WinJoinYSBM : public Vertex {
public:
    OuterHMapM WIDtoCIDtoCounts;  // Hashmap for WID -> c_id -> click/view counts
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;  // Hashmap for window completeness

    // New hashmap to store click and view event times for each WID and c_id
    unordered_map<long int, unordered_map<long int, EventTimePair>> CIDtoEventTime;

    // Mutexes for thread safety
    pthread_mutex_t WIDtoCIDtoCounts_mutex;  // Mutex for the outer hashmap
    pthread_mutex_t WIDtoWrapperUnit_mutex;  // Mutex for window completeness hashmap

    // Constructor and Destructor
    WinJoinYSBM(int tag, int rank, int worldSize);
    ~WinJoinYSBM();

    // Core methods for batch and stream processing
    void batchProcess();
    void streamProcess(int channel);

};

#endif /* OPERATOR_WINJOINYSBM_HPP_ */
