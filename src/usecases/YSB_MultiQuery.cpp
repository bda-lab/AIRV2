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
 * YSB_MultiQuery.cpp
 *
 *  Created on: July 23, 2018
 *      Author: martin.theobald, vinu.venugopal
 */

#include "YSB_MultiQuery.hpp"

#include "../yahoo/EventFilter.hpp"
#include "../yahoo/EventGenerator.hpp"
#include "../yahoo_multiquery/SliceAggregator.hpp"
#include "../yahoo_multiquery/QueryAggregator.hpp"
#include "../yahoo/PartialAggregator.hpp"
#include "../yahoo/SHJoin.hpp"
#include "../yahoo_multiquery/QueryCollector.hpp"
using namespace std;
/**
 * We calculate the latency as the difference between the result generation timestamp for a given `time_window` and `campaign_id`
 * pair and the event timestamp of the latest record generated that belongs to that bucket.
 **/

YSB_MultiQuery::YSB_MultiQuery(unsigned long throughput, int queries) : Dataflow()
{

    generator = new EventGenerator(1, rank, worldSize, throughput);
    filter = new EventFilter(2, rank, worldSize);
    join = new SHJoin(3, rank, worldSize);
    par_aggregate = new PartialAggregator(4, rank, worldSize);
    slice_aggregator = new SliceAggregator(5, rank, worldSize,queries);
    query_aggregator = new QueryAggregator(6, rank, worldSize,queries);
    //collector = new QueryCollector(7, rank, worldSize);

    addLink(generator, filter);
    addLink(filter, join);
    addLink(join, par_aggregate);
    addLink(par_aggregate, slice_aggregator);
    addLink(slice_aggregator, query_aggregator);
    //addLink(query_aggregator, collector);

    generator->initialize();
    filter->initialize();
    join->initialize();
    par_aggregate->initialize();
    slice_aggregator->initialize();
    query_aggregator->initialize();
    //collector->initialize();
}

YSB_MultiQuery::~YSB_MultiQuery()
{

    delete generator;
    delete filter;
    delete join;
    delete par_aggregate;
    delete slice_aggregator;
    delete query_aggregator;
    //delete collector;
}
