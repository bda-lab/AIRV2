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
 * SlidingWinAgg.cpp
 *
 *  Created on: 17, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "SlidingWinAgg.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>
#include <bits/stdc++.h>
#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

SlidingWinAgg::SlidingWinAgg(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{
    cout << "SlidingWinAgg [" << tag << "] CREATED @ " << rank << " World Size " << worldSize << endl;
    pthread_mutex_init(&WIDtoIHM_mutex, NULL);
    pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
    // std::unordered_map<long int, set<long int>> slicePresenceMap;
    // std::unordered_map<long int, InnerHMap> WIDtoIHM;
}

SlidingWinAgg::~SlidingWinAgg()
{
    D(cout << "SlidingWinAgg [" << tag << "] DELETED @ " << rank << endl;);
}

void SlidingWinAgg::batchProcess()
{
    D(cout << "SlidingWinAgg->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void SlidingWinAgg::streamProcess(int channel)
{
    D(cout << "SlidingWinAgg->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;);

    Message *inMessage, *outMessage;
    list<Message *> *tmpMessages = new list<Message *>();
    Serialization sede;

    EventSlice eventSlice;
    EventPC eventPC;

    int c = 0;
    while (ALIVE)
    {
        pthread_mutex_lock(&listenerMutexes[channel]);

        while (inMessages[channel].empty())
            pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);

        while (!inMessages[channel].empty())
        {
            inMessage = inMessages[channel].front();
            inMessages[channel].pop_front();
            tmpMessages->push_back(inMessage);
        }

        pthread_mutex_unlock(&listenerMutexes[channel]);

        while (!tmpMessages->empty())
        {
            inMessage = tmpMessages->front();
            tmpMessages->pop_front();

            long int WID;
            list<long int> completed_windows;

            int event_count = (inMessage->size) / sizeof(EventSlice);
            pthread_mutex_lock(&WIDtoIHM_mutex);
            outMessage = new Message(sizeof(EventPC) * 100); // Create new message with max. required capacity

            for (int i = 0; i < event_count; i++)
            {
                sede.YSBdeserializeSlice(inMessage, &eventSlice, i * sizeof(EventSlice));
                // WID = eventSlice.slice_id / (rank + 1);
                // wid = (floor((f - rank) / worldsize) * worldsize) + rank
                if (eventSlice.slice_id - rank < 0)
                {
                    // cout << "Event Slice Id " << eventSlice.slice_id << " At rank" << rank << " IS BEING SKIPPED " << endl;
                    continue;
                }
                WID = (floor((eventSlice.slice_id - rank) / worldSize) * worldSize) + rank;
                // WID = (floor((eventSlice.slice_id - rank) / groupSize) * groupSize) + rank;

                std::pair<long int, int> wid_rank_pair = std::make_pair(WID, rank);
                // cout << "Event Slice Id " << eventSlice.slice_id << " At rank" << rank << " temp " << floor((eventSlice.slice_id - rank) / worldSize) << " belongs to WID: " << WID << endl;
                // **Update**: Track slices properly
                slicePresenceMap[wid_rank_pair].insert(eventSlice.slice_id);
                if (slicePresenceMap[wid_rank_pair].size() == worldSize)
                {
                    // cout << "Wid " << WID << " rank " << rank << " is completed" << endl;
                    completed_windows.push_back(WID);
                    slicePresenceMap.erase(wid_rank_pair); // Remove WID from tracking map once complete
                }

                // **Update**: Aggregate slices for the same WID
                WIDtoIHM_it = WIDtoIHM.find(WID);
                if (WIDtoIHM_it != WIDtoIHM.end())
                {
                    CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.find(eventSlice.c_id);
                    if (CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end())
                    {
                        // Aggregate counts and max_event_time across slices
                        CIDtoCountAndMaxEventTime_it->second.first += eventSlice.count;
                        if (CIDtoCountAndMaxEventTime_it->second.second < eventSlice.latency)
                            CIDtoCountAndMaxEventTime_it->second.second = eventSlice.latency;
                    }
                    else
                    {
                        WIDtoIHM_it->second[eventSlice.c_id] = std::make_pair(eventSlice.count, eventSlice.latency);
                    }
                }
                else
                {
                    InnerHMap newInnerMap;
                    newInnerMap[eventSlice.c_id] = std::make_pair(eventSlice.count, eventSlice.latency);
                    WIDtoIHM[WID] = newInnerMap;
                }
            }

            // **Update**: Generate output after slices aggregation
            while (!completed_windows.empty())
            {
                WID = completed_windows.front();
                completed_windows.pop_front();

                outMessage = new Message(sizeof(EventPC) * 100); // Create new message with max. required capacity

                WIDtoIHM_it = WIDtoIHM.find(WID);
                if (WIDtoIHM_it != WIDtoIHM.end())
                {
                    for (CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.begin();
                         CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end();
                         CIDtoCountAndMaxEventTime_it++)
                    {
                        eventPC.WID = WID;
                        eventPC.c_id = CIDtoCountAndMaxEventTime_it->first;
                        eventPC.count = CIDtoCountAndMaxEventTime_it->second.first;
                        eventPC.latency = CIDtoCountAndMaxEventTime_it->second.second;

                        sede.YSBserializePC(&eventPC, outMessage);
                    }

                    WIDtoIHM_it->second.clear(); // Clear inner map
                    WIDtoIHM.erase(WID);         // Remove from outer map
                }
            }

            pthread_mutex_unlock(&WIDtoIHM_mutex);

            for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
            {
                if (outMessage && outMessage->size > 0)
                {
                    int idx = 0; // Channel equal to rank number
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage);

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }
            }
            delete inMessage;
            c++;
        }

        tmpMessages->clear();
    }

    delete tmpMessages;
}
