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
 * SerializedCollectorM.cpp
 *
 *  Created on: Dec 26, 2018
 *      Author: vinu.venugopal
 */

#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "SerializedCollectorM.hpp"
#include <bits/stdc++.h>
#include <mpi.h>
#include <chrono>
#include <time.h>
using namespace std;
using namespace std::chrono;

SerializedCollectorM::SerializedCollectorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

    // Global stats
    sum_latency = 0;
    sum_counts = 0;
    num_messages = 0;
    // min_window_id = 0;
    is_min_window_id_initialized = false;
    S_CHECK(if (rank == 0) {
        datafile.open("Data/results" + to_string(rank) + ".tsv");
    })

    D(cout << "SerializedCollectorM [" << tag << "] CREATED @ " << rank << endl;)
}

SerializedCollectorM::~SerializedCollectorM()
{
    D(cout << "SerializedCollectorM [" << tag << "] DELETED @ " << rank << endl;)
}

void SerializedCollectorM::batchProcess()
{
    D(cout << "SerializedCollectorM->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void SerializedCollectorM::streamProcess(int channel)
{

    D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;)

    if (rank == 0)
    {

        Message *inMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        EventWJ eventWJ;
        unordered_map<string, EventWJ> widCidToEventWJ;
        unordered_map<long int, vector<long int>> widToCids;
        unordered_map<long int, long int> widToSumCount;
        int c = 0;
        while (ALIVE)
        {

            pthread_mutex_lock(&listenerMutexes[channel]);

            while (inMessages[channel].empty())
                pthread_cond_wait(&listenerCondVars[channel],
                                  &listenerMutexes[channel]);

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

                int event_count = inMessage->size / sizeof(EventWJ);

                int i = 0;
                while (i < event_count)
                {
                    sede.YSBdeserializeWJ(inMessage, &eventWJ,
                                          i * sizeof(EventWJ));

                    string key = to_string(eventWJ.WID) + "_" + to_string(eventWJ.c_id);
                    widCidToEventWJ[key] = eventWJ;
                    widToCids[eventWJ.WID].push_back(eventWJ.c_id);
                    i++;
                }
                sum_counts += event_count; // count of distinct c_id's processed
                num_messages++;
                widToSumCount[eventWJ.WID] = event_count;
                if (!is_min_window_id_initialized)
                {
                    min_window_id = eventWJ.WID;
                    is_min_window_id_initialized = true;
                    cout << "Min Window ID = " << min_window_id << endl;
                }
                delete inMessage; // delete message from incoming queue
                c++;
            }
            int iter = 0, total_count = 0;
            // Process events for min_window_id
            if (widToCids.count(min_window_id) > 0)
            {
                long int time_now = (long int)(MPI_Wtime() * 1000.0);
                while (widToCids.count(min_window_id) > 0)
                {
                    for (long int cid : widToCids[min_window_id])
                    {
                        string key = to_string(min_window_id) + "_" + to_string(cid);
                        EventWJ eventWJ = widCidToEventWJ[key];
                        sum_latency += (time_now - eventWJ.latency);
                        total_count += eventWJ.ClickCount + eventWJ.ViewCount;
                        cout << "WID: " << eventWJ.WID << "\tc_id: " << eventWJ.c_id << "\tClick count: " << eventWJ.ClickCount << "\tView count: " << eventWJ.ViewCount << "\tmax_event_time: " << eventWJ.latency << endl;
                        widCidToEventWJ.erase(key);
                    }
                    cout << "\n  #" << num_messages << " COUNT: " << total_count
                         << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << widToSumCount[eventWJ.WID] << "\n";
                    widToCids.erase(min_window_id);
                    min_window_id++;
                }
            }
            tmpMessages->clear();
        }
        delete tmpMessages;
    }
}
