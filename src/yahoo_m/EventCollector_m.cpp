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
 * EventCollector_m.cpp
 *
 *  Created on: 12, Aug, 2019
 *      Author: vinu.venugopal


 */
#include <mpi.h>
#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "EventCollector_m.hpp"

using namespace std;

EventCollectorM::EventCollectorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

    // Global stats
    sum_latency = 0;
    sum_counts = 0;
    num_messages = 0;

    S_CHECK(if (rank == 0) {
        datafile.open("Data/results" + to_string(rank) + ".tsv");
    })

    D(cout << "EVENTCOLLECTOR [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorM::~EventCollectorM()
{
    D(cout << "EVENTCOLLECTOR [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorM::batchProcess()
{
    D(cout << "EVENTCOLLECTOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void EventCollectorM::streamProcess(int channel)
{

    D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;)

    if (rank == 0)
    {

        Message *inMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        EventWJ eventWJ;

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

                cout << "EVENTCOLLECTOR->POP MESSAGE: TAG [" << tag << "] @ "
                     << rank << " CHANNEL " << channel << " BUFFER "
                     << inMessage->size << endl;

                int event_count = inMessage->size / sizeof(EventWJ);
                cout << "EVENT_COUNT: " << event_count << endl;

                int i = 0, count = 0;
                while (i < event_count)
                {
                    sede.YSBdeserializeWJ(inMessage, &eventWJ,
                                          i * sizeof(EventWJ));
                    long int time_now = (long int)(MPI_Wtime() * 1000.0);
                    sum_latency += (time_now - eventWJ.latency);
                    count += eventWJ.ClickCount + eventWJ.ViewCount;
                    //					sede.YSBprintPC(&eventPC);
                    S_CHECK(
                        datafile
                            << eventWJ.WID << "\t"
                            << eventWJ.c_id << "\t"

                            << endl;

                    )
                    cout << "WID: " << eventWJ.WID << "\tc_id: " << eventWJ.c_id << "\tClick count: " << eventWJ.ClickCount << "\tView count: " << eventWJ.ViewCount << "\tmax_event_time: " << eventWJ.latency << endl;

                    i++;
                }
                sum_counts += event_count; // count of distinct c_id's processed
                num_messages++;

                cout << "\n  #" << num_messages << "\tCOUNT: " << count
                     << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts
                     << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
                     << endl;

                delete inMessage; // delete message from incoming queue
                c++;
            }

            tmpMessages->clear();
        }

        delete tmpMessages;
    }
}
