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
 * QueryAggregator.cpp
 *
 *  Created on: 17, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "QueryAggregator.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

QueryAggregator::QueryAggregator(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{
    D(cout << "QueryAggregator [" << tag << "] CREATED @ " << rank << endl;);
    pthread_mutex_init(&WIDtoIHM_mutex, NULL);
    pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
}

QueryAggregator::~QueryAggregator()
{
    D(cout << "QueryAggregator [" << tag << "] DELETED @ " << rank << endl;);
}

void QueryAggregator::batchProcess()
{
    D(cout << "QueryAggregator->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void QueryAggregator::streamProcess(int channel)
{

    D(cout << "QueryAggregator->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;);
    // if (rank == 0)
    // {
    //     Message *inMessage, *outMessage;
    //     list<Message *> *tmpMessages = new list<Message *>();
    //     Serialization sede;

    //     WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
    //     OuterHMap::iterator WIDtoIHM_it;
    //     InnerHMap::iterator CIDtoCountAndMaxEventTime_it;

    //     WrapperUnit wrapper_unit;
    //     EventSlice eventSlice;
    //     EventPC eventPC;

    //     int c = 0;
    //     while (ALIVE)
    //     {
    //         pthread_mutex_lock(&listenerMutexes[channel]);

    //         while (inMessages[channel].empty())
    //             pthread_cond_wait(&listenerCondVars[channel],
    //                               &listenerMutexes[channel]);

    //         while (!inMessages[channel].empty())
    //         {
    //             inMessage = inMessages[channel].front();
    //             inMessages[channel].pop_front();
    //             tmpMessages->push_back(inMessage);
    //         }

    //         pthread_mutex_unlock(&listenerMutexes[channel]);

    //         while (!tmpMessages->empty())
    //         {

    //             inMessage = tmpMessages->front();
    //             tmpMessages->pop_front();

    //             cout << "QueryAggregator->POP MESSAGE: TAG [" << tag << "] @ "
    //                  << rank << " CHANNEL " << channel << " BUFFER "
    //                  << inMessage->size << endl;

    //             long int WID;
    //             list<long int> completed_windows;

    //             sede.unwrap(inMessage);
    //             if (inMessage->wrapper_length > 0)
    //             {
    //                 cout << "Query Aggregator: Unwrapping the first wrapper unit" << endl;
    //                 cout << "******************************************************************" << endl;
    //                 sede.unwrapFirstWU(inMessage, &wrapper_unit);
    //                 sede.printWrapper(&wrapper_unit);

    //                 // find a fix for this
    //                 wrapper_unit.completeness_tag_denominator = 1;

    //                 WID = wrapper_unit.window_start_time;
    //                 cout << "The window id is: " << WID << endl;
    //                 if (wrapper_unit.completeness_tag_denominator == 1)
    //                 {
    //                     completed_windows.push_back(WID);
    //                 }
    //                 else
    //                 {
    //                     pthread_mutex_lock(&WIDtoWrapperUnit_mutex); //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    //                     if ((WIDtoWrapperUnit_it = WIDtoWrapperUnit.find(WID)) != WIDtoWrapperUnit.end())
    //                     {

    //                         WIDtoWrapperUnit_it->second.first =
    //                             WIDtoWrapperUnit_it->second.first + wrapper_unit.completeness_tag_numerator;
    //                         cout << "WID: " << WID << " NUM: " << WIDtoWrapperUnit_it->second.first << " DEN: " << WIDtoWrapperUnit_it->second.second << endl;
    //                         if (WIDtoWrapperUnit_it->second.first / WIDtoWrapperUnit_it->second.second)
    //                         {
    //                             completed_windows.push_back(WID);
    //                             WIDtoWrapperUnit.erase(WID);
    //                         }
    //                     }
    //                     else
    //                     {

    //                         WIDtoWrapperUnit.emplace(WID,
    //                                                  make_pair(
    //                                                      wrapper_unit.completeness_tag_numerator,
    //                                                      wrapper_unit.completeness_tag_denominator));
    //                     }

    //                     pthread_mutex_unlock(&WIDtoWrapperUnit_mutex); //^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                 }
    //             }

    //             int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

    //             outMessage = new Message(sizeof(EventPC) * 100); // create new message with max. required capacity

    //             int event_count = (inMessage->size - offset) / sizeof(EventSlice);

    //             pthread_mutex_lock(&WIDtoIHM_mutex);

    //             int i = 0, j = 0, k = 0;
    //             while (i < event_count)
    //             {
    //                 // sede.YSBprintSlice(&eventSlice);
    //                 sede.YSBdeserializeSlice(inMessage, &eventSlice,
    //                                          offset + (i * sizeof(EventSlice)));

    //                 WID = eventSlice.latency / AGG_WIND_SPAN;

    //                 if ((WIDtoIHM_it = WIDtoIHM.find(WID)) != WIDtoIHM.end())
    //                 {

    //                     if ((CIDtoCountAndMaxEventTime_it =
    //                              WIDtoIHM_it->second.find(eventSlice.c_id)) != WIDtoIHM_it->second.end())
    //                     {

    //                         CIDtoCountAndMaxEventTime_it->second.first =
    //                             CIDtoCountAndMaxEventTime_it->second.first + eventSlice.count;

    //                         if (CIDtoCountAndMaxEventTime_it->second.second < eventSlice.latency)
    //                         { // new max. event time!
    //                             CIDtoCountAndMaxEventTime_it->second.second =
    //                                 eventSlice.latency;
    //                         }
    //                     }
    //                     else
    //                     { // new entry in inner hashmap!

    //                         WIDtoIHM_it->second.emplace(eventSlice.c_id,
    //                                                     std::make_pair(eventSlice.count,
    //                                                                    eventSlice.latency));
    //                         k++;
    //                     }
    //                 }
    //                 else
    //                 { // new entry in outer hashmap!

    //                     InnerHMap new_CIDtoCountAndMaxEventTime(100);
    //                     new_CIDtoCountAndMaxEventTime.emplace(eventSlice.c_id,
    //                                                           std::make_pair(eventSlice.count,
    //                                                                          eventSlice.latency));
    //                     WIDtoIHM.emplace(WID, new_CIDtoCountAndMaxEventTime);

    //                     j++;
    //                 }

    //                 i++;
    //             }

    //             while (!completed_windows.empty())
    //             {

    //                 WID = completed_windows.front();
    //                 completed_windows.pop_front();

    //                 WIDtoIHM_it = WIDtoIHM.find(WID);
    //                 if (WIDtoIHM_it != WIDtoIHM.end())
    //                 {

    //                     j = 0;
    //                     for (CIDtoCountAndMaxEventTime_it =
    //                              WIDtoIHM_it->second.begin();
    //                          CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end();
    //                          CIDtoCountAndMaxEventTime_it++)
    //                     {

    //                         eventPC.WID = WID;
    //                         eventPC.c_id = CIDtoCountAndMaxEventTime_it->first;
    //                         eventPC.count =
    //                             CIDtoCountAndMaxEventTime_it->second.first;
    //                         eventPC.latency = CIDtoCountAndMaxEventTime_it->second.second;
    //                         // eventPC.latency = eventSlice.latency;
    //                         // cout << "  " << j << "\tWID: " << eventPC.WID
    //                         // 	 << "\tc_id: " << eventPC.c_id << "\tcount: "
    //                         // 	 << eventPC.count << "\tlatency: "
    //                         // 	 << eventPC.latency << " SIZE "
    //                         // 	 << outMessage->size << " CAP "
    //                         // 	 << outMessage->capacity << endl;

    //                         sede.YSBserializePC(&eventPC, outMessage);
    //                         j++;
    //                     }

    //                     WIDtoIHM_it->second.clear(); // clear inner map
    //                     WIDtoIHM.erase(WID);         // remove from outer map
    //                 }
    //             }

    //             pthread_mutex_unlock(&WIDtoIHM_mutex);
    //             int n = 0;
    //             for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
    //                  ++v)
    //             {

    //                 if (outMessage->size > 0)
    //                 {

    //                     int idx = n * worldSize + 0; // send to rank 0 only

    //                     // Normal mode: synchronize on outgoing message channel & send message
    //                     pthread_mutex_lock(&senderMutexes[idx]);
    //                     outMessages[idx].push_back(outMessage);

    //                     D(cout << "QueryAggregator->PUSHBACK MESSAGE [" << tag << "] #"
    //                            << c << " @ " << rank << " IN-CHANNEL " << channel
    //                            << " OUT-CHANNEL " << idx << " SIZE "
    //                            << outMessage->size << " CAP "
    //                            << outMessage->capacity << endl;);

    //                     pthread_cond_signal(&senderCondVars[idx]);
    //                     pthread_mutex_unlock(&senderMutexes[idx]);
    //                 }
    //                 else
    //                 {

    //                     delete outMessage;
    //                 }

    //                 n++;
    //                 break; // only one successor node allowed!
    //             }

    //             delete inMessage;
    //             c++;
    //         }

    //         tmpMessages->clear();
    //     }

    //     delete tmpMessages;
    // }
    if (rank == 1)
    {
        Message *inMessage, *outMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
        OuterHMap::iterator WIDtoIHM_it;
        InnerHMap::iterator CIDtoCountAndMaxEventTime_it;

        // WrapperUnit wrapper_unit;
        EventSlice eventSlice;
        EventPC eventPC;

        std::unordered_map<long int, int> sliceCountMap;
        std::unordered_map<long int, InnerHMap> WIDtoIHM;

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

                cout << "QueryAggregator->POP MESSAGE: TAG [" << tag << "] @ "
                     << rank << " CHANNEL " << channel << " BUFFER "
                     << inMessage->size << endl;

                long int WID;
                list<long int> completed_windows;

                sede.unwrap(inMessage);
                if (inMessage->wrapper_length > 0)
                {
                    cout << "Query Aggregator: Unwrapping the first wrapper unit" << endl;
                    cout << "******************************************************************" << endl;
                    sede.unwrapFirstWU(inMessage, &wrapper_unit);
                    sede.printWrapper(&wrapper_unit);

                    WID = wrapper_unit.window_start_time / (rank + 1);
                    cout << "The window id is: " << WID << endl;
                    pthread_mutex_lock(&WIDtoIHM_mutex);

                    if (++sliceCountMap[WID] == 2)
                    {
                        completed_windows.push_back(WID);
                    }
                }
                //

                int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));
                int event_count = (inMessage->size - offset) / sizeof(EventSlice);
                outMessage = new Message(sizeof(EventPC) * 100); // create new message with max. required capacity
                pthread_mutex_lock(&WIDtoIHM_mutex);
                for (int i = 0; i < event_count; i++)
                {
                    sede.YSBdeserializeSlice(inMessage, &eventSlice, offset + (i * sizeof(EventSlice)));

                    if ((WIDtoIHM_it = WIDtoIHM.find(WID)) != WIDtoIHM.end())
                    {
                        if ((CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.find(eventSlice.c_id)) != WIDtoIHM_it->second.end())
                        {
                            CIDtoCountAndMaxEventTime_it->second.first += eventSlice.count;
                            CIDtoCountAndMaxEventTime_it->second.second = std::max(CIDtoCountAndMaxEventTime_it->second.second, eventSlice.latency);
                        }
                        else
                        {
                            WIDtoIHM_it->second.emplace(eventSlice.c_id, std::make_pair(eventSlice.count, eventSlice.latency));
                        }
                    }
                    else
                    {
                        InnerHMap new_CIDtoCountAndMaxEventTime(100);
                        new_CIDtoCountAndMaxEventTime.emplace(eventSlice.c_id, std::make_pair(eventSlice.count, eventSlice.latency));
                        WIDtoIHM.emplace(WID, new_CIDtoCountAndMaxEventTime);
                    }
                }

                pthread_mutex_unlock(&WIDtoIHM_mutex);

                while (!completed_windows.empty())
                {
                    WID = completed_windows.front();
                    completed_windows.pop_front();

                    outMessage = new Message(sizeof(EventPC) * 100); // create new message with max. required capacity

                    WIDtoIHM_it = WIDtoIHM.find(WID);
                    if (WIDtoIHM_it != WIDtoIHM.end())
                    {
                        for (CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.begin(); CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end(); CIDtoCountAndMaxEventTime_it++)
                        {
                            eventPC.WID = WID;
                            eventPC.c_id = CIDtoCountAndMaxEventTime_it->first;
                            eventPC.count = CIDtoCountAndMaxEventTime_it->second.first;
                            eventPC.latency = CIDtoCountAndMaxEventTime_it->second.second;

                            sede.YSBserializePC(&eventPC, outMessage);
                        }

                        WIDtoIHM_it->second.clear(); // clear inner map
                        WIDtoIHM.erase(WID);         // remove from outer map
                    }

                    if (outMessage->size > 0)
                    {
                        int idx = 0; // Send to rank 0

                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage);

                        D(cout << "QueryAggregator->PUSHBACK MESSAGE [" << tag << "] #" << c << " @ " << rank << " IN-CHANNEL " << channel << " OUT-CHANNEL " << idx << " SIZE " << outMessage->size << " CAP " << outMessage->capacity << endl;);

                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                    }
                    else
                    {
                        delete outMessage;
                    }
                }

                delete inMessage;
                c++;
            }

            tmpMessages->clear();
        }

        delete tmpMessages;
    }
}