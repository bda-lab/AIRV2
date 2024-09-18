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
 * SerializedCollector.cpp
 *
 *  Created on: Dec 26, 2018
 *      Author: vinu.venugopal
 */

#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "SerializedCollector.hpp"
#include <bits/stdc++.h>
#include <mpi.h>
#include <chrono>
#include <time.h>
using namespace std;
using namespace std::chrono;

SerializedCollector::SerializedCollector(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;
	// min_window_id = 0;
	is_min_window_id_initialized=false;
	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(cout << "SerializedCollector [" << tag << "] CREATED @ " << rank << endl;)
}

SerializedCollector::~SerializedCollector()
{
	D(cout << "SerializedCollector [" << tag << "] DELETED @ " << rank << endl;)
}

void SerializedCollector::batchProcess()
{
	D(cout << "SerializedCollector->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void SerializedCollector::streamProcess(int channel)
{

	D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	if (rank == 0)
	{

		Message *inMessage;
		list<Message *> *tmpMessages = new list<Message *>();
		Serialization sede;

		EventPC eventPC;
		unordered_map<string, EventPC> widCidToEventPC;
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

				int event_count = inMessage->size / sizeof(EventPC);

				int i = 0;
				while (i < event_count)
				{
					sede.YSBdeserializePC(inMessage, &eventPC,
										  i * sizeof(EventPC));

					string key = to_string(eventPC.WID) + "_" + to_string(eventPC.c_id);
					widCidToEventPC[key] = eventPC;
					widToCids[eventPC.WID].push_back(eventPC.c_id);
					i++;
				}
				sum_counts += event_count; // count of distinct c_id's processed
				num_messages++;
				widToSumCount[eventPC.WID] = event_count;
				if (!is_min_window_id_initialized)
                {
                    min_window_id = eventPC.WID;
                    is_min_window_id_initialized = true;
					cout<<"Min Window ID = "<<min_window_id<<endl;
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
						EventPC eventPC = widCidToEventPC[key];
						sum_latency += (time_now - eventPC.latency);
						total_count += eventPC.count;
						cout << "WID: " << eventPC.WID << "\tc_id: " << eventPC.c_id << "\tcount: " << eventPC.count << "\tmax_event_time: " << eventPC.latency << endl;
						widCidToEventPC.erase(key);
					}
					cout << "\n  #" << num_messages << " COUNT: " << total_count
						 << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << widToSumCount[eventPC.WID] << "\n";
					widToCids.erase(min_window_id);
					min_window_id++;
				}
			}
			tmpMessages->clear();
		}
		delete tmpMessages;
	}
}
