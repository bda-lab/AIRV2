#include "QueryAggregator_m.hpp"

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

QueryAggregatorM::QueryAggregatorM(int tag, int rank, int worldSize, int q) : Vertex(tag, rank, worldSize)
{
    queries = q;
    D(cout << "QueryAggregator [" << tag << "] CREATED @ " << rank << endl;);
    pthread_mutex_init(&WIDtoIHM_mutex, NULL);
    pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
}

QueryAggregatorM::~QueryAggregatorM()
{
    D(cout << "QueryAggregator [" << tag << "] DELETED @ " << rank << endl;);
    pthread_mutex_destroy(&WIDtoIHM_mutex);
    pthread_mutex_destroy(&WIDtoWrapperUnit_mutex);
}

void QueryAggregatorM::batchProcess()
{
    D(cout << "QueryAggregator->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void QueryAggregatorM::streamProcess(int channel)
{
    D(cout << "QueryAggregator->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;);

    if (rank == 0)
    {
        Message *inMessage, *outMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        // Updated event structures
        EventSliceM eventSlice;
        EventWJ eventWJ;
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

                int event_count = (inMessage->size) / sizeof(EventSliceM);

                pthread_mutex_lock(&WIDtoIHM_mutex);  // Lock only around necessary parts
                outMessage = new Message(sizeof(EventWJ) * 100);
                for (int i = 0; i < event_count; i++)
                {
                    sede.YSBdeserializeSliceM(inMessage, &eventSlice, i * sizeof(EventSliceM));

                    // Populate eventWJ fields
                    eventWJ.WID = eventSlice.slice_id; 
                    eventWJ.c_id = eventSlice.c_id;
                    eventWJ.ClickCount = eventSlice.ClickCount;
                    eventWJ.ViewCount = eventSlice.ViewCount;
                    eventWJ.latency = eventSlice.latency;
                    eventWJ.ratio = (eventWJ.ViewCount > 0) ? 
                                    static_cast<double>(eventWJ.ClickCount) / eventWJ.ViewCount : 0;

                    // Serialize and print
                    sede.YSBprintWJ(&eventWJ);
                    sede.YSBserializeWJ(&eventWJ, outMessage);
                }
                pthread_mutex_unlock(&WIDtoIHM_mutex); // Unlock once processing done

                // Send outMessage to next vertices
                for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
                {
                    if (outMessage && outMessage->size > 0)
                    {
                        int idx = 0; // Channel for rank 0
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
    else if (rank < queries)
    {

        Message *inMessage, *outMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        // Updated event structures
        EventSliceM eventSlice;
        EventWJ eventWJ;
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

                int event_count = (inMessage->size) / sizeof(EventSliceM);
                pthread_mutex_lock(&WIDtoIHM_mutex);  // Lock only necessary section

                for (int i = 0; i < event_count; i++)
                {
                    sede.YSBdeserializeSliceM(inMessage, &eventSlice, i * sizeof(EventSliceM));
                    WID = eventSlice.slice_id / (rank + 1);
                    std::pair<long int, int> wid_rank_pair = std::make_pair(WID, rank);

                    // Track slices
                    slicePresenceMap[wid_rank_pair].insert(eventSlice.slice_id);
                    if (slicePresenceMap[wid_rank_pair].size() == (rank + 1))
                    {
                        completed_windows.push_back(WID);
                        slicePresenceMap.erase(wid_rank_pair); // Remove completed windows
                    }

                    // Aggregation of counts and max latency
                    auto &innerMap = WIDtoIHM[WID];
                    auto &clickViewMaxEventTime = innerMap[eventSlice.c_id];

                    clickViewMaxEventTime.first.first += eventSlice.ClickCount;  // Update ClickCount
                    clickViewMaxEventTime.first.second += eventSlice.ViewCount;  // Update ViewCount
                    clickViewMaxEventTime.second = max(clickViewMaxEventTime.second, eventSlice.latency); // Update max latency
                }

                // Output aggregated results for completed windows
                while (!completed_windows.empty())
                {
                    WID = completed_windows.front();
                    completed_windows.pop_front();

                    outMessage = new Message(sizeof(EventWJ) * 100); // Create new message

                    auto &innerMap = WIDtoIHM[WID];
                    for (auto &entry : innerMap)
                    {
                        eventWJ.WID = WID;
                        eventWJ.c_id = entry.first;
                        eventWJ.ClickCount = entry.second.first.first;
                        eventWJ.ViewCount = entry.second.first.second;
                        eventWJ.latency = entry.second.second;

                        eventWJ.ratio = (eventWJ.ViewCount > 0) ? 
                                        static_cast<double>(eventWJ.ClickCount) / eventWJ.ViewCount : 0;

                        sede.YSBprintWJ(&eventWJ);
                        sede.YSBserializeWJ(&eventWJ, outMessage);
                    }
                    innerMap.clear();  // Clear inner map once processed
                }
                pthread_mutex_unlock(&WIDtoIHM_mutex);  // Unlock after processing

                // Send outMessage to next vertices
                for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
                {
                    if (outMessage && outMessage->size > 0)
                    {
                        int idx = rank; // Use rank as channel index
                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage);
                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                    }
                }
                delete inMessage;
                c++;
            }
            tmpMessages->clear(); // Clear temporary messages
        }
        delete tmpMessages; // Free message list
    }
}