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
 * EventGenerator.cpp
 *
 *  Created on: Dec  06, 2018
 *      Author: vinu.venugopal
 */

#ifndef CONNECTOR_EVENTGENERATOR_HPP_
#define CONNECTOR_EVENTGENERATOR_HPP_

#include "../dataflow/Vertex.hpp"
#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;
#include <fstream>

class EventGenerator : public Vertex
{

public:
	EventGenerator(int tag, int rank, int worldSize, unsigned long tp);

	~EventGenerator();

	void batchProcess();

	void streamProcess(int channel);

private:
	unsigned long throughput;
	// long int gen_window_id;
	std::ofstream datafile;

	vector<string> ad_ids;

	void getNextMessage(EventDG *event, WrapperUnit *wrapper_unit,
						Message *message, int events_per_msg, long int time_now);

	int myrandom(int min, int max);

	string eventtypes[3] = {"click", "view", "purchase"};
};

#endif /* CONNECTOR_EVENTGENERATOR_HPP_ */
