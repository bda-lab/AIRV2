#include "YSBM_MultiQuery.hpp"

#include "../yahoo_m/EventCollector_m.hpp"
#include "../yahoo_m/EventFilter_m.hpp"
#include "../yahoo/EventGenerator.hpp"
#include "../yahoo_m/FullAggregator_m.hpp"
#include "../yahoo_m/WinJoinYSB_m.hpp"
#include "../yahoo/PartialAggregator.hpp"
#include "../yahoo/SHJoin.hpp"
#include "../yahoo_m/WinJoinSlice_m.hpp"
#include "../yahoo_m/QueryAggregator_m.hpp"
#include "../yahoo_m/QueryCollector_m.hpp"


using namespace std;

YSBM_MultiQuery::YSBM_MultiQuery(unsigned long throughput) :
		Dataflow() {

	generator = new EventGenerator(1, rank, worldSize, throughput);
	filter = new EventFilterM(2, rank, worldSize);
	joinClick = new SHJoin(3, rank, worldSize);
	joinView = new SHJoin(4, rank, worldSize);
	par_aggregateClick = new PartialAggregator(5, rank, worldSize);
	par_aggregateView = new PartialAggregator(6, rank, worldSize);
	full_aggregateClick = new FullAggregatorM(7, rank, worldSize, 1); //last argument denotes event_type click=1 and view=2
	full_aggregateView = new FullAggregatorM(8, rank, worldSize, 2);
	ratioFinder = new WinJoinSliceM(9, rank, worldSize);
	queryAggregator = new QueryAggregatorM(10, rank, worldSize);
	collector = new QueryCollectorM(11, rank, worldSize);

	addLink(generator, filter);
	addLink(filter, joinClick);
	addLink(filter, joinView);
	addLink(joinClick, par_aggregateClick);
	addLink(joinView, par_aggregateView);
	addLink(par_aggregateClick, full_aggregateClick);
	addLink(par_aggregateView, full_aggregateView);
	addLink(full_aggregateView, ratioFinder);
	addLink(full_aggregateClick, ratioFinder);
	addLink(ratioFinder, queryAggregator);
	addLink(queryAggregator, collector);


	generator->initialize();
	filter->initialize();
	joinClick->initialize();
	joinView->initialize();
	par_aggregateClick->initialize();
	par_aggregateView->initialize();
	full_aggregateClick->initialize();
	full_aggregateView->initialize();
	ratioFinder->initialize();
	queryAggregator->initialize();
	collector->initialize();

}

YSBM_MultiQuery::~YSBM_MultiQuery() {

	delete generator;
	delete filter;
	delete joinClick;
	delete joinView;
	delete par_aggregateClick;
	delete par_aggregateView;
	delete full_aggregateClick;
	delete full_aggregateView;
	delete ratioFinder;
	delete queryAggregator;
	delete collector;

}
