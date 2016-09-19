#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <limits>
#include <algorithm>
#include <string>
#include <chrono>
#include <vector>
#include <thread>
#include <list>
using std::cin;
using std::cout;
using std::endl;
using std::string;
using std::unordered_map;
using std::list;
using std::vector;


struct Edge {
	public:
	int fromIndex;
	int toIndex;
	int departure;
	int arrival;
};



/*
********************************************************************************************************
Program settings
********************************************************************************************************
*/



// berlin
// string fileName = "berlin.txt";
// const int NUM_OF_VERTICES = 12746;
// const int NUM_OF_EDGES = 1209980;

// wikipedia-growth
string fileName = "out.wikipedia-growth";
const int NUM_OF_VERTICES = 1870709;
const int NUM_OF_EDGES = 39953145;

// munmun_digg_reply
// string fileName = "out.munmun_digg_reply";
// const int NUM_OF_VERTICES = 30360;
// const int NUM_OF_EDGES = 86203;

// loans
// string fileName = "out.prosper-loans";
// const int NUM_OF_VERTICES = 89269;
// const int NUM_OF_EDGES = 3343284;

// digg-friends
// string fileName = "out.digg-friends";
// const int NUM_OF_VERTICES = 279630;
// const int NUM_OF_EDGES = 1731653;



const int numOfRuns = 100;
int departureTime = 0;
bool printResult = true;

int main() {

	vector<Edge> edges;

	/*
	********************************************************************************************************
	Read data file
	********************************************************************************************************
	*/
	cout << "Reading data file..." << endl;

	std::ifstream input("e:/data/" + fileName + ".edges");
	string line;
	while (std::getline(input, line)) {
		std::istringstream iss(line);

		int fromIndex;
		int toIndex;
		int departure;
		int arrival;

		if (!(iss >> fromIndex >> toIndex >> departure >> arrival)) {
			break;
		}
		
		// Add edge
		Edge e;
		e.fromIndex = fromIndex;
		e.toIndex = toIndex;
		e.departure = departure;
		e.arrival = arrival;

		edges.push_back(e);
	}
	input.close();



	/*
	********************************************************************************************************
	Sort edges
	********************************************************************************************************
	*/

	struct sort_by_departure {
		bool operator() (Edge& lhs, Edge& rhs) { return lhs.departure < rhs.departure; }
	};

	std::sort(edges.begin(), edges.end(), sort_by_departure());



	/*
	********************************************************************************************************
	Get shortest paths -- sequential
	********************************************************************************************************
	*/

	
	//int* labels = new int[NUM_OF_VERTICES * numOfRuns];
	int* labels = (int*)malloc(NUM_OF_VERTICES * sizeof(int));
	int imax = std::numeric_limits<int>::max();


	auto start = std::chrono::system_clock::now();


	for (int r = 0; r < numOfRuns; r++) {
		for (int i = 0; i < NUM_OF_VERTICES; i++) {
			labels[i] = imax;
		}

		//labels[NUM_OF_VERTICES - 1 - r] = departureTime;
		labels[r] = departureTime;

		for (int i = 0; i < NUM_OF_EDGES; i++) {
			Edge e = edges.at(i);

			int sourceIndex = e.fromIndex;

			if (e.departure >= labels[sourceIndex]) {
				int destIndex = e.toIndex;
				if (e.arrival < labels[destIndex]) {
					labels[destIndex] = e.arrival;
				}

			}
		}
		
		if (printResult) {
			int numReachable = 0;
			for(int i = 0; i < NUM_OF_VERTICES; i++) {
				if (labels[i] != imax) {
					numReachable++;
				}
			}

			cout << "Number of reachable vertices: " << numReachable << endl;
		
		}


	}


	auto end = std::chrono::system_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	std::cout << endl << "Sequential: " << elapsed.count() << endl;

	
	

	/*
	********************************************************************************************************
	Clean up
	********************************************************************************************************
	*/

	edges.clear();
	free(labels);


	return 0;


}





