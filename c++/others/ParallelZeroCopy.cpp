#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <limits>
#include <algorithm>
#include <string>
#include <chrono>
#include <sys/time.h>
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
//string fileName = "berlin.txt";
//const int NUM_OF_VERTICES = 12746;
//const int NUM_OF_EDGES = 1209980;

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



int NUM_OF_THREADS = 4;

int threshold = NUM_OF_THREADS * 20;

int max_batch_size = 24000;

int numOfQueries = 100;

int departureTime = 0;

bool printResult = false;

int main(int argc, char** argv) {

	vector<Edge> edges;

	/*
	********************************************************************************************************
	Read data file
	********************************************************************************************************
	*/
	cout << "Reading data file..." << endl;

	std::ifstream input("../data/" + fileName + ".edges");
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
	Get batches information
	********************************************************************************************************
	*/
	cout << "Reading meta file..." << endl;

	vector<int> batchSizes;

	std::ifstream input2("../data/" + fileName + ".meta");
	string line2;
	while (std::getline(input2, line2)) {
		std::istringstream iss(line2);

		int size;

		if (!(iss >> size)) {
			break;
		}

		while (size > max_batch_size) {
			batchSizes.push_back(max_batch_size);
			size = size - max_batch_size;
		}

		batchSizes.push_back(size);

	}
	input.close();

	int totalEdges = 0;
	for (int i = 0; i < batchSizes.size(); i++) {
		totalEdges += batchSizes.at(i);
	}


	cout << "Total number of edges in all batches: " << totalEdges << "." << endl;

	if (totalEdges != NUM_OF_EDGES) {
		cout << "Error! The number of edges does not match." << endl;
		return 1;
	}



	/*
	********************************************************************************************************
	Create thread-based edge distributions
	********************************************************************************************************
	*/


	unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
	cout << "Number of cpu cores: " << concurentThreadsSupported << endl;

	if (concurentThreadsSupported != NUM_OF_THREADS) {
		cout << "Error! The number of threads does not match." << endl;
		return 1;
	}

	cout << "threshold: " << threshold << "..." << endl;


	struct Batch {
		bool parallel;
		// For parallel processing
		//vector<vector<Edge>> threadWorkload;
    vector<Edge>* threadWorkload;

		// For sequential processing
		vector<Edge> singleWorkload;
	};


	vector<Batch> batches;

	int currentStart = 0;
	int currentEnd = -1;
	for (int i = 0; i < batchSizes.size(); i++) {
		int currentSize = batchSizes.at(i);

		currentStart = currentEnd + 1;
		currentEnd = currentStart + currentSize - 1;
		
		Batch batch; 
		
		if (currentSize < threshold) {
			// sequential
			batch.parallel = false;	

			vector<Edge> singleWorkload;
			for (int j = currentStart; j <= currentEnd; j++) {
				singleWorkload.push_back(edges.at(j));
			}

			batch.singleWorkload = singleWorkload;

		} else {
			// parallel
			batch.parallel = true;
			int shortestLength;
			int thread;

			vector<Edge>* threadWorkload = new vector<Edge>[NUM_OF_THREADS];

			unordered_map<int, int> vertexThreadMap;

			for (int k = currentStart; k <= currentEnd; k++) {
				Edge e = edges.at(k);
				
				int destIndex = e.toIndex;
				
				auto searchThread = vertexThreadMap.find(destIndex);
				if (searchThread != vertexThreadMap.end()) {
					thread = searchThread->second;
					
					threadWorkload[thread].push_back(e);
				} else {
					
					thread = 0;
					shortestLength = threadWorkload[thread].size();
	
					// Find the shortest queue
					for (int t = 1; t < NUM_OF_THREADS; t++) {
						if (threadWorkload[t].size() < shortestLength) {
							shortestLength = threadWorkload[t].size();
							thread = t;
						}
					}
					
					threadWorkload[thread].push_back(e);
					vertexThreadMap[destIndex] = thread;
				}
			}
			batch.threadWorkload = threadWorkload;
		}
		batches.push_back(batch);
	}

	//cout << "Start batch verification..." << endl;
	for (int i = 0; i < batches.size(); i++) {
		Batch b = batches.at(i);

		if (b.parallel == true) {
			//cout << "Parallel: ";
			for (int j = 0; j < NUM_OF_THREADS; j++) {
				vector<Edge> individualload = b.threadWorkload[j];
			}
		} else {
			//cout << "Sequential: " << b.singleWorkload.size() << endl;
		}
	}
	//cout << "End batch verification..." << endl;




	/*
	********************************************************************************************************
	Get shortest paths -- parallel -- single source multiple times
	********************************************************************************************************
	*/

	

	int* labels = new int[NUM_OF_VERTICES];
	int imax = std::numeric_limits<int>::max();

	auto start = std::chrono::system_clock::now();
  long time_init_label = 0;
  long time_parallel = 0;
	for (int i = 0; i < numOfQueries; i++) {
		//cout << "Start new query..." << endl;

    struct timeval start_timeval, end_timeval;
		// Initialize labels
    gettimeofday(&start_timeval, NULL);
		for (int j = 0; j < NUM_OF_VERTICES; j++) {
			labels[j] = imax;
		}
    gettimeofday(&end_timeval, NULL);
    time_init_label += (end_timeval.tv_sec * 1000 + end_timeval.tv_usec / 1000)
                       - (start_timeval.tv_sec * 1000 + start_timeval.tv_usec / 1000);

		// Get source vertex
		labels[i] = departureTime;

		
		for (int j = 0; j < batches.size(); j++) {
			Batch& b = batches.at(j);

			if (b.parallel == true) {
        struct timeval start_parallel, end_parallel;
        gettimeofday(&start_parallel, NULL);

        #pragma omp parallel for
        for (int k = 0; k < NUM_OF_THREADS; ++k) {
          //std::cout << "test" << std::endl;
					vector<Edge>& individualload = b.threadWorkload[k];
          //vector<Edge> individualload = b.threadWorkload[k];
          //b.threadWorkload[k];
          //int size = individualload.size();

					for (int l = 0; l < individualload.size(); l++) {
						Edge& e = individualload.at(l);

						//cout << e.fromIndex << " " << e.toIndex << " " << e.departure << " " << e.arrival << endl;

						//int sourceIndex = e.fromIndex;
						if (e.departure >= labels[e.fromIndex]) {
							//cout << "Feasible!!" << endl;
							//int destIndex = e.toIndex;
							if (e.arrival < labels[e.toIndex]) {
								labels[e.toIndex] = e.arrival;
								//cout << "Update!!" << endl;
							}
						}
					}
				} // end omp parallel

        gettimeofday(&end_parallel, NULL);
        time_parallel = time_parallel + (end_parallel.tv_sec * 1000 + end_parallel.tv_usec / 1000)
            - (start_parallel.tv_sec * 1000 + start_parallel.tv_usec / 1000);

        //cout << endl;

			} else {

				vector<Edge>& singleWorkload = b.singleWorkload;
				//cout << "Sequential: " << b.singleWorkload.size() << endl;

				for (int k = 0; k < singleWorkload.size(); k++) {
					Edge& e = singleWorkload.at(k);

					//cout << e.fromIndex << " " << e.toIndex << " " << e.departure << " " << e.arrival << endl;

					int sourceIndex = e.fromIndex;
					if (e.departure >= labels[sourceIndex]) {
						//cout << "Feasible!!" << endl;

						int destIndex = e.toIndex;
						if (e.arrival < labels[destIndex]) {
							labels[destIndex] = e.arrival;
							//cout << "Update!!" << endl;

						}
					}
				}


			}

		}

		if (printResult) {
			int numReachable = 0;
			for (int k = 0; k < NUM_OF_VERTICES; k++) {
				if (labels[k] != imax) {
					numReachable++;
				}
			}
			cout << "Number of reachable vertices: " << numReachable << endl;
		}

		//cout << "End new query..." << endl;

	}

	auto end = std::chrono::system_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	std::cout << endl << "Parallel running time: " << elapsed.count() << endl;
  std::cout << "time_init_label" << time_init_label << endl;
  std::cout << "time_parallel_label" << time_parallel << endl;


  /*
  ********************************************************************************************************
  Clean up
  ********************************************************************************************************
  */

	edges.clear();
	delete[] labels;


	return 0;


}





