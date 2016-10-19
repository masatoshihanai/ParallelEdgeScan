#include <atomic>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <limits>
#include <algorithm>
#include <string>
#include <chrono>
#include <vector>
#include <list>
#include <condition_variable>
#include <mutex>
#include <thread>

struct Edge {
 public:
  Edge() {}
  Edge(int fromIndex_, int toIndex_,
       int departure_, int arrival_)
      : fromIndex(fromIndex_), toIndex(toIndex_),
        departure(departure_), arrival(arrival_) {};
  Edge(const Edge& edge_) {
    fromIndex = edge_.fromIndex;
    toIndex = edge_.toIndex;
    departure = edge_.departure;
    arrival = edge_.arrival;
  }
 public:
  int fromIndex;
  int toIndex;
  int departure;
  int arrival;
};

class Barrier {
  std::mutex mtx_;
  std::condition_variable cond_;
  std::size_t num_thread;
  std::size_t count_;
  std::size_t num_barier;
  bool wait_master_end_;

 public:
  explicit Barrier(std::size_t num_thread)
      : count_(num_thread),
        num_thread(num_thread),
        num_barier(0),
        wait_master_end_(false){};

  void start_threads() {
    std::unique_lock<std::mutex> lock(mtx_);
    wait_master_end_ = true;
    cond_.notify_all();
  }

  void wait_start() {
    std::unique_lock<std::mutex> lock(mtx_);
    cond_.wait(lock, [this] {return wait_master_end_;});
  }

  void wait() {
    std::size_t countBarrier = num_barier;
    std::unique_lock<std::mutex> lock(mtx_);
    if (!--count_) {
      ++num_barier;
      count_ = num_thread;
      cond_.notify_all();
    } else {
      cond_.wait(lock, [this, countBarrier] {return countBarrier != num_barier;});
    }
  }
};

/*
********************************************************************************************************
Program settings
********************************************************************************************************
*/

//berlin
//std::string fileName = "berlin.txt";
//const int NUM_OF_VERTICES = 12746; // Scale 13 - 14
//const int NUM_OF_EDGES = 1209980;

// wikipedia-growth
//std::string fileName = "out.wikipedia-growth";
//const int NUM_OF_VERTICES = 1870709; // Scale 21
//const int NUM_OF_EDGES = 39953145; //

// munmun_digg_reply
// string fileName = "out.munmun_digg_reply";
// const int NUM_OF_VERTICES = 30360; // Scale 15
// const int NUM_OF_EDGES = 86203;

// loans
// string fileName = "out.prosper-loans";
// const int NUM_OF_VERTICES = 89269; // Scale 16
// const int NUM_OF_EDGES = 3343284;

// digg-friends
// string fileName = "out.digg-friends";
// const int NUM_OF_VERTICES = 279630; // Scale 18
// const int NUM_OF_EDGES = 1731653;

int THREASHOLD = 1;
int numOfQueries = 100;
int departureTime = 0;
bool printResult = true;

int main(int argc, char** argv) {
  int NUM_THREADS = std::stoi(argv[1]);
  std::string edgeFile = argv[2];
  std::string metaFile = argv[3];

  std::cerr << "# of threads: " << NUM_THREADS << std::endl;
  std::cerr << "Edge File: " << edgeFile << std::endl;
  std::cerr << "Meta File: " << metaFile << std::endl;

  std::vector<Edge> edges;
  /*
  ********************************************************************************************************
  Read data file
  ********************************************************************************************************
  */
  std::cerr << "Reading data file..." << std::endl;

  std::ifstream inputEdge(edgeFile);
  std::string edgeLine;
  std::unordered_set<int> vertexSet;
  while (std::getline(inputEdge, edgeLine)) {
    std::istringstream iss(edgeLine);

    int fromIndex; int toIndex;
    int departure; int arrival;
    if (!(iss >> fromIndex >> toIndex >> departure >> arrival)) {
      break;
    }
    edges.push_back(Edge(fromIndex, toIndex, departure, arrival));
    vertexSet.insert(fromIndex);
    vertexSet.insert(toIndex);
  }
  inputEdge.close();
  int NUM_OF_VERTICES = vertexSet.size();

  /*
  ********************************************************************************************************
  Get batches information
  ********************************************************************************************************
  */
  std::cerr << "Reading meta file..." << std::endl;

  std::vector<int> batchSizes;
  std::ifstream inputBatchSize(metaFile);
  std::string lineBatchSize;
  while (std::getline(inputBatchSize, lineBatchSize)) {
    std::istringstream iss(lineBatchSize);
    int size;
    if (!(iss >> size)) { break; }
    batchSizes.push_back(size);
  }
  inputBatchSize.close();

  /*
  ********************************************************************************************************
  Create thread-based edge distributions
  ********************************************************************************************************
  */
  std::vector<std::vector<Edge> > batchsPerThread(NUM_THREADS);

  // Make Edge Distribution for cache optimize
  auto it = edges.begin();
  int batchID = 0;
  // For multiple threads
  while (batchID < batchSizes.size() && batchSizes[batchID] > THREASHOLD) {
    // insert edges
    for (int edgeCounter = 0; edgeCounter < batchSizes[batchID]; ++edgeCounter) {
      batchsPerThread[edgeCounter % NUM_THREADS].push_back(*it);
      ++it;
    }
    // insert end point of the batch
    for (int threadID = 0; threadID < NUM_THREADS; ++threadID) {
      batchsPerThread[threadID].push_back(Edge(-1,-1,-1,-1));
    }
    ++batchID;
  }

  // For single threads
  while (batchID < batchSizes.size()) {
    for (int edgeCounter = 0; edgeCounter < batchSizes[batchID]; ++edgeCounter) {
      batchsPerThread[0].push_back(*it);
      ++it;
    }
    ++batchID;
  }

  for (int threadID = 0; threadID < NUM_THREADS; ++threadID) {
    batchsPerThread[threadID].push_back(Edge(-2,-2,-2,-2));
  }

  // check all edges are deployed
  if (it != edges.end()) { std::cerr << "fail to read" << std::endl; return 1; }

  /*
  ********************************************************************************************************
  Get shortest paths -- parallel -- single source multiple times
  ********************************************************************************************************
  */
  int imax = std::numeric_limits<int>::max();
  std::vector<std::atomic<int>> labels(NUM_OF_VERTICES);
  std::vector<std::vector<Edge>*> reallocatedEdges(NUM_THREADS, NULL);

  Barrier barrier(NUM_THREADS);
  //std::mutex mutex_labels_;
  std::vector<std::thread*> threads(NUM_THREADS);
  std::chrono::time_point<std::chrono::system_clock> start, end;

  for (int threadID = 1; threadID < NUM_THREADS; ++threadID) {
    threads[threadID] = new std::thread([&](int id, int numBarrier) {
      reallocatedEdges[id] = new std::vector<Edge>(batchsPerThread[id]);
      barrier.wait();
      if (id == 0) start = std::chrono::system_clock::now();

      for (int i = 0; i < numOfQueries; ++i) {
        // Init labels
        for (int lid = id; lid < NUM_OF_VERTICES; lid += NUM_THREADS) {
          labels[lid].store(imax);
        }
        barrier.wait();
        if (id == 0) labels[i].store(departureTime);
        barrier.wait();
        // Get source vertex
        //auto edgeItr =  batchsPerThread[id].begin();
        auto edgeItr = reallocatedEdges[id]->begin();
        for (int barrierCounter = 0; barrierCounter < numBarrier; ++barrierCounter) {
          while (edgeItr->departure => 0) {
            if (edgeItr->departure >= labels[edgeItr->fromIndex].load()) {
              // CAS based
              int value = labels[edgeItr->toIndex];
              while (edgeItr->arrival < value &&
                  !(labels[edgeItr->toIndex].compare_exchange_weak(value, edgeItr->arrival)));
              // Lock based
              //std::lock_guard<std::mutex> lock(mutex_labels_);
              //if (edgeItr->arrival < labels[edgeItr->toIndex].load()) {
              //  labels[edgeItr->toIndex].store(edgeItr->arrival);
              //}
            }
            ++edgeItr;
          }
          if (edgeItr->departure == -2) { break; }
          barrier.wait();
          ++edgeItr;
        }
//        if (printResult) {
//          if (id == 0) {
//            int numReachable = 0;
//            for (int i = 0; i < NUM_OF_VERTICES; i++) {
//              if (labels[i].load() != imax) {
//                numReachable++;
//              }
//            }
//            std::cout << "Number of reachable vertices: " << numReachable << std::endl;
//          }
//          barrier.wait();
//        }
      }

      if (id == 0) end = std::chrono::system_clock::now();
      delete reallocatedEdges[id];
    }, threadID, batchSizes.size());
  }

  {
    int id = 0;
    int numBarrier = batchSizes.size();
    reallocatedEdges[id] = new std::vector<Edge>(batchsPerThread[id]);
    barrier.wait();
    if (id == 0) start = std::chrono::system_clock::now();

    for (int i = 0; i < numOfQueries; ++i) {
      // Init labels
      for (int lid = id; lid < NUM_OF_VERTICES; lid += NUM_THREADS) {
        labels[lid].store(imax);
      }
      barrier.wait();
      if (id == 0) labels[i].store(departureTime);
      barrier.wait();
      // Get source vertex
      //auto edgeItr =  batchsPerThread[id].begin();
      auto edgeItr = reallocatedEdges[id]->begin();
      for (int barrierCounter = 0; barrierCounter < numBarrier; ++barrierCounter) {
        while (edgeItr->departure > 0) {
          if (edgeItr->departure >= labels[edgeItr->fromIndex].load()) {
            // CAS based
            int value = labels[edgeItr->toIndex];
            while (edgeItr->arrival < value &&
                   !(labels[edgeItr->toIndex].compare_exchange_weak(value, edgeItr->arrival)));
            // Lock based
            //std::lock_guard<std::mutex> lock(mutex_labels_);
            //if (edgeItr->arrival < labels[edgeItr->toIndex].load()) {
            //  labels[edgeItr->toIndex].store(edgeItr->arrival);
            //}
          }
          ++edgeItr;
        }
        if (edgeItr->departure == -2) { break; }
        barrier.wait();
        ++edgeItr;
      }
    }
    if (id == 0) end = std::chrono::system_clock::now();
    delete reallocatedEdges[id];
  }

  for (int threadID = 1; threadID < NUM_THREADS; ++threadID) {
    threads[threadID]->join();
    delete threads[threadID];
    threads[threadID] = NULL;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << std::endl << "Parallel running time: " << elapsed.count() << std::endl;
  std::cout << "Optimized, " << NUM_THREADS << ", " << elapsed.count() << std::endl;
  return 0;
}
