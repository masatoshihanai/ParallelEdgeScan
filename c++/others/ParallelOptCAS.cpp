#include <atomic>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <limits>
#include <algorithm>
#include <string>
#include <chrono>
#include <vector>
#include <list>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <sys/time.h>

struct Edge {
 public:
  Edge(int fromIndex_, int toIndex_,
       int departure_, int arrival_)
      : fromIndex(fromIndex_), toIndex(toIndex_),
        departure(departure_), arrival(arrival_) {};
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
std::string fileName = "out.wikipedia-growth";
const int NUM_OF_VERTICES = 1870709; // Scale 21
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

int NUM_THREADS = std::thread::hardware_concurrency();
int THREASHOLD = NUM_THREADS * 20;
int numOfQueries = 100;
int departureTime = 0;
bool printResult = true;

int main(int argc, char** argv) {
  std::vector<Edge> edges;
  /*
  ********************************************************************************************************
  Read data file
  ********************************************************************************************************
  */
  std::cerr << "Reading data file..." << std::endl;

  // TODO from args
  std::ifstream inputEdge("../data/" + fileName + ".edges");
  std::string edgeLine;
  while (std::getline(inputEdge, edgeLine)) {
    std::istringstream iss(edgeLine);

    int fromIndex; int toIndex;
    int departure; int arrival;
    if (!(iss >> fromIndex >> toIndex >> departure >> arrival)) {
      break;
    }
    edges.push_back(Edge(fromIndex, toIndex, departure, arrival));
  }
  inputEdge.close();

  /*
  ********************************************************************************************************
  Get batches information
  ********************************************************************************************************
  */
  std::cerr << "Reading meta file..." << std::endl;

  // TODO from args
  std::vector<int> batchSizes;
  std::ifstream inputBatchSize("../data/" + fileName + ".meta");
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
  for (int batchID = 0; batchID < batchSizes.size(); ++batchID) {
    // insert edges
    for (int edgeCounter = 0; edgeCounter < batchSizes[batchID]; ++edgeCounter) {
      batchsPerThread[edgeCounter % NUM_THREADS].push_back(*it);
      ++it;
    }
    // insert end point of the batch
    for (int threadID = 0; threadID < NUM_THREADS; ++threadID) {
      batchsPerThread[threadID].push_back(Edge(-1,-1,-1,-1));
    }
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

  Barrier barrier(NUM_THREADS);
  //std::mutex mutex_labels_;
  std::vector<std::thread*> threads(NUM_THREADS);
  std::chrono::time_point<std::chrono::system_clock> start, end;
  long time_init_label = 0;

  for (int threadID = 0; threadID < NUM_THREADS; ++threadID) {
    threads[threadID] = new std::thread([&](int id, int numBarrier) {
      barrier.wait_start();
      if (id == 0) start = std::chrono::system_clock::now();

      for (int i = 0; i < numOfQueries; ++i) {
        if (id == 0)  {
          struct timeval start_timeval, end_timeval;
          gettimeofday(&start_timeval, NULL);
          for (int j = 0; j < NUM_OF_VERTICES; ++j) {
            labels[j].store(imax);
          }
          labels[i].store(departureTime);
          gettimeofday(&end_timeval, NULL);
          time_init_label += (end_timeval.tv_sec * 1000 + end_timeval.tv_usec / 1000)
                             - (start_timeval.tv_sec * 1000 + start_timeval.tv_usec / 1000);
        }
        barrier.wait();
        // Get source vertex
        auto edgeItr =  batchsPerThread[id].begin();
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
    }, threadID, batchSizes.size());
  }
  barrier.start_threads();

  for (int threadID = 0; threadID < NUM_THREADS; ++threadID) {
    threads[threadID]->join();
    delete threads[threadID];
    threads[threadID] = NULL;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cerr << std::endl << "Parallel running time: " << elapsed.count() << std::endl;
  std::cerr << "Time init label: " << time_init_label << std::endl;
  return 0;
}
