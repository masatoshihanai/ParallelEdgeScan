#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>



#define gpuErrchk(ans) { gpuAssert((ans), __FILE__, __LINE__); }
inline void gpuAssert(cudaError_t code, const char *file, int line, bool abort=true) {
   if (code != cudaSuccess) {
      fprintf(stderr,"GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
      if (abort) exit(code);
   }
}



typedef struct Edge {
	int fromIndex;
	int toIndex;
	int departure;
	int arrival;
} Edge;

/*
********************************************************************************************************
Cuda functions
********************************************************************************************************
*/
__global__ 
void edgeScan(int n, unsigned int* labels, Edge* edges, int offset) {

	int i = threadIdx.x + blockDim.x * blockIdx.x;

	if (i < n) {

		Edge e = edges[offset + i];

		int fromIndex = e.fromIndex;
		int toIndex = e.toIndex;
		int departure = e.departure;
		int arrival = e.arrival;

		if (departure >= labels[fromIndex]) {

			atomicMin(&(labels[toIndex]), arrival);

		}

	}

}



__global__
void initLabels(int n, unsigned int* labels) {
	int i = threadIdx.x + blockDim.x * blockIdx.x;
	if (i < n) {
		labels[i] = INT_MAX;
	}
}

__global__
void setSource(unsigned int* labels, int source, int time) {
	labels[source] = time;
}



/*
********************************************************************************************************
Function Definitions
********************************************************************************************************
*/
int stringToInt(char* str, int len);
int compare(const void* a, const void* b);


// berlin
int numOfBatches = 1495;
int numOfEdges = 1209980;
int numOfVertices = 12746;
char file1[] = "e:\\data\\berlin.txt.meta";
char file2[] = "e:\\data\\berlin.txt.edges";

// wikipedia-growth
// int numOfBatches = 700;
// int numOfEdges = 39953145;
// int numOfVertices = 1870709;
// char file1[] = "e:\\data\\out.wikipedia-growth.meta";
// char file2[] = "e:\\data\\out.wikipedia-growth.edges";

// munmun_digg_reply
// int numOfBatches = 30;
// int numOfEdges = 86203;
// int numOfVertices = 30360;
// char file1[] = "e:\\data\\out.munmun_digg_reply.meta";
// char file2[] = "e:\\data\\out.munmun_digg_reply.edges";

// loans
// int numOfBatches = 59;
// int numOfEdges = 3343284;
// int numOfVertices = 89269;
// char file1[] = "e:\\data\\out.prosper-loans.meta";
// char file2[] = "e:\\data\\out.prosper-loans.edges";

// digg-friends
// int numOfBatches = 4623;
// int numOfEdges = 1731653;
// int numOfVertices = 279630;
// char file1[] = "e:\\data\\out.digg-friends.meta";
// char file2[] = "e:\\data\\out.digg-friends.edges";


int numOfRuns = 100;

const int max_thread = 512;

bool printResult = false;

/*
********************************************************************************************************
Entry point
********************************************************************************************************
*/
int main() {

	/*
	********************************************************************************************************
	Read meta file
	********************************************************************************************************
	*/

	printf("\nReading file %s...\n", file1);


	FILE *fp;
	fp = fopen(file1,"r"); // read mode
 
	if(fp == NULL) {
		perror("Error while opening the file.\n");
		exit(EXIT_FAILURE);
	}


	int* batchSizes = (int*)malloc(numOfBatches* sizeof(int));


	char line[256];
	int count = 0;
	while (fgets(line, sizeof(line), fp) != NULL) {
        /* note that fgets don't strip the terminating \n, checking its
           presence would allow to handle lines longer that sizeof(line) */

		int size = stringToInt(line, strlen(line) - 1);
		batchSizes[count] = size;
		count++;

		// printf("New batch size added: %d...\n", size);
	}
    
	
	fclose(fp);



	/*
	********************************************************************************************************
	Read data file
	********************************************************************************************************
	*/

	printf("\nReading file %s...\n", file2);

	fp = fopen(file2,"r"); // read mode
 
	if(fp == NULL) {
		perror("Error while opening the file.\n");
		exit(EXIT_FAILURE);
	}
 

	Edge* edges = (Edge *)malloc(numOfEdges * sizeof(Edge));
	// Initialize all values of the array to 0
	// for(int i = 0; i < numOfEdges; i++) {
	// 	memset(&(edges[i]),0,sizeof(Edge));
	// }

	count = 0;
	while (fgets(line, sizeof(line), fp) != NULL) {
        /* note that fgets don't strip the terminating \n, checking its
           presence would allow to handle lines longer that sizeof(line) */
		
		char copy[256] = "";
		strncpy(copy, line, strlen(line));

		char* parts;
		parts = strtok(copy," ");

		if (parts != NULL) {
			edges[count].fromIndex = stringToInt(parts, strlen(parts));
			parts = strtok(NULL, " ");
			edges[count].toIndex = stringToInt(parts, strlen(parts));
			parts = strtok(NULL, " ");
			edges[count].departure = stringToInt(parts, strlen(parts));
			parts = strtok(NULL, " ");
			edges[count].arrival = stringToInt(parts, strlen(parts) - 1);
		}

		count++;

	}

	fclose(fp);


	/*
	********************************************************************************************************
	Get shortest paths
	********************************************************************************************************
	*/

	// GPU implementation
	unsigned int* labels = (unsigned int*)malloc(numOfVertices * sizeof(unsigned int));

	unsigned int* gpuLabels;
	Edge* gpuEdges;
	

	const int labelSize = numOfVertices * sizeof(unsigned int);
	const int edgeSize = numOfEdges * sizeof(Edge);
	

	gpuErrchk(cudaMalloc((void**)&gpuLabels, labelSize));
	gpuErrchk(cudaMalloc((void**)&gpuEdges, edgeSize));
	

	gpuErrchk(cudaMemcpy(gpuEdges, edges, edgeSize, cudaMemcpyHostToDevice));
	gpuErrchk(cudaDeviceSynchronize());

	clock_t begin, end;
	double time_spent;
	begin = clock();

	for (int i = 0; i < numOfRuns; i++) {
		// Initialize labels

		initLabels<<<numOfVertices / max_thread + 1, max_thread>>>(numOfVertices, gpuLabels);
		gpuErrchk(cudaPeekAtLastError());

		gpuErrchk(cudaDeviceSynchronize());

		// end = clock();
		// time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
		// printf("Up to init label running time: %f\n", time_spent);


		setSource<<<1, 1>>>(gpuLabels, i, 0);
		gpuErrchk(cudaPeekAtLastError());
		gpuErrchk(cudaDeviceSynchronize());

		// end = clock();
		// time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
		// printf("Up to set source running time: %f\n", time_spent);


		// Start edge scan
		int offset = 0;
		for (int j = 0; j < numOfBatches; j++) {
			int size = batchSizes[j];

			edgeScan<<<size / max_thread + 1, max_thread>>>(size, gpuLabels, gpuEdges, offset);
			gpuErrchk(cudaPeekAtLastError());
			
			gpuErrchk(cudaDeviceSynchronize());

			offset = offset + size;
		}

		// end = clock();
		// time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
		// printf("Up to GPU computing running time: %f\n", time_spent);



		gpuErrchk(cudaMemcpy(labels, gpuLabels, labelSize, cudaMemcpyDeviceToHost));
		gpuErrchk(cudaDeviceSynchronize());

		if (printResult) {
			int numOfReachable = 0;
			for (int j = 0; j < numOfVertices; j++) {
				//printf("Label[%d]=%d\n", j, labels[j]);
				if (labels[j] != INT_MAX) {
					numOfReachable++;
				}
			}
			printf("Number of reachable vertices: %d...\n", numOfReachable);

		}

	}

	end = clock();
	time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Total running time: %f\n", time_spent);

	


	// Clean up GPU memory
	cudaFree(gpuLabels);
	cudaFree(gpuEdges);
	//cudaFree(gpuBatchSizes);


	cudaDeviceSynchronize();



	/*
	********************************************************************************************************
	Cleanup
	********************************************************************************************************
	*/


	free(labels);

	free(batchSizes);
	free(edges);

	return 0;
}



int stringToInt(char* str, int len) {

	int size = 0;

	for(int i = 0; i < len; i++) {
		size = size * 10 + (str[i] - '0');

	}
	return size;
}
		

int compare(const void* a, const void* b) {
     int int_a = * ( (int*) a );
     int int_b = * ( (int*) b );

     if ( int_a == int_b ) return 0;
     else if ( int_a < int_b ) return -1;
     else return 1;
}







 

