#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <time.h>





typedef struct Edge {
	int fromIndex;
	int toIndex;
	int departure;
	int arrival;
} Edge;



/*
********************************************************************************************************
Dynamic integer array
********************************************************************************************************
*/
typedef struct IntArray {
	int* array;
	size_t used;
	size_t size;
} IntArray;

void initIntArray(IntArray* a, size_t initialSize) {
	a->array = (int*)malloc(initialSize* sizeof(int));
	a->used = 0;
	a->size = initialSize;
}

void insertIntArray(IntArray* a, int element) {
	if (a->used == a->size) {
  		a->size *= 2;
		a->array = (int *)realloc(a->array, a->size * sizeof(int));
	}
	a->array[a->used++] = element;
}

void freeIntArray(IntArray* a) {
	free(a->array);
	a->array = NULL;
	a->used = a->size = 0;
}







/*
********************************************************************************************************
Dynamic edge array
********************************************************************************************************
*/
typedef struct EdgeArray {
	Edge* array;
	size_t used;
	size_t size;
} EdgeArray;

void initEdgeArray(EdgeArray* a, size_t initialSize) {
	a->array = (Edge *)malloc(initialSize * sizeof(Edge));
	a->used = 0;
	a->size = initialSize;

	// Initialize all values of the array to 0
	for(int i = 0; i<initialSize; i++) {
		memset(&a->array[i],0,sizeof(Edge));
	}
}

void insertEdgeArray(EdgeArray* a, Edge element) {
	if (a->used == a->size) {
		a->size *= 2;
		a->array = (Edge*)realloc(a->array, a->size * sizeof(Edge));
	}

	// Copy all fields
    a->array[a->used].fromIndex = element.fromIndex;
    a->array[a->used].toIndex = element.toIndex;
    a->array[a->used].departure = element.departure;
    a->array[a->used].arrival = element.arrival;

    a->used++;
}

void freeEdgeArray(EdgeArray* a) {
	free(a->array);
	a->array = NULL;
	a->used = a->size = 0;
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

	printf("\nCalculating shortest paths...\n");

	int* labels = (int*)malloc(numOfVertices * sizeof(int));
	int departureTime = 0;

	clock_t begin, end;
	double time_spent;
	begin = clock();

	for (int r = 0; r < numOfRuns; r++) {

		// Initialize labels
		for (int i = 0; i < numOfVertices; i++) {
			labels[i] = INT_MAX;
		}

		labels[r] = departureTime;

		int start = 0;
		for (int i = 0; i < numOfBatches; i++) {
			int size = batchSizes[i];

			for (int j = start; j < start + size; j++) {
				Edge e = edges[j];
				int fromIndex = e.fromIndex;
				int toIndex = e.toIndex;
				int departure = e.departure;
				int arrival = e.arrival;

				if (departure >= labels[fromIndex]) {

					if (arrival < labels[toIndex]) {

						labels[toIndex] = arrival;

					}

				}

			}

			start = start + size;

		}

		if (printResult) {
			int numOfReachable = 0;
			for (int i = 0; i < numOfVertices; i++) {
				if (labels[i] != INT_MAX) {
					numOfReachable++;
				}
			}
			printf("Number of reachable vertices: %d...\n", numOfReachable);

		}

	}

	end = clock();
	time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Program running time: %f\n", time_spent);

	

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







 

