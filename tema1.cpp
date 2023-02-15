#include <bits/stdc++.h>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

// the struct for each thread that enters the thread function
struct Thread_Arg {
    // basically a matrix of sets in which each mapper puts the values
    vector < vector < set <int> > > *mappers;
    int thread_id;
    int number_of_mappers;
    int number_of_reducers;
    void *file_queue;   // the queue for the files left to be processed
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
};

// the function that returns base^exponent
// the same as pow but iteratively
unsigned long long compute_number(int base, int exponent) {
    unsigned long long x = 1;
    for (int i = 0; i < exponent; i++) {
        x = x * base;
    }
    return x;
}

// the function that verifies if a number is a perfect
// power by applying a binary search on the interval
// from 1 to (number/ exponent)
// if such a perfect power is found, the function
// returns 1, 0 otherwise
int verify_perfect_power(int number, int exponent) {
    // base ^ exponent = number
    int low = 1, high, mid;

    if (number == 1) {  // 1 is always a perfect power
        high = number;
    } else {
        high = number / exponent;
    }

    while(low <= high) {
        mid = (low + high) / 2;
        unsigned long long x = compute_number(mid, exponent);
        if (x == number) {
            return 1;
        }
        else if (x > number)  {
            high = mid - 1;
        }  
        else {
            low = mid + 1;
        }   
    }
    return 0;
}

// the thread function that contains both the mapping
// and the reducing process, making sure that all the
// mappers finish before the reducers start producing
// the final results
void *map_reduce(void *arg) {
    struct Thread_Arg *t = (struct Thread_Arg *)arg;
    queue<string> *file_queue = (queue<string> *)t->file_queue;

    // the mapping process

    // making sure the current thread is a mapper
    if (t->thread_id < t->number_of_mappers) {
        
        // the threads dinamically get
        // each file to process from a queue
        while (!file_queue->empty()) {
            string my_file;  // the current file to process
            my_file.clear(); // making sure the string is empty

            // accessing the queue must be done
            // one by one to avoid race condition
            int m = pthread_mutex_lock(t->mutex);
	        if (m) {
		        printf("Error locking the mutex\n");
		        exit(-1);
	        }

            // getting the file and 
            // removing it from the queue
            if (!file_queue->empty()) {
                my_file = file_queue->front();
                file_queue->pop();
            }

	        m = pthread_mutex_unlock(t->mutex);
	        if (m) {
		        printf("Error unlocking the mutex\n");
		        exit(-1);
	        }
            
            // again, making sure that the thread indeed got a file
            if (!my_file.empty()) {

                // opening the file and reading its content
                ifstream f;
                f.open(my_file);

                if (f.is_open()) {
                    string string;
                    // the first line is the number of numbers to process
                    f >> string;
                
                    while (f >> string) {
                        int n = stoi(string);   // take each number
                        for (int j = 0; j < t->number_of_reducers; j++) {
                            // if it is a perfect power
                            if (verify_perfect_power(n, j + 2)) {
                                // it inserts it in the matrix of sets
                                (t->mappers)->at(t->thread_id).at(j).insert(n);
                            }
                        }
                    }
                }
                f.close();  // closing the file
            }
        }
    }

    // the barrier to make sure all of the mappers finish
    // processing the files before the reducers can begin
    pthread_barrier_wait(t->barrier);

    // the reducing process

    // making sure the current thread is a reducer
    if (t->thread_id < t->number_of_reducers) {
        // each reducer computes a set
        // from all the data from te mappers
        set<int> s;
        // just going through the matrix, getting each
        // subset and inserting it into the set declared before
        for (int i = 0; i < t->number_of_mappers; i++) {
            set<int>::iterator itr;
            for (itr = (t->mappers)->at(i).at(t->thread_id).begin(); 
                itr != (t->mappers)->at(i).at(t->thread_id).end(); itr++) {
                s.insert(*itr);
            }
        }

        // building the string that represents the name of the
        // file in which the reducer must write the size of its set
        string out_file = "out";
        out_file += to_string(t->thread_id + 2);
        out_file += ".txt";

        // opening the file, writing the result and closing the file
        ofstream WriteFile(out_file);
        WriteFile << s.size();
        WriteFile.close();
    }

  	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	if (argc < 4) {
        cout << "This is how you should run the program: ";
        cout << "./tema1 <no_mappers> <no_reducers> <input_file>" << endl;
        exit(-1);
    }

    int n_mappers = atoi(argv[1]);   // the number of mappers
    int n_reducers = atoi(argv[2]);  // the number of reducers
    char *input_file = argv[3];

    // the number of threads is chosen by the maximum value
    // between the number of mappers and the number of reducers
    int threads_number = n_mappers;
    if (n_mappers < n_reducers) threads_number = n_reducers;

    pthread_t threads[threads_number];
  	void *status;
    int verify_succes; // for the functions that create and join the threads

    // opening the file given and reading the first line
    ifstream ReadFiles(input_file);
    string n_of_files;
    getline(ReadFiles, n_of_files);

    // reading the names of the files to be processed
    // and inserting them into a queue
    queue<string> file_queue;
    string file_name;
    while (getline(ReadFiles, file_name)) {
        file_queue.push(file_name);
    }

    pthread_mutex_t *mutex;
    mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	int m = pthread_mutex_init(mutex, NULL);
	if (m) {
		printf("Error initialising the mutex\n");
		exit(-1);
	}

    pthread_barrier_t *barrier;
    barrier = (pthread_barrier_t *)malloc(sizeof(pthread_barrier_t));
	int b = pthread_barrier_init(barrier, NULL, threads_number);
	if (b) {
		printf("Error initialising the barrier\n");
		exit(-1);
	}

    // the matrix of sets in which the
    // mappers will compute their results
    vector < vector < set <int> > > mappers(n_mappers, 
                        vector < set<int> > (n_reducers));

    Thread_Arg *t;

    for (int i = 0; i < threads_number; i++) { // for each thread
        t = (struct Thread_Arg *)malloc(sizeof(struct Thread_Arg));
        // assigning each field of the struct
        t->thread_id = i;
        t->number_of_mappers = n_mappers;
        t->number_of_reducers = n_reducers;
        t->barrier = barrier;
        t->mutex = mutex;
        t->mappers = &mappers;
        t->file_queue = (void *)&file_queue;

        // creating the thread
        verify_succes = pthread_create(&threads[i], NULL, map_reduce, (void *)t);
		if (verify_succes) {
	  		printf("Error in creating thread %d\n", i);
	  		exit(-1);
		}
    }

    // joining the threads
    for (int i = 0; i < threads_number; i++) {
		verify_succes = pthread_join(threads[i], &status);
		if (verify_succes) {
	  		printf("Error waiting for thread %d\n", i);
	  		exit(-1);
		}
  	}

    ReadFiles.close();  // closing the file
    
    // destroying the mutex and the barrier
    m = pthread_mutex_destroy(mutex);
	if (m) {
		printf("Error destroying the mutex\n");
		exit(-1);
	}
    b = pthread_barrier_destroy(barrier);
	if (b) {
		printf("Error destroying the barrier\n");
		exit(-1);
	}

  	pthread_exit(NULL);
    return(0);
}