#include <bits/stdc++.h>
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>

using namespace std;

struct Thread_Arg {
    vector < vector < set <int> > > *mappers;
    int thread_id;
    int number_of_mappers;
    int number_of_reducers;
    int number_of_files_to_process = 0;
    void *file_queue;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex;
};

unsigned long long compute_number(int base, int exponent, int number) {
    unsigned long long x = 1;
    for(int i = 0; i < exponent; i++) {
        x = x * base;
    }
    return x;
}

int verify_perfect_power(int number, int exponent) {
    // base ^ exponent = number
    int low = 1, high, mid;
    if (number == 1) {
        high = number;
    } else {
        high = number / exponent;
    }
    while(low <= high) {
        mid = (low + high) / 2;
        unsigned long long x = compute_number(mid, exponent, number);
        if(x == number) {
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

void *map_reduce(void *arg) {
    struct Thread_Arg *t = (struct Thread_Arg *)arg;
    queue<string> *file_queue = (queue<string> *)t->file_queue;

    //cout << file_queue->size() << endl;

    //the mapping process
    if (t->thread_id < t->number_of_mappers) {
        // while(!file_queue->empty()) {
        //     cout << file_queue->front() << endl;
        //     file_queue->pop();
        // }

        // for(int i = 0; i < t->number_of_files_to_process; i++) {
        //     cout << "ok" << endl;
        //     //string my_file = v->back();
        //     //cout << my_file << endl;
        //     //v->pop_back();

            // ifstream f;
            // //f.open(my_file);
            // if (f.is_open()) {
            //     string string;
            //     f >> string;
            //     int n_numbers = stoi(string);
                
            //     while (f >> string)
            //     {
            //         int n = stoi(string);
            //         for (int j = 0; j < t->number_of_reducers; j++) {
            //             if (verify_perfect_power(n, j + 2)) {
            //                 (t->mappers)->at(t->thread_id).at(j).insert(n);
            //             }
            //         }
            //     }
            // }
            // f.close();
        // }

        //cout << "Here!" << endl;

        while (!file_queue->empty()) {
            string my_file;
            my_file.clear();

            //cout << my_file << endl;
            //cout << "okb" << endl;
            int m = pthread_mutex_lock(t->mutex);
	        if (m) {
		        printf("Error locking the mutex\n");
		        exit(-1);
	        }
            //cout << "locked!" << endl;

            if(!file_queue->empty()) {
                my_file = file_queue->front();
                file_queue->pop();
                //cout << my_file << endl;
            }

	        m = pthread_mutex_unlock(t->mutex);
	        if (m) {
		        printf("Error unlocking the mutex\n");
		        exit(-1);
	        }
            
            if(!my_file.empty()) {
                ifstream f;
                f.open(my_file);
                if (f.is_open()) {
                    string string;
                    f >> string;
                    int n_numbers = stoi(string);
                
                    while (f >> string) {
                        int n = stoi(string);
                        for (int j = 0; j < t->number_of_reducers; j++) {
                            if (verify_perfect_power(n, j + 2)) {
                                (t->mappers)->at(t->thread_id).at(j).insert(n);
                            }
                        }
                    }
                }
                f.close();
            }
        }
    }

    //barrier
    pthread_barrier_wait(t->barrier);

    //the reducing process
    if (t->thread_id < t->number_of_reducers) {
        set<int> s;
        for (int i = 0; i < t->number_of_mappers; i++) {
            set<int>::iterator itr;
            for(itr = (t->mappers)->at(i).at(t->thread_id).begin(); 
                itr != (t->mappers)->at(i).at(t->thread_id).end(); itr++) {
                s.insert(*itr);
            }
        }
        string out_file = "out";
        out_file += to_string(t->thread_id + 2);
        out_file += ".txt";
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

    int n_mappers = atoi(argv[1]);
    int n_reducers = atoi(argv[2]);
    char *input_file = argv[3];
    int threads_number = n_mappers, verify_succes;

    // the number of threads is chosen by the maximum value
    // between the number of mappers and the number of reducers
    if (n_mappers < n_reducers) threads_number = n_reducers;

    pthread_t threads[threads_number];
  	void *status;
    int current_file = 1;

    ifstream ReadFiles(input_file);

    string n_of_files;
    getline(ReadFiles, n_of_files);
    //int number_of_files = stoi(n_of_files);

    pthread_mutex_t *mutex, *mutex1;
    pthread_barrier_t *barrier;
    vector < vector < set <int> > > mappers(n_mappers, 
                        vector < set<int> > (n_reducers));

    Thread_Arg *t;
    queue<string> file_queue;

    string file_name;
    while(getline(ReadFiles, file_name)) {
        file_queue.push(file_name);
    }

    //cout << file_queue.size() << endl;

    // while(!file_queue.empty()) {
    //     cout << file_queue.front() << endl;
    //     file_queue.pop();
    // }

    mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	int m = pthread_mutex_init(mutex, NULL);
	if (m) {
		printf("Error initialising the mutex\n");
		exit(-1);
	}

    mutex1 = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	m = pthread_mutex_init(mutex1, NULL);
	if (m) {
		printf("Error initialising the mutex\n");
		exit(-1);
	}

    barrier = (pthread_barrier_t *)malloc(sizeof(pthread_barrier_t));
	int b = pthread_barrier_init(barrier, NULL, threads_number);
	if (b) {
		printf("Error initialising the barrier\n");
		exit(-1);
	}

    int start, end;

    for (int i = 0; i < threads_number; i++) {
        t = (struct Thread_Arg *)malloc(sizeof(struct Thread_Arg));
        t->thread_id = i;
        t->number_of_mappers = n_mappers;
        t->number_of_reducers = n_reducers;
        t->barrier = barrier;
        t->mutex = mutex;
        t->mappers = &mappers;
        t->file_queue = (void *)&file_queue;

        verify_succes = pthread_create(&threads[i], NULL, map_reduce, (void *)t);

		if (verify_succes) {
	  		printf("Error in creating thread %d\n", i);
	  		exit(-1);
		}
    }

    for (int i = 0; i < threads_number; i++) {
		verify_succes = pthread_join(threads[i], &status);

		if (verify_succes) {
	  		printf("Error waiting for thread %d\n", i);
	  		exit(-1);
		}
  	}

    ReadFiles.close();
    
    m = pthread_mutex_destroy(mutex);
	if (m) {
		printf("Error destroying the mutex\n");
		exit(-1);
	}
    m = pthread_mutex_destroy(mutex1);
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