#include <math.h> 
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>

/* Header Starts */

/*
* DYNAMIC ARRAYS IMPLEMENTATION
*/
struct CharList{
	char *vals;
	int size;  
	int next_idx; 
};

struct IntList{
	int *vals;
	int size;  
	int next_idx; 
};

struct CharList *init_CharList(); 
void add_char(struct CharList *l, char c); 
struct IntList *init_IntList(); 
void add_int(struct IntList *l, int i); 
void free_List(struct CharList *cl, struct IntList *il);


/*
* FILTER HELPER FUNCTIONS  
*/
void initiate_sieve(int n); 
void find_prime_factors(int n, int read_fd, int factors_wfd, int nums_len, int limit, int factors_below_sqrt);
int remove_multiples(int *target_multiple, int nums_len, int read_fd, int write_fd);
int process_filter(int n, int new_filter, int write_fd);
void evaluate_filtered(int n, int read_fd, int filters_used);

/*
* READ/WRITE HELPER FUNCTIONS 
*/
int read_next_int(int read_fd);
void write_str(char *data, int write_fd);
void write_int(int i, int write_fd);
void handle_rw_err(int res, int write);

/*
* UTILITY FUNCTIONS
*/
double get_max_filter(int n); 
char *comma_seperate(struct IntList *l);
int is_prime(int n); 
void check_malloc(void *malloc_res);
int usage_err();

/* Header Ends */

typedef struct IntList IntList_t; 
typedef struct CharList CharList_t; 


int main(int argc, char const *argv[]){
	if (argc != 2){
		return usage_err();
	}
	char *str_remains; 
	int n = strtol(argv[1], &str_remains, 10);

	int extras = 0; 
	if (*str_remains != '\0'){ // check if input had non-numeric chars
		extras = 1; 
	}
	if (n < 2 || extras){
		return usage_err();
	}
	initiate_sieve(n); 
	return 0;		
}					

void initiate_sieve(int n){
	int factor_pipe[2];
	pipe(factor_pipe); 

	int start_process = fork(); 

	if (start_process == 0){
		int nums_pipe[2];  
		pipe(nums_pipe); 
		int limit = get_max_filter(n);
		for (int i = 2; i < n; i++){
			write_int(i, nums_pipe[1]); 
		}
		close(nums_pipe[1]);
		close(factor_pipe[0]); 
		find_prime_factors(n, nums_pipe[0], factor_pipe[1], n-1, limit, 0); 
		close(factor_pipe[1]); 
		close(nums_pipe[0]); 
	}else if (start_process > 0){
		int status; 
		if (wait(&status) != -1){
			if (WIFEXITED(status)){
				int exit_status = WEXITSTATUS(status); 		
				close(factor_pipe[1]); 
				evaluate_filtered(n, factor_pipe[0], exit_status);
				close(factor_pipe[0]);
				printf("Number of filters = %d\n", exit_status-1);
			}else{
				perror("abnormal exit\n");
				exit(1); 
			}
		}else{
			perror("wait"); 
			exit(1); 
		}
	}else{
		perror("fork"); 
		exit(1); 
	}

}

/*
* Finds the prime factors of n and stores it in factors_wfd which must be a 
* write file descriptor. read_fd must allow reading from a pipe that contains
* numbers from 2 to n. Set limit to the point at which recursion should stop, 
* (use get_max_filter function). Set factors_below_sqrt to 0. 
*/						
void find_prime_factors(int n, int read_fd, int factors_wfd, int nums_len, 
	int limit, int factors_below_sqrt){  

	int filter_process = fork(); 

	if (filter_process == 0){
		// make a pipe to relay the filtered numbers from this process, to the child 
		// of this process 
		int relay_filtered[2]; 
		pipe(relay_filtered);

		// next filter is initially set to 2, and is reset in the remove_multiples function
		int next_filter = 2;
		// remove_multiples writes writes non-multiples of next_filter to the relay_filtered pipe
		int num_factors = remove_multiples(&next_filter, nums_len, read_fd, relay_filtered[1]); 
			
		close(relay_filtered[1]);

		int filter_type = process_filter(n, next_filter, factors_wfd);
		if (filter_type == 2){
			factors_below_sqrt += 1; 
		}

		if (next_filter <= limit && filter_type > 0 && factors_below_sqrt < 2){
			// Recursively create a new process to filter multiples of next_filter from the filtered numbers 
			find_prime_factors(n, relay_filtered[0], factors_wfd, num_factors, limit, factors_below_sqrt); 
		}else if (factors_below_sqrt >= 2){
			// if we're not recursing again due to there being at least 2 factors below sqrt(n), 
			// then write a -1 to the factors pipe to signify an early return ie. that the two 
			// factors in the pipe are not valid results 
			write_int(-1, factors_wfd);
		}

		close(factors_wfd); 
		close(relay_filtered[0]); 
		exit(0); 
	}else if (filter_process > 0){
		int status; 
		if (wait(&status) != -1){
			if (WIFEXITED(status)){
				int exit_status = WEXITSTATUS(status); 
				close(read_fd); 
				close(factors_wfd); 
				exit(exit_status + 1); 

			}else{
				fprintf(stderr, "A child has exited abnormally. Any output is not to be trusted\n");
			}
		}else{
			perror("wait");
			fprintf(stderr, "The wait() system call has failed. Any output that follows is not to be trusted\n");
			exit(1); 
		}
	}else{
		perror("fork"); 
		fprintf(stderr, "The fork() system call has failed. Any output that follows is not to be trusted\n");
		exit(1); 
	}
}


/**
* Given a pointer to an int, next_filter, the amount of numbers
* in a pipe, nums_len, a read file descriptor to that pipe, read_fd, 
* and a write file descriptor to another pipe, write_fd, read all
* the integers using read_fd, and write them to the pipe pointed to
* by write_fd if the integer is not a multiple of next_filter. 
* Return the total numbers written to write_fd. 
*
* NOTE: Upon completion, this function will close read_fd. 
*/
int remove_multiples(int *next_filter, int nums_len, int read_fd, int write_fd){
	int num_factors = 0; 
	if (read_fd != -1){
		*next_filter = read_next_int(read_fd); 
			// Stop at nums_len-1 since we already read first int to set next_filter 
			for (int i = 0; i < nums_len-1; i++){ 
				int next_int = read_next_int(read_fd);
				int is_factor = ((next_int % (*next_filter)) == 0); 
				if (!is_factor){
					num_factors += 1; 
					write_int(next_int, write_fd);
				} 
			}
			close(read_fd); 
		}
		return num_factors;
}

/*
* Given the number inputted by the user, n, and the value of
* the next filter, new_filter, and a write file descriptor to
* a pipe to hold prime factors of n, write new_filter to
* with write_fd if new_filter is a factor of n. 
* Returns: 
* 0 to signal an error (new_filter == 0), 
* 2 if new_filter is a factor of n and is less than sqrt(n), 
* 1 otherwise. 
*/
int process_filter(int n, int new_filter, int write_fd){
	if (new_filter == 0){
		return 0; 
	}
	if (n % new_filter == 0){
		// if new_filter is a factor of n, store it in the factor pipe
		write_int(new_filter, write_fd); 
		double sqrt_n = sqrt(n); 
		if (new_filter < sqrt_n){
			return 2; 
		}
	}
	return 1; 
}

/*
* Given an int representing whether or not the program exited early, 
* and an IntList of factors found for n, return an int array with indexes: 
* 0 = whether or not n is a product of primes
* 1 = the first factor
* 2 = the second factor 
*/
int *is_prod_of_primes(IntList_t *factors, int n, int early_exit){
	int prod_of_primes = 0, factor_1 = -1, factor_2 = -1; 
	IntList_t *results_list = init_IntList(); 
	if (early_exit){
		prod_of_primes = 0; 
	}else if(factors->next_idx == 1){
		// n might be the prod. of 2 primes
		int factor_val = factors->vals[0]; 
		if ((factor_val * factor_val) == n){
			prod_of_primes = 1; 
			factor_1 = factor_val, factor_2 = factor_val;
		}else{
			// since we stop at sqrt(n), it might be possible that we missed 
			// the other prime factor. check to see if we did 
			int q = n / factor_val;
			if (is_prime(q)){
				prod_of_primes = 1, factor_1 = factor_val, factor_2 = q; 
			} 
		}
	}else if (factors->next_idx == 2){
		prod_of_primes = 1; 
		factor_1 = factors->vals[0], factor_2 = factors->vals[1]; 				
	}else if(factors->next_idx == 0){
		prod_of_primes = -1; 
	}
	add_int(results_list, prod_of_primes); 
	add_int(results_list, factor_1); 
	add_int(results_list, factor_2); 
	int *data_arr = results_list->vals; 
	free(results_list); 
	return data_arr; 
}

/*
* Given the number inputted by the user, n, 
* and a read file descriptor to a pipe containing
* prime factors of n, and the number of filters used, 
* print the appropriate output to stdout.  
*/
void evaluate_filtered(int n, int read_fd, int filters_used){
	int early_exit = 0; 
	int next_factor = read_next_int(read_fd);
	IntList_t *factors = init_IntList();

	// read all the integers from the factor pipe into the factors IntList
	while (next_factor != 0){
		if (next_factor == -1){
			early_exit = 1; 
		}
		add_int(factors, next_factor); 
		next_factor = read_next_int(read_fd);
	}
	int *data_arr = is_prod_of_primes(factors, n, early_exit);
	int prod_of_primes = data_arr[0], factor_1 = data_arr[1], factor_2 = data_arr[2];  

	if (prod_of_primes == 1){
		printf("%d %d %d\n", n, factor_1, factor_2);
	}else if (prod_of_primes == -1){
		printf("%d is prime\n", n);
	}else{
		printf("%d is not the product of two primes\n", n);
	}
	free_List(NULL, factors); 
	free(data_arr); 
}


/*
* DYNAMIC ARRAYS IMPLEMENTATION
*/


/*
* Return a dynamically allocated CharList with initialized values. 
*/
CharList_t *init_CharList(){
	CharList_t *char_list = malloc(sizeof(CharList_t));
	check_malloc(char_list);
	char_list->vals = NULL; 
	char_list->size = 0; 
	char_list->next_idx = 0; 
	return char_list;
}

/*
* Given an char, c, and a CharList, l, add c to l. 
*/
void add_char(CharList_t *l, char c){
	if (l->vals == NULL){
		l->vals = malloc(sizeof(char)*2);
		check_malloc(l->vals);
		l->next_idx = 0; 
		l->size = 2;  
	}
	if (l->next_idx == l->size){
		char *new_arr = malloc(sizeof(char) * ((l->size)*2));
		for (int i = 0; i < l->size; i++){
			new_arr[i] = l->vals[i]; 
		}
		free(l->vals); 
		l->vals = new_arr; 
		l->size = (l->size)*2;
	}
	l->vals[l->next_idx] = c; 
	l->next_idx += 1; 
}

/**
* Return a dynamically allocated IntList with initialized values. 
*/
IntList_t *init_IntList(){
	IntList_t *int_list = malloc(sizeof(IntList_t));
	check_malloc(int_list);
	int_list->vals = NULL; 
	int_list->size = 0; 
	int_list->next_idx = 0; 
	return int_list; 
}

/*
* Given an integer, i, and an IntList, l, add i to l. 
*/
void add_int(IntList_t *l, int i){
	if (l->vals == NULL){
		l->vals = malloc(sizeof(int)*2);
		check_malloc(l->vals);
		l->next_idx = 0; 
		l->size = 2;  
	}
	if (l->next_idx == l->size){
		int *new_arr = malloc(sizeof(int) * ((l->size)*2));
		check_malloc(new_arr);
		for (int j = 0; j < l->size; j++){
			new_arr[j] = l->vals[j]; 
		}
		free(l->vals); 
		l->vals = new_arr; 
		l->size = (l->size)*2;
	}
	l->vals[l->next_idx] = i; 
	l->next_idx += 1; 
}

/*
* Given a possibly null CharList and a possibly null IntList,
* free the memory allocated by each (if any). 
*/
void free_List(CharList_t *cl, IntList_t *il){
	if (cl != NULL){
		if (cl->vals != NULL){
			free(cl->vals); 
		}
		free(cl); 
	}
	if (il != NULL){
		if (il->vals != NULL){
			free(il->vals); 
		}
		free(il); 
	}
}


/*
* READ/WRITE HELPER FUNCTIONS 
*/


/*
* Get the next integer pointed to from the pipeline pointed to by
* read_fd, and return it. If there are no integers in the pipe 
* or there is an error reading from the pipe, return 0; 
*/
int read_next_int(int read_fd){

	// CharList to hold the digits read 
	CharList_t *cl = init_CharList(); 

	int end = 0; 
	while (!end){
		char c; 
		int r = read(read_fd, &c, sizeof(char));
		handle_rw_err(r, 0); 
		if (c == ',' || r == 0 || r == -1){
			// comma found. ie. end of the first number OR error occured 
			end = 1;
		}else{
			add_char(cl, c); 
		}
	}
	add_char(cl, '\0');
	int num = strtol(cl->vals, NULL, 10); 
	free_List(cl, NULL); 
	return num; 
}

/*
* Given a pointer to a string, str, write the string in the 
* pipe pointed to by the file descriptor write_fd. 
*/
void write_str(char *str, int write_fd){
	int data_size = strlen(str); 
	for (int i = 0; i < data_size; i++){
		int write_res = write(write_fd, &(str[i]), sizeof(char));
		handle_rw_err(write_res, 1);  
	}
}

/*
* Given an integer, i, and a write file descriptor, write_fd, 
* write the integer followed by a comma to the pipe pointed 
* to by write_fd. 
*/
void write_int(int i, int write_fd){
	IntList_t *data = init_IntList(); 
	add_int(data, i); 
	char *data_str = comma_seperate(data); 
	write_str(data_str, write_fd); 
	free_List(NULL, data); 	
}

/*
* Given a result, res, from a read or write system call, and an int
* communicating whether res is from a write call (1) or a read call (0),
* check the result for errors, and take action if it is an error. 
*/
void handle_rw_err(int res, int write){
	char *err = "read"; 
	if (write){
		err = "write"; 
	}
	char *err_msg = "Warning: Error encountered. Any output that follows is not to be trusted\n"; 
	if (res == -1){
		if (write){
			if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
				perror("signal");
				fprintf(stderr, "%s", err_msg);
				exit(1);
			}
		}
		perror(err); 
		fprintf(stderr, "%s", err_msg);
		exit(1); 
	}
}


/*
* UTILITY FUNCTIONS
*/


/*
* Given an integer, n, return the maximum numbers of filters to be created
* for n. 
*/
double get_max_filter(int n){
	double sqrt_dbl = sqrt(n); 
	int sqrt_int = (int) sqrt_dbl; 
	double max_filter = sqrt_dbl; // create filters til m is >= sqrt(n) 
	// If n is a perfect square, do not include the square root (ie. stop right before sqrt(n))
	if (sqrt_int == sqrt_dbl){
		max_filter -= 1;
	}
	return max_filter; 
}

/**
Given an IntList, l, return a string containing every integer in l, seperated
by commas. The last character of every return will always be a comma. 
*/
char *comma_seperate(IntList_t *l){
	CharList_t *int_chars = init_CharList(); 
	for (int i = 0; i < l->next_idx; i++){
		char num_str[10]; // INT_MAX = 10 digits
		sprintf(num_str, "%d", (l->vals)[i]);
		for (int i = 0; i < strlen(num_str); i++){
			add_char(int_chars, num_str[i]); 
		}
		add_char(int_chars, ','); 
	}
	add_char(int_chars, '\0'); 
	char *comma_seperated = int_chars->vals;
	free(int_chars); 
	return comma_seperated;   
}

/**
* malloc_res should a pointer returned by a malloc sys call.
*/
void check_malloc(void *malloc_res){
	if (malloc_res == NULL){
		fprintf(stderr, "A process was unable to allocate memory on the heap. Any output that follows is not to be trusted\n");
		exit(1);
	}
}

/**
* Return 1 iff n is prime and 0 otherwise
*/
int is_prime(int n){
	int limit = ceil(sqrt(n)); 
	for (int i = 2; i <= limit; i++){
		if (n % i == 0){
			return 0; 
		}
	}
	return 1; 
}

/*
* Print a message telling the user that they've entered an invalid input,
* and return 1; 
*/
int usage_err(){
	fprintf(stderr, "Usage:\n\tpfact n\n");
	return 1; 
}