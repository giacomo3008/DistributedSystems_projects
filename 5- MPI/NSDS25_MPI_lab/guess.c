#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>
#include <limits.h>

int rank;
int num_procs;

const int num_rounds = 10;

const int min_num = 1;
const int max_num = 1000;

// Array, one element per process
// The leader board, instantiated and used in process 0
int *leaderboard = NULL;

// Array, one element per process
// The array of number selected in the current round
int *selected_numbers = NULL;

// The leader for the current round
int leader = 0;

// Allocate dynamic variables
void allocate_vars()
{
  selected_numbers = (int *)malloc(sizeof(int) * num_procs);
  leaderboard = (int *)calloc(num_procs, sizeof(int));
  leaderboard[0] = 1;
}

// Deallocate dynamic variables
void free_vars()
{
  free(selected_numbers);
  free(leaderboard);
}

// Select a random number between min_num and max_num
int select_number()
{
  return min_num + rand() % (max_num - min_num + 1);
}

// Function used to communicate the selected number to the leader
void send_num_to_leader(int num)
{
  MPI_Gather(
      &num,
      1,
      MPI_INT,
      selected_numbers,
      1,
      MPI_INT,
      leader,
      MPI_COMM_WORLD);
}

// Compute the winner (-1 if there is no winner)
// Function invoked by the leader only
int compute_winner(int number_to_guess)
{
  int min = INT_MAX;
  int winner = -1;
  for (int i = 0; i < num_procs; i++)
  {
    int diff = abs(number_to_guess - selected_numbers[i]);
    if (diff < min)
    {
      winner = i;
      min = diff;
    }
    else if (diff == min)
    {
      winner = -1;
    }
  }
  return winner;
}

// Function used to communicate the winner to everybody
void send_winner(int *winner)
{
  MPI_Bcast(
      winner,
      1,
      MPI_INT,
      leader,
      MPI_COMM_WORLD);
}

// Update leader
void update_leader(int winner)
{
  if (winner != -1)
  {
    leader = winner;
  }
}

// Update leaderboard (invoked by process 0 only)
void update_leaderboard(int winner)
{
  if (winner != -1)
  {
    leaderboard[winner]++;
  }
}

// Print the leaderboard
void print_leaderboard(int round, int winner)
{
  printf("\n* Round %d *\n", round);
  if (winner == -1)
  {
    printf("Winner: nessuno (pareggio, leader invariato: P%d)\n", leader);
  }
  else
  {
    printf("Winner: P%d\n", winner);
  }

  printf("Leaderboard\n");
  for (int i = 0; i < num_procs; i++)
  {
    printf("P%d:\t%d\n", i, leaderboard[i]);
  }
}

int main(int argc, char **argv)
{
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  srand(time(NULL) + rank);

  allocate_vars();

  for (int round = 0; round < num_rounds; round++)
  {
    int selected_number = select_number();
    send_num_to_leader(selected_number);

    int winner;
    if (rank == leader)
    {
      int num_to_guess = select_number();
      printf("leader%d sceglie il numero: %d\n", rank, num_to_guess);
      winner = compute_winner(num_to_guess);
    }
    send_winner(&winner);
    update_leader(winner);

    if (rank == 0)
    {
      update_leaderboard(winner);
      print_leaderboard(round, winner);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  free_vars();

  MPI_Finalize();
}
