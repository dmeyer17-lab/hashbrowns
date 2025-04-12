#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap.
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity)
{

  ts_hashmap_t *map = (ts_hashmap_t *)malloc(sizeof(ts_hashmap_t));
  if (map == NULL)
    return NULL;

  map->table = (ts_entry_t **)calloc(capacity, sizeof(ts_entry_t *));
  if (map->table == NULL)
  {
    free(map);
    return NULL;
  }

  map->capacity = capacity;
  map->numOps = 0;
  map->size = 0;

  map->locks = (pthread_mutex_t *)malloc(capacity * sizeof(pthread_mutex_t));
  if (map->locks == NULL)
  {
    free(map->table);
    free(map);
    return NULL;
  }

  for (int i = 0; i < capacity; i++)
  {
    pthread_mutex_init(&map->locks[i], NULL);
  }

  pthread_mutex_init(&map->numOps_mutex, NULL);
  pthread_mutex_init(&map->size_mutex, NULL);

  map->gets = 0;
  map->put_adds = 0;
  map->put_reps = 0;
  map->del_succ = 0;
  map->del_fail = 0;
  return map;
}
/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key)
{
  unsigned int index = (unsigned int)key % (unsigned int)map->capacity;
  pthread_mutex_lock(&map->locks[index]);

  ts_entry_t *entry = map->table[index];
  int value = INT_MAX;

  while (entry != NULL)
  {
    if (entry->key == key)
    {
      value = entry->value;
      break;
    }
    entry = entry->next;
  }

  pthread_mutex_unlock(&map->locks[index]);

  pthread_mutex_lock(&map->numOps_mutex);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_mutex);

  pthread_mutex_lock(&map->numOps_mutex);
  map->gets++;
  pthread_mutex_unlock(&map->numOps_mutex);
  return value;
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value)
{
  unsigned int index = (unsigned int)key % (unsigned int)map->capacity;
  pthread_mutex_lock(&map->locks[index]);

  ts_entry_t *entry = map->table[index];
  ts_entry_t *prev = NULL;
  int old_val = INT_MAX;
  int found = 0;

  while (entry != NULL)
  {
    if (entry->key == key)
    {
      old_val = entry->value;
      entry->value = value;
      found = 1;
      break;
    }
    prev = entry;
    entry = entry->next;
  }

  if (!found)
  {
    ts_entry_t *new_entry = (ts_entry_t *)malloc(sizeof(ts_entry_t));
    if (new_entry == NULL)
    {
      pthread_mutex_unlock(&map->locks[index]);
      pthread_mutex_lock(&map->numOps_mutex);
      map->numOps++;
      pthread_mutex_unlock(&map->numOps_mutex);
      return INT_MAX;
    }
    new_entry->key = key;
    new_entry->value = value;
    new_entry->next = NULL;

    if (prev == NULL)
    {
      map->table[index] = new_entry;
    }
    else
    {
      prev->next = new_entry;
    }

    pthread_mutex_lock(&map->size_mutex);
    map->size++;
    pthread_mutex_unlock(&map->size_mutex);
  }

  pthread_mutex_unlock(&map->locks[index]);

  pthread_mutex_lock(&map->numOps_mutex);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_mutex);

  pthread_mutex_lock(&map->numOps_mutex);
  if (found)
    map->put_reps++;
  else
    map->put_adds++;
  pthread_mutex_unlock(&map->numOps_mutex);
  return old_val;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key)
{
  unsigned int index = (unsigned int)key % (unsigned int)map->capacity;
  pthread_mutex_lock(&map->locks[index]);

  ts_entry_t *entry = map->table[index];
  ts_entry_t *prev = NULL;
  int value = INT_MAX;
  int found = 0;

  while (entry != NULL)
  {
    if (entry->key == key)
    {
      value = entry->value;
      if (prev == NULL)
      {
        map->table[index] = entry->next;
      }
      else
      {
        prev->next = entry->next;
      }
      free(entry);
      found = 1;
      break;
    }
    prev = entry;
    entry = entry->next;
  }

  if (found)
  {
    pthread_mutex_lock(&map->size_mutex);
    map->size--;
    pthread_mutex_unlock(&map->size_mutex);
  }

  pthread_mutex_unlock(&map->locks[index]);

  pthread_mutex_lock(&map->numOps_mutex);
  map->numOps++;
  pthread_mutex_unlock(&map->numOps_mutex);

  pthread_mutex_lock(&map->numOps_mutex);
  if (found)
    map->del_succ++;
  else
    map->del_fail++;
  pthread_mutex_unlock(&map->numOps_mutex);
  return value;
}

/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map)
{
  for (int i = 0; i < map->capacity; i++)
  {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map)
{
  if (map == NULL)
    return;

  // profiling run
  printf("-----------------------------------------------\n");
  printf("Profiling Run:\n");
  printf("  gets      = %d (%.0f%%)\n", map->gets, (map->gets * 100.0) / map->numOps);
  printf("  put adds  = %d (%.0f%%)\n", map->put_adds, (map->put_adds * 100.0) / map->numOps);
  printf("  put reps  = %d (%.0f%%)\n", map->put_reps, (map->put_reps * 100.0) / map->numOps);
  printf("  del succ  = %d (%.0f%%)\n", map->del_succ, (map->del_succ * 100.0) / map->numOps);
  printf("  del fail  = %d (%.0f%%)\n", map->del_fail, (map->del_fail * 100.0) / map->numOps);
  printf("  total ops = %d\n", map->numOps);
  printf("  map size  = %d\n", map->size);
  printf("-----------------------------------------------\n");

  for (int i = 0; i < map->capacity; i++)
  {
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      ts_entry_t *next = entry->next;
      free(entry);
      entry = next;
    }
  }

  free(map->table);

  for (int i = 0; i < map->capacity; i++)
  {
    pthread_mutex_destroy(&map->locks[i]);
  }
  free(map->locks);

  pthread_mutex_destroy(&map->numOps_mutex);
  pthread_mutex_destroy(&map->size_mutex);

  free(map);
}