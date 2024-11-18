// #include <stdlib.h>
// #include <stdio.h>
// int main(int argc, char **argv)
// {
//     printf("tema1a");

//     return 0;
// }
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>

// Constante
#define MAX_WORD_LEN 50
#define ALPHABET_SIZE 26
#define MAX_FILES 1000

// Structuri
typedef struct {
    char word[MAX_WORD_LEN];
    int file_ids[MAX_FILES];
    int file_count;
} WordEntry;

typedef struct {
    WordEntry *entries;
    int capacity;
    int size;
    pthread_mutex_t lock;
} SharedReducerData;

typedef struct {
    char **files;
    int num_files;
    int mapper_id;
    int total_files;
    SharedReducerData *reducers[ALPHABET_SIZE];
    pthread_mutex_t *task_lock;
    int *current_task;
} MapperArgs;

typedef struct {
    SharedReducerData *reducer_data;
    char output_file[MAX_WORD_LEN];
} ReducerArgs;

// Functii auxiliare
int is_valid_char(char c) {
    return isalnum(c);
}

void normalize_word(char *word) {
    for (int i = 0; word[i]; i++) {
        word[i] = tolower(word[i]);
    }
}

int get_reducer_index(char c) {
    return tolower(c) - 'a';
}

void add_to_reducer(SharedReducerData *reducer, const char *word, int file_id) {
    pthread_mutex_lock(&reducer->lock);

    for (int i = 0; i < reducer->size; i++) {
        if (strcmp(reducer->entries[i].word, word) == 0) {
            for (int j = 0; j < reducer->entries[i].file_count; j++) {
                if (reducer->entries[i].file_ids[j] == file_id) {
                    pthread_mutex_unlock(&reducer->lock);
                    return;
                }
            }
            reducer->entries[i].file_ids[reducer->entries[i].file_count++] = file_id;
            pthread_mutex_unlock(&reducer->lock);
            return;
        }
    }

    // Adaugare cuvant nou
    if (reducer->size == reducer->capacity) {
        reducer->capacity *= 2;
        reducer->entries = realloc(reducer->entries, reducer->capacity * sizeof(WordEntry));
    }
    strcpy(reducer->entries[reducer->size].word, word);
    reducer->entries[reducer->size].file_ids[0] = file_id;
    reducer->entries[reducer->size].file_count = 1;
    reducer->size++;

    pthread_mutex_unlock(&reducer->lock);
}

// Functia Mapper
void *mapper(void *args) {
    MapperArgs *mapper_args = (MapperArgs *)args;

    while (1) {
        pthread_mutex_lock(mapper_args->task_lock);
        if (*mapper_args->current_task >= mapper_args->total_files) {
            pthread_mutex_unlock(mapper_args->task_lock);
            break;
        }
        int file_index = (*mapper_args->current_task)++;
        pthread_mutex_unlock(mapper_args->task_lock);

        char *file = mapper_args->files[file_index];
        FILE *fp = fopen(file, "r");
        if (!fp) {
            perror("Error opening file");
            continue;
        }

        char word[MAX_WORD_LEN];
        while (fscanf(fp, "%49s", word) == 1) {
            char filtered_word[MAX_WORD_LEN] = "";
            for (int i = 0; word[i]; i++) {
                if (is_valid_char(word[i])) {
                    strncat(filtered_word, &word[i], 1);
                }
            }
            normalize_word(filtered_word);
            if (strlen(filtered_word) > 0) {
                int reducer_index = get_reducer_index(filtered_word[0]);
                add_to_reducer(mapper_args->reducers[reducer_index], filtered_word, file_index + 1);
            }
        }
        fclose(fp);
    }
    return NULL;
}

// Functia Reducer
void *reducer(void *args) {
    ReducerArgs *reducer_args = (ReducerArgs *)args;
    SharedReducerData *data = reducer_args->reducer_data;

    FILE *fp = fopen(reducer_args->output_file, "w");
    if (!fp) {
        perror("Error opening output file");
        return NULL;
    }

    // Sortare descrescatoare
    for (int i = 0; i < data->size - 1; i++) {
        for (int j = i + 1; j < data->size; j++) {
            if (data->entries[i].file_count < data->entries[j].file_count ||
                (data->entries[i].file_count == data->entries[j].file_count &&
                 strcmp(data->entries[i].word, data->entries[j].word) > 0)) {
                WordEntry temp = data->entries[i];
                data->entries[i] = data->entries[j];
                data->entries[j] = temp;
            }
        }
    }

    // Scriere rezultat
    for (int i = 0; i < data->size; i++) {
        fprintf(fp, "%s:[", data->entries[i].word);
        for (int j = 0; j < data->entries[i].file_count; j++) {
            fprintf(fp, "%d", data->entries[i].file_ids[j]);
            if (j < data->entries[i].file_count - 1) {
                fprintf(fp, " ");
            }
        }
        fprintf(fp, "]\n");
    }
    fclose(fp);
    return NULL;
}

// Functia Principala
int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <num_mappers> <num_reducers> <input_file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);

    FILE *input_file = fopen(argv[3], "r");
    if (!input_file) {
        perror("Error opening input file");
        return EXIT_FAILURE;
    }

    int total_files;
    fscanf(input_file, "%d", &total_files);

    char **files = malloc(total_files * sizeof(char *));
    for (int i = 0; i < total_files; i++) {
        files[i] = malloc(256 * sizeof(char));
        fscanf(input_file, "%s", files[i]);
    }
    fclose(input_file);

    // Initializare reduceri
    SharedReducerData *reducers[ALPHABET_SIZE];
    for (int i = 0; i < ALPHABET_SIZE; i++) {
        reducers[i] = malloc(sizeof(SharedReducerData));
        reducers[i]->entries = malloc(10 * sizeof(WordEntry));
        reducers[i]->capacity = 10;
        reducers[i]->size = 0;
        pthread_mutex_init(&reducers[i]->lock, NULL);
    }

    // Mapperi
    pthread_t mapper_threads[num_mappers];
    MapperArgs mapper_args[num_mappers];
    pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;
    int current_task = 0;

    for (int i = 0; i < num_mappers; i++) {
        mapper_args[i] = (MapperArgs){.files = files,
                                      .num_files = total_files,
                                      .mapper_id = i,
                                      .total_files = total_files,
                                      .reducers = {0},
                                      .task_lock = &task_lock,
                                      .current_task = &current_task};
        memcpy(mapper_args[i].reducers, reducers, sizeof(reducers));
        pthread_create(&mapper_threads[i], NULL, mapper, &mapper_args[i]);
    }

    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_threads[i], NULL);
    }

    // Reduceri
    pthread_t reducer_threads[num_reducers];
    ReducerArgs reducer_args[num_reducers];
    for (int i = 0; i < ALPHABET_SIZE; i++) {
        char output_file[MAX_WORD_LEN];
        snprintf(output_file, sizeof(output_file), "%c.txt", 'a' + i);
        reducer_args[i % num_reducers] = (ReducerArgs){.reducer_data = reducers[i], .output_file = ""};
        strcpy(reducer_args[i % num_reducers].output_file, output_file);
        pthread_create(&reducer_threads[i % num_reducers], NULL, reducer, &reducer_args[i % num_reducers]);
    }

    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }

    // Curatare resurse
    for (int i = 0; i < ALPHABET_SIZE; i++) {
        free(reducers[i]->entries);
        pthread_mutex_destroy(&reducers[i]->lock);
        free(reducers[i]);
    }

    for (int i = 0; i < total_files; i++) {
        free(files[i]);
    }
    free(files);

    pthread_mutex_destroy(&task_lock);
    return EXIT_SUCCESS;
}
