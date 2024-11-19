#include <pthread.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

struct MapperArgs {
    const std::vector<std::string> *files;                              // Pointer to array of input file paths
    std::vector<std::map<std::string, std::set<int>>> partial_results;  // Per-mapper partial results -> will contain {word: {file_id}}
    pthread_mutex_t *task_lock;                                         // Lock for accessing current_task
    int *current_task;                                                  // Pointer to index of the next file to process
    int total_files;
    int mapper_id;
    int num_reducers;
    pthread_barrier_t *barrier;  // Barrier to synchronize with other mappers
};

struct ReducerArgs {
    const std::vector<MapperArgs> *mapper_args;  // Pointer to array of MapperArgs -> will have access to all partial results
    int reducer_id;                              // Reducer id -> used to determine which letters to process
    int num_reducers;
    int num_mappers;
    pthread_barrier_t *barrier;  // Barrier to synchronize with mappers
};

// Transform word to lowercase and remove non-alphabetic characters
std::string normalize_word(const std::string &word) {
    std::string normalized;
    for (char c : word) {
        if (isalpha(static_cast<unsigned char>(c))) {
            normalized += tolower(static_cast<unsigned char>(c));
        }
    }
    return normalized;
}

// Have global way to get reducer id based on first letter
// to ensure consistency between mappers and reducers
int get_reducer_id(char first_char, int num_reducers) {
    int letter_index = first_char - 'a';
    return (letter_index * num_reducers) / 26;
}

void *mapper(void *args) {
    auto *mapper_args = static_cast<MapperArgs *>(args);

    while (true) {
        int file_index;

        // Get task
        // We need to lock the task_lock to ensure that the current_task is
        // accessed atomically, because it is a shared resource between mapper threads
        pthread_mutex_lock(mapper_args->task_lock);
        // Check if all files have been processed
        if (*(mapper_args->current_task) >= mapper_args->total_files) {
            pthread_mutex_unlock(mapper_args->task_lock);
            break;
        }
        file_index = (*(mapper_args->current_task))++;
        pthread_mutex_unlock(mapper_args->task_lock);

        // Process file
        const std::string &file = (*mapper_args->files)[file_index];
        std::ifstream infile(file);
        if (!infile.is_open()) {
            std::cerr << "Error opening file: " << file << "\n";
            continue;
        }

        std::string word;
        // Read word by word from file
        while (infile >> word) {
            // Normalize word
            word = normalize_word(word);
            if (!word.empty()) {
                // Check if word contains digits
                bool has_digit = false;
                for (char c : word) {
                    if (isdigit(static_cast<unsigned char>(c))) {
                        has_digit = true;
                        break;
                    }
                }

                if (has_digit) {
                    continue;  // Skip words that contain digits
                }
                char first_char = word[0];
                // Again check if first character is a letter
                if (isalpha(static_cast<unsigned char>(first_char))) {
                    first_char = tolower(static_cast<unsigned char>(first_char));
                    if (first_char >= 'a' && first_char <= 'z') {
                        // For each word, insert it in the corresponding reducer's partial results
                        first_char = tolower(static_cast<unsigned char>(first_char));
                        // Get reducer id based on first letter (consistent with reducer)
                        int reducer_id = get_reducer_id(first_char, mapper_args->num_reducers);
                        auto &partial_map = mapper_args->partial_results[reducer_id];
                        // It will contain {word: {file_id}} sent to reducer for this reducer_id
                        partial_map[word].insert(file_index + 1);
                    }
                }
            }
        }
    }
    // Signal that this mapper has finished
    // Wait for all mappers to finish
    pthread_barrier_wait(mapper_args->barrier);
    return nullptr;
}

void *reducer(void *args) {
    auto *reducer_args = static_cast<ReducerArgs *>(args);
    // We signal that the reducer has started
    // Still need to wait for all mappers to finish
    pthread_barrier_wait(reducer_args->barrier);
    // If we reached this point, all mappers have finished
    const auto &mapper_args = *(reducer_args->mapper_args);
    int reducer_id = reducer_args->reducer_id;
    int num_mappers = reducer_args->num_mappers;

    // Combined results for this reducer
    std::map<std::string, std::set<int>> combined_results;

    // Collect partial results from all mappers that are relevant for this
    // reducer (based on reducer_id)
    for (int i = 0; i < num_mappers; ++i) {
        const auto &partial_map = mapper_args[i].partial_results[reducer_id];
        for (const auto &[word, file_ids] : partial_map) {
            combined_results[word].insert(file_ids.begin(), file_ids.end());
        }
    }

    // Sort and write to output files
    std::vector<std::pair<std::string, std::set<int>>> sorted_results(combined_results.begin(), combined_results.end());

    // Two criteria for sorting:
    // 1. Number of files containing the word (descending order)
    // 2. Alphabetical order (if the number of files is the same)
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto &a, const auto &b) {
                  if (a.second.size() != b.second.size())
                      return a.second.size() > b.second.size();  // Descending order
                  return a.first < b.first;                      // Alphabetical order
              });

    // Write to output files based on starting letters
    for (const auto &[word, file_ids] : sorted_results) {
        char first_char = word[0];
        // Check if first character is a letter
        if (isalpha(static_cast<unsigned char>(first_char))) {
            first_char = tolower(static_cast<unsigned char>(first_char));
            // Check if the word belongs to this reducer (based on first letter
            // and reducer_id)
            if (get_reducer_id(first_char, reducer_args->num_reducers) == reducer_id) {
                std::string output_filename = std::string(1, first_char) + ".txt";
                std::ofstream outfile(output_filename, std::ios::app);
                if (!outfile.is_open()) {
                    std::cerr << "Error opening output file: " << output_filename << "\n";
                    continue;
                }
                // If it's all good, write the word and the list of file_ids
                outfile << word << ":[";
                for (auto it = file_ids.begin(); it != file_ids.end(); ++it) {
                    if (it != file_ids.begin()) outfile << " ";
                    outfile << *it;
                }
                outfile << "]\n";
            }
        }
    }

    return nullptr;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <num_mappers> <num_reducers> <input_file>\n";
        return EXIT_FAILURE;
    }

    int num_mappers = std::stoi(argv[1]);
    int num_reducers = std::stoi(argv[2]);

    std::ifstream infile(argv[3]);
    if (!infile.is_open()) {
        std::cerr << "Error opening input file\n";
        return EXIT_FAILURE;
    }

    int total_files;
    infile >> total_files;

    std::vector<std::string> files(total_files);
    for (int i = 0; i < total_files; ++i) {
        infile >> files[i];
    }
    pthread_barrier_t barrier;
    // Barrier to synchronize between mappers and reducers
    pthread_barrier_init(&barrier, nullptr, num_mappers + num_reducers);

    // ----INITIALIZATION----
    // Separated the intialization of all threads and arguments to make it easier to read

    // ----MAPPER THREADS----

    // We have a vector of partial results for each mapper, chose 26 because of
    // the alphabet size (26 letters) => each mapper will produce a partial alphabet
    std::vector<std::map<std::string, std::set<int>>> partial_results(26);
    // Lock for accessing current_task
    pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;
    int current_task = 0;

    std::vector<pthread_t> mapper_threads(num_mappers);
    std::vector<MapperArgs> mapper_args(num_mappers);

    // Initialize mapper arguments
    for (int i = 0; i < num_mappers; ++i) {
        mapper_args[i].files = &files;
        mapper_args[i].partial_results.resize(26);
        mapper_args[i].task_lock = &task_lock;
        mapper_args[i].current_task = &current_task;
        mapper_args[i].total_files = total_files;
        mapper_args[i].mapper_id = i;
        mapper_args[i].num_reducers = num_reducers;
        mapper_args[i].barrier = &barrier;
    }
    // Create mapper threads
    for (int i = 0; i < num_mappers; ++i) {
        pthread_create(&mapper_threads[i], nullptr, mapper, &mapper_args[i]);
    }

    // ----REDUCER THREADS----

    // Initialize reducer arguments
    std::vector<pthread_t> reducer_threads(num_reducers);
    std::vector<ReducerArgs> reducer_args(num_reducers);
    // Initialize reducer arguments
    // Reducer id is used to determine which letters to process
    // (first_letter * num_reducers) / 26 = reducer_id
    for (int i = 0; i < num_reducers; ++i) {
        reducer_args[i].mapper_args = &mapper_args;
        reducer_args[i].reducer_id = i;
        reducer_args[i].num_reducers = num_reducers;
        reducer_args[i].num_mappers = num_mappers;
        reducer_args[i].barrier = &barrier;
    }
    // Create reducer threads
    for (int i = 0; i < num_reducers; ++i) {
        pthread_create(&reducer_threads[i], nullptr, reducer, &reducer_args[i]);
    }

    // ----WAIT FOR THREADS TO FINISH----

    // Wait for mapper threads to finish
    for (int i = 0; i < num_mappers; ++i) {
        pthread_join(mapper_threads[i], nullptr);
    }
    // Wait for reducer threads to finish
    for (int i = 0; i < num_reducers; ++i) {
        pthread_join(reducer_threads[i], nullptr);
    }
    // Destroy barrier
    pthread_barrier_destroy(&barrier);

    return 0;
}
