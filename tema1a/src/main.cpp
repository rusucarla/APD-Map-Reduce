#include <pthread.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

struct MapperArgs {
    const std::vector<std::string> *files;
    std::vector<std::map<std::string, std::set<int>>> partial_results;  // Per-mapper partial results
    pthread_mutex_t *task_lock;
    int *current_task;
    int total_files;
    int mapper_id;
    int num_reducers;
};

struct ReducerArgs {
    const std::vector<MapperArgs> *mapper_args; // Pointer to array of MapperArgs
    int reducer_id;
    int num_reducers;
    int num_mappers;
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
        pthread_mutex_lock(mapper_args->task_lock);
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
        while (infile >> word) {
            word = normalize_word(word);
            if (!word.empty()) {
                bool has_digit = false;
                for (char c : word) {
                    if (isdigit(static_cast<unsigned char>(c))) {
                        has_digit = true;
                        break;
                    }
                }

                if (has_digit) {
                    continue; // Skip words that contain digits
                }
                char first_char = word[0];
                if (isalpha(static_cast<unsigned char>(first_char))) {
                    first_char = tolower(static_cast<unsigned char>(first_char));
                    if (first_char >= 'a' && first_char <= 'z') {
                        first_char = tolower(static_cast<unsigned char>(first_char));
                        int reducer_id = get_reducer_id(first_char, mapper_args->num_reducers);
                        auto &partial_map = mapper_args->partial_results[reducer_id];
                        partial_map[word].insert(file_index + 1);
                    }
                }
            }
        }
    }

    return nullptr;
}

void *reducer(void *args) {
    auto *reducer_args = static_cast<ReducerArgs *>(args);
    const auto &mapper_args = *(reducer_args->mapper_args);
    int reducer_id = reducer_args->reducer_id;
    int num_mappers = reducer_args->num_mappers;

    // Combined results for this reducer
    std::map<std::string, std::set<int>> combined_results;

    // Collect partial results from all mappers
    for (int i = 0; i < num_mappers; ++i) {
        const auto &partial_map = mapper_args[i].partial_results[reducer_id];
        for (const auto &[word, file_ids] : partial_map) {
            combined_results[word].insert(file_ids.begin(), file_ids.end());
        }
    }

    // Sort and write to output files
    std::vector<std::pair<std::string, std::set<int>>> sorted_results(combined_results.begin(), combined_results.end());

    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto &a, const auto &b) {
                  if (a.second.size() != b.second.size())
                      return a.second.size() > b.second.size(); // Descending order
                  return a.first < b.first; // Alphabetical order
              });

    // Write to output files based on starting letters
    for (const auto &[word, file_ids] : sorted_results) {
        char first_char = word[0];
        if (isalpha(static_cast<unsigned char>(first_char))) {
            first_char = tolower(static_cast<unsigned char>(first_char));
            if (get_reducer_id(first_char, reducer_args->num_reducers) == reducer_id) {
                std::string output_filename = std::string(1, first_char) + ".txt";
                std::ofstream outfile(output_filename, std::ios::app);
                if (!outfile.is_open()) {
                    std::cerr << "Error opening output file: " << output_filename << "\n";
                    continue;
                }
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

    std::vector<std::map<std::string, std::set<int>>> partial_results(26);
    pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;
    int current_task = 0;

    std::vector<pthread_t> mapper_threads(num_mappers);
    std::vector<MapperArgs> mapper_args(num_mappers);

    for (int i = 0; i < num_mappers; ++i) {
        mapper_args[i].files = &files;
        mapper_args[i].partial_results.resize(26);
        mapper_args[i].task_lock = &task_lock;
        mapper_args[i].current_task = &current_task;
        mapper_args[i].total_files = total_files;
        mapper_args[i].mapper_id = i;
        mapper_args[i].num_reducers = num_reducers;
    }

    for (int i = 0; i < num_mappers; ++i) {
        pthread_create(&mapper_threads[i], nullptr, mapper, &mapper_args[i]);
    }   

    for (int i = 0; i < num_mappers; ++i) {
        pthread_join(mapper_threads[i], nullptr);
    }

    std::vector<pthread_t> reducer_threads(num_reducers);
    std::vector<ReducerArgs> reducer_args(num_reducers);
    for (int i = 0; i < num_reducers; ++i) {
        reducer_args[i].mapper_args = &mapper_args;
        reducer_args[i].reducer_id = i;
        reducer_args[i].num_reducers = num_reducers;
        reducer_args[i].num_mappers = num_mappers;
    }

    for (int i = 0; i < num_reducers; ++i) {
        pthread_create(&reducer_threads[i], nullptr, reducer, &reducer_args[i]);
    }

    for (int i = 0; i < num_reducers; ++i) {
        pthread_join(reducer_threads[i], nullptr);
    }

    return 0;
}
