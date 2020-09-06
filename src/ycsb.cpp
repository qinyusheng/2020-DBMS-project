#include "../include/pm_ehash.h"
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <chrono>
#include <cstdlib>
using namespace std;
using namespace chrono;

typedef struct operation{
    uint64_t op;
    uint64_t record;
} operation;

#if 1
int main() {
    uint64_t load_count;
    uint64_t run_count;
    double load_time;
    double run_time;
    uint64_t run_insert;
    uint64_t run_read;
    uint64_t run_update;

    char loadPath[7][256];
    char runPath[7][256];
    strcpy(loadPath[0], "../workloads/1w-rw-50-50-load.txt");
    strcpy(loadPath[1], "../workloads/10w-rw-0-100-load.txt");
    strcpy(loadPath[2], "../workloads/10w-rw-25-75-load.txt");
    strcpy(loadPath[3], "../workloads/10w-rw-50-50-load.txt");
    strcpy(loadPath[4], "../workloads/10w-rw-75-25-load.txt");
    strcpy(loadPath[5], "../workloads/10w-rw-100-0-load.txt");
    strcpy(loadPath[6], "../workloads/220w-rw-50-50-load.txt");
    strcpy(runPath[0], "../workloads/1w-rw-50-50-run.txt");
    strcpy(runPath[1], "../workloads/10w-rw-0-100-run.txt");
    strcpy(runPath[2], "../workloads/10w-rw-25-75-run.txt");
    strcpy(runPath[3], "../workloads/10w-rw-50-50-run.txt");
    strcpy(runPath[4], "../workloads/10w-rw-75-25-run.txt");
    strcpy(runPath[5], "../workloads/10w-rw-100-0-run.txt");
    strcpy(runPath[6], "../workloads/220w-rw-50-50-run.txt");

    vector<operation> load;
    vector<operation> run;
    FILE* fp;
    char str[100];
    
    for (int index = 0; index<7; index++) {

        run_insert = 0;
        run_read = 0;
        run_update = 0;

        //preload
        fp = fopen(loadPath[index], "r");
        while (fscanf(fp, "%s", str) != EOF) {
            operation tmp;
            uint64_t key = 0;
            switch(str[0]) {
                case 'I':
                    tmp.op = 1;
                    break;
                case 'R':
                    tmp.op = 2;
                    break;
                case 'U':
                    tmp.op = 3;
                    break;
                default:
                    break;
            }
            fscanf(fp, "%s", str);

            //memcpy(&(tmp.record), str, 8);
            for (int k = 0; k < 8; k++) {
                key = key * 10 + str[k] - '0';
            }

            tmp.record = key;
            load.push_back(tmp);
        }
        fclose(fp);
        load_count = load.size();

        //prerun
        fp = fopen(runPath[index], "r");
        while (fscanf(fp, "%s", str) != EOF) {
            operation tmp;
            uint64_t key = 0;
            switch(str[0]) {
                case 'I':
                    tmp.op = 1;
                    run_insert++;
                    break;
                case 'R':
                    tmp.op = 2;
                    run_read++;
                    break;
                case 'U':
                    tmp.op = 3;
                    run_update++;
                    break;
                default:
                    break;
            }
            fscanf(fp, "%s", str);

            //memcpy(&(tmp.record), str, 8);
            for (int k = 0; k < 8; k++) {
                key = key * 10 + str[k] - '0';
            }
            tmp.record = key;

            run.push_back(tmp);
        }
        fclose(fp);
        run_count = run.size();

        //load
        PmEHash* ehash = new PmEHash();
        auto start = system_clock::now();
        for (int i=0; i<load.size(); i++) {
            kv tmp;
            uint64_t val;
            tmp.key = load[i].record;
            switch(load[i].op) {
                case 1:
                    ehash->insert(tmp);
                    break;
                case 2:
                    ehash->search(tmp.key, val);
                    break;
                case 3:
                    ehash->update(tmp);
                    break;
                default:
                    break;
            }
        }
        auto end = system_clock::now();
        auto duration = duration_cast<microseconds>(end-start);
        load_time = double(duration.count()) * microseconds::period::num / microseconds::period::den;

        cout << "\n********************************" << loadPath[index] << "*******************************\n";
        cout << "load total time : " << load_time << "s" << endl;
        cout << "load total operations : " << load_count << endl;
        cout << "load operations per second : " << load_count / load_time << endl;
        cout << "********************************";
        for (int j=0; j<strlen(loadPath[index])-1; j++)
            cout << '*';
        cout << "********************************\n";
        
        
        //run
        start = system_clock::now();
        for (int i=0; i<run.size(); i++) {
            kv tmp;
            uint64_t val;
            tmp.key = run[i].record;
            switch(run[i].op) {
                case 1:
                    ehash->insert(tmp);
                    break;
                case 2:
                    ehash->search(tmp.key, val);
                    break;
                case 3:
                    ehash->update(tmp);
                    break;
                default:
                    break;
            }
        }
        end = system_clock::now();
        duration = duration_cast<microseconds>(end-start);
        run_time = double(duration.count()) * microseconds::period::num / microseconds::period::den;
        ehash->selfDestory();

        cout << "\n********************************" << runPath[index] << "*******************************\n";
        cout << "run total Time : " << run_time << "s" << endl;
        cout << "run total operations : " << run_count << endl;
        cout << "run operations per second : " << run_count / run_time << endl;
        cout << "INSERT : " << run_insert << endl;
        cout << "READ : " << run_read << endl;
        cout << "UPDATE : " << run_update << endl;
        cout << "********************************";
        for (int j=0; j<strlen(loadPath[index])-1; j++)
            cout << '*';
        cout << "********************************\n";
    }
}
#endif
