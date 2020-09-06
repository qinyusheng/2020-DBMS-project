#ifndef _PM_E_HASH_H
#define _PM_E_HASH_H

#include<cstdint>
#include<queue>
#include<map>
#include<libpmem.h>
#include<unistd.h>
#include<iostream>
#include <vector>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <iostream>

using std::queue;
using std::map;

#define DATA_PAGE_SLOT_NUM 16
#define DATA_PAGE_LIBRARY "/mnt/mem/"
#define BUCKET_SLOT_NUM           15
#define DEFAULT_CATALOG_SIZE      16
#define META_NAME                 "pm_ehash_metadata"
#define CATALOG_NAME              "pm_ehash_catalog"
#define PM_EHASH_DIRECTORY        "/mnt/mem/"      // add your own directory path to store the pm_ehash

/* 
---the physical address of data in NVM---
fileId: 1-N, the data page name
offset: data offset in the file
*/
typedef struct pm_address
{
    uint32_t fileId;
    uint32_t offset;
    bool operator< (const pm_address t) const{
        return (this->fileId < t.fileId) || (this->fileId == t.fileId && this->offset < t.offset);
    }
} pm_address;

/*
the data entry stored by the  ehash
*/
typedef struct kv
{
    uint64_t key;
    uint64_t value;
} kv;

typedef struct pm_bucket
{
    uint64_t local_depth;
    uint8_t  bitmap[BUCKET_SLOT_NUM / 8 + 1];      // one bit for each slot
    kv       slot[BUCKET_SLOT_NUM];                                // one slot for one kv-pair
} pm_bucket;

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash
typedef struct data_page {
    // fixed-size record design
    // uncompressed page format
    bool bitmap[DATA_PAGE_SLOT_NUM];
    struct pm_bucket kv_slot[DATA_PAGE_SLOT_NUM];
} data_page;

// 此处补充关于桶的操作
int IsEmptySlot(pm_bucket* bk, int index); // Just whether bk[index] is free or not. return 0 as not and otherwise free.
int IsFullBucket(pm_bucket* bk);
int IsEmptyBucket(pm_bucket* bk);
int BucketInsertKv(pm_bucket* bk, int index, kv* newkv); // return 0 if success
int BucketGetFreeSlot(pm_bucket* bk); // return -1 if the bucket is full
int BucketSearch(pm_bucket* bk, uint64_t key); // return index
int BucketDelete(pm_bucket* bk, int index); // return 0 if success  

typedef struct ehash_catalog
{
    pm_address* buckets_pm_address;         // pm address array of buckets
    pm_bucket**  buckets_virtual_address;    // virtual address array mapped by pmem_map
} ehash_catalog;

typedef struct ehash_metadata
{
    uint64_t max_file_id;      // next file id that can be allocated
    uint64_t catalog_size;     // the catalog size of catalog file(amount of data entry)
    uint64_t global_depth;   // global depth of PmEHash
} ehash_metadata;

extern std::vector<data_page *> PageList;

class PmEHash
{
private:
    
    ehash_metadata*                               metadata;                    // virtual address of metadata, mapping the metadata file
    ehash_catalog                                      catalog;                        // the catalog of hash
    
    queue<pm_bucket*>                         free_list;                      //all free slots in data pages to store buckets
    map<pm_bucket*, pm_address> vAddr2pmAddr;       // map virtual address to pm_address, used to find specific pm_address
    map<pm_address, pm_bucket*> pmAddr2vAddr;       // map pm_address to virtual address, used to find specific virtual address
    
    uint64_t hashFunc(uint64_t key);

    pm_bucket* getFreeBucket(uint64_t key);
    pm_bucket* getNewBucket();
    void freeEmptyBucket(pm_bucket* bucket);
    kv* getFreeKvSlot(pm_bucket* bucket);

    void splitBucket(uint64_t bucket_id);
    void mergeBucket(uint64_t bucket_id);

    void extendCatalog();
    void* getFreeSlot(pm_address& new_address);
    void allocNewPage();

    // only read/write pm_address
    int WriteCatalogToFile(ehash_catalog* cat);
    int ReadCatalogFromFile(ehash_catalog* cat);
    // int Remove()
    int WriteMetaToFile(ehash_metadata* meta);
    int ReadMetaFromFile(ehash_metadata* meta);

    void recover();
    void mapAllPage();

public:
    PmEHash();
    ~PmEHash();

    int insert(kv new_kv_pair);
    int remove(uint64_t key);
    int update(kv kv_pair);
    int search(uint64_t key, uint64_t& return_val);

    void showCatalog();
    void selfDestory();
};

void printPage();

int WritePageToFile(data_page* page);

data_page* ReadPageFromFile(uint32_t fid);

// return: -1-full, >0-offset
int GetNewBucketFromPage(const data_page* page);

// return the pointer to the bucket, and NULL if fail
pm_bucket* GetVirtualAddress(pm_address addr);

// must has the legal fid
// return: 0-success, 1-has this page
int CreateNewPage(uint32_t fid);

// must has the legal fid
// return: 0-success, 1-has no this page
int DeletePage(uint32_t fid);

void UseBucketSlot(pm_address* addr);

void FreeBucketSlot(pm_address* addr);


#endif
