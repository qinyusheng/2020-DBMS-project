#include"../include/pm_ehash.h"

// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现

std::vector<data_page *> PageList;

/**/
// return: 0-success, >0-offset
int WritePageToFile(data_page* page)
{
    pmem_persist(page, sizeof(data_page));
    //pmem_unmap(page, sizeof(data_page));
    return 0;
}

// return: -1-full, >0-offset
data_page* ReadPageFromFile(uint32_t fid)
{
    char *buf = new char[strlen(DATA_PAGE_LIBRARY) + sizeof(fid) + 1];
    sprintf(buf, "%s%d", DATA_PAGE_LIBRARY, fid);
    const char* path = buf;

    if (-1 == access(path, F_OK)) {
        // perror("In file[%s], line[%s]", __FILE__, __LINE__);
        perror("Read Page error 1");
        return NULL;
    }
    data_page* tmp = (data_page*)pmem_map_file(path, sizeof(data_page), PMEM_FILE_CREATE,
                                               0777, NULL, NULL);
    delete buf;
    return tmp;                                           
}

// return: -1-full, >0-offset
int GetNewBucketFromPage(const data_page* page)
{
    for (int i = 0; i < DATA_PAGE_SLOT_NUM; i ++) {
        if (! page->bitmap[i]) return i;
    }
    return -1;
}

pm_bucket* GetVirtualAddress(pm_address addr)
{
    if (addr.offset >= DATA_PAGE_SLOT_NUM) return NULL;
    return &(PageList[addr.fileId]->kv_slot[addr.offset]);
}

// must has the legal fid
// return: 0-success, 1-has this page
int CreateNewPage(uint32_t fid)
{
    char *buf = new char[strlen(DATA_PAGE_LIBRARY) + sizeof(fid) + 1];
    sprintf(buf, "%s%d", DATA_PAGE_LIBRARY, fid);
    const char* path = buf;

    data_page* p = (data_page*)pmem_map_file(path, sizeof(data_page), PMEM_FILE_CREATE,
                                             0777, NULL, NULL);
    // if (NULL == p) {
    //     printf("File already exist.");
    //     return 1;
    // }            

    for (int i = 0; i < DATA_PAGE_SLOT_NUM; i ++) {
        p->bitmap[i] = 0;
        p->kv_slot[i].local_depth = 0;
    }
    
    pmem_persist(p, sizeof(data_page));
    PageList.push_back(p);
    delete buf;
    return 0;
}

// must has the legal fid
// return: 0-success, 1-has no this page
int DeletePage(uint32_t fid)
{
    if (PageList.size() <= fid) return 1;

    char *buf = new char[strlen(DATA_PAGE_LIBRARY) + sizeof(fid) + 1];
    sprintf(buf, "%s%d", DATA_PAGE_LIBRARY, fid);
    const char* path = buf;

    if (0 != pmem_unmap(PageList[fid], sizeof(data_page))) {
        // perror("In file[%s], line[%s]", __FILE__, __LINE__);
        perror("Delete Page error 1!");
        return 1;
    }
    if (0 != remove(path)) {
        // perror("In file[%s], line[%s]", __FILE__, __LINE__);
        perror("Delete Page error 2!");
        return 1;
    }
    PageList[fid] = NULL;
    delete buf;
    return 0;
}
/**/


void UseBucketSlot(pm_address* addr)
{
    PageList[addr->fileId]->bitmap[addr->offset] = true;
}


void FreeBucketSlot(pm_address* addr)
{
    PageList[addr->fileId]->bitmap[addr->offset] = false;
}


/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    int err;
    // metadata
    this->metadata = NULL;
    err = ReadMetaFromFile(this->metadata);

    // catalog
    if (err) {
        catalog.buckets_pm_address = new pm_address[metadata->catalog_size];
        for (int i = 0; i < metadata->catalog_size ; i ++) {
            catalog.buckets_pm_address[i].fileId = 1;
            catalog.buckets_pm_address[i].offset = 0;
        }
        WriteCatalogToFile(&catalog);
        // std::cout << "catalog here\n";
    } else {
        err = ReadCatalogFromFile(&this->catalog);
        // if no catalog file
        if (err == 1) {
            catalog.buckets_pm_address = new pm_address[metadata->catalog_size];
            for (int i = 0; i < metadata->catalog_size; i ++) {
                catalog.buckets_pm_address[i].fileId = 1;
                catalog.buckets_pm_address[i].offset = 0;
            }
            WriteCatalogToFile(&catalog);
            // std::cout << "only no has catalog file\n";
        }
    }

    // read all data page and get virtual address
    PageList.clear();
    PageList.push_back(NULL);
    if (err) {	
        metadata->max_file_id = 0;
        allocNewPage();
        pm_bucket* p;
        p = getNewBucket();
        pm_address paddr = vAddr2pmAddr[p];
        if (paddr.offset != 0) std::cout << "error queue!\n";

        catalog.buckets_virtual_address = new pm_bucket* [metadata->catalog_size];

        for (int i = 0; i < metadata->catalog_size; i ++) {
            catalog.buckets_virtual_address[i] = p;
        }

    } else {
        recover();
        // push virtual address to catalog
        catalog.buckets_virtual_address = new pm_bucket* [metadata->catalog_size];
        for (int i = 0; i < metadata->catalog_size; i ++) {
            catalog.buckets_virtual_address[i] = pmAddr2vAddr[catalog.buckets_pm_address[i]];
        }
    }
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {

}
/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    uint64_t return_val;
    if(search(new_kv_pair.key, return_val) == 0) return -1;
    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
    int index = BucketGetFreeSlot(bucket);
    BucketInsertKv(bucket, index, &new_kv_pair);
    
 //   std::cout << "local_depth=" << bucket->local_depth << " ";
   // std::cout << "bucket address: fid=" << vAddr2pmAddr[bucket].fileId << " offset=" << vAddr2pmAddr[bucket].offset << std::endl;
    //persit;
    WritePageToFile(PageList[vAddr2pmAddr[bucket].fileId]);

    return 0;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    uint64_t return_val;
    if(search(key, return_val) != 0) return -1;
    uint64_t has_key = hashFunc(key);
    pm_bucket* bk = this->catalog.buckets_virtual_address[has_key];
    int index = BucketSearch(bk, key);
    BucketDelete(bk, index);
    if(IsEmptyBucket(bk))
    {
/*
        int bucket_id = 0;
        for(int i = 0; i < metadata->catalog_size; i++)
        {
            if(catalog.buckets_virtual_address[i] == bk)
                bucket_id = i;
        }
*/
        mergeBucket(has_key);
    }
    //persist
    WritePageToFile(PageList[catalog.buckets_pm_address[has_key].fileId]);
    
    return 0;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    uint64_t return_val;
    if(search(kv_pair.key, return_val) != 0) return -1;
    uint64_t has_key = hashFunc(kv_pair.key);
    pm_bucket* bk = this->catalog.buckets_virtual_address[has_key];
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if (!IsEmptySlot(bk, i) && bk->slot[i].key == kv_pair.key) {
            bk->slot[i].value = kv_pair.value;
            //persist
            WritePageToFile(PageList[catalog.buckets_pm_address[has_key].fileId]);
            return 0;
        }
    }
    return -1;
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    uint64_t has_key = hashFunc(key);
    pm_bucket* bk = this->catalog.buckets_virtual_address[has_key];

    if(IsEmptyBucket(bk)) return -1;
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if ( !IsEmptySlot(bk, i) && bk->slot[i].key == key) {
            return_val = bk->slot[i].value;
            return 0;
        }
    }
    return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    // 从头开始取
    // return (key >> (64 - this->metadata->global_depth));
    return key%(1 << metadata->global_depth);
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t index = hashFunc(key);

    pm_bucket* bk = this->catalog.buckets_virtual_address[index];
    while (IsFullBucket(bk)) {
        splitBucket(index);
        index = hashFunc(key);
        bk =  this->catalog.buckets_virtual_address[index];
    }

    return bk;
}

// return a new empty bucket
pm_bucket* PmEHash::getNewBucket() {
    if (free_list.empty()) {
        allocNewPage();
    }
    pm_bucket* r =  free_list.front();
    free_list.pop();
    UseBucketSlot(&(vAddr2pmAddr[r]));
    r->local_depth = 0;
    for (int i = 0; i < (BUCKET_SLOT_NUM/2+1); i ++) {
        r->bitmap[i] = 0;
    }
    return r;
}

// 清空bucket，具体功能待定
void PmEHash::freeEmptyBucket(pm_bucket* bucket) {
    int n = BUCKET_SLOT_NUM /8+1;
    for (int i = 0; i < n; i ++) {
        bucket->bitmap[i] = 0;
    }
    free_list.push(bucket);
    FreeBucketSlot(&(vAddr2pmAddr[bucket]));
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    int index = BucketGetFreeSlot(bucket);
    return &(bucket->slot[index]);
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    pm_bucket* tar_bk = catalog.buckets_virtual_address[bucket_id];
    pm_bucket* new_bk = getNewBucket();

    // 需要时把目录项翻倍
    if (tar_bk->local_depth == metadata->global_depth) extendCatalog();
    // 设置新深度
    tar_bk->local_depth ++;
    new_bk->local_depth = tar_bk->local_depth;

    // 计算分裂需要的数据
    int base = 1 << (tar_bk->local_depth-1);
    int num = 1 << (metadata->global_depth - tar_bk->local_depth + 1);
    int start = bucket_id % base;

    // 把目录项分配到对应的桶上
    for (int i = 1; i < num; i += 2) {
        int index = i*base + start;
        catalog.buckets_pm_address[index] = vAddr2pmAddr[new_bk];
        catalog.buckets_virtual_address[index] = new_bk;
    }

    // 再把数据进行插入
    int nid = 0;
    kv t;
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if (! IsEmptySlot(tar_bk, i)) {
            uint64_t kk = hashFunc(tar_bk->slot[i].key);
            
            if (new_bk == catalog.buckets_virtual_address[kk]) {
                t.key = tar_bk->slot[i].key;
                t.value = tar_bk->slot[i].value;
                BucketDelete(tar_bk, i);
               	BucketInsertKv(new_bk, nid, &t); 
                nid ++;
            }
        }
    }
    // 检测两个桶里是否有一个是满的，有的话再分裂
/*
    if (IsFullBucket(tar_bk)) {
        splitBucket(start);
    } else if (IsFullBucket(new_bk)) {
        splitBucket(start+base);
    }
*/
    // 更新目录项
    WriteCatalogToFile(&catalog);
    return ;
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    pm_bucket* tar_bk = catalog.buckets_virtual_address[bucket_id];
    uint64_t d = tar_bk->local_depth - 1;
    // 计算合并需要的数据
    int base = 1 << tar_bk->local_depth;
    int num = 1 << (metadata->global_depth - tar_bk->local_depth);
    int start = bucket_id % base;
    int mid = base >> 1;
    int end = (start < mid)? start+mid : start-mid;

    if (catalog.buckets_virtual_address[end]->local_depth != tar_bk->local_depth) return ;

    for (int i = 0; i < num; i ++) {
        int from = end;
        int to = i*base + start;
        //std::cout << "from=" << from << " to=" << to << std::endl;
        catalog.buckets_pm_address[to].fileId = catalog.buckets_pm_address[from].fileId;
        catalog.buckets_pm_address[to].offset = catalog.buckets_pm_address[from].offset;
        catalog.buckets_virtual_address[to] = catalog.buckets_virtual_address[from];
    }
    catalog.buckets_virtual_address[end]->local_depth --;

    freeEmptyBucket(tar_bk);

    // 更新目录项
    WriteCatalogToFile(&catalog);
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
    ehash_catalog n;
    uint64_t size = metadata->catalog_size * 2;
    
    // 借用n申请空间
    n.buckets_pm_address = new pm_address[size];
    n.buckets_virtual_address = new pm_bucket*[size];

    // 开始往n中填充数据项，只是简单地拷贝
    for (int i = 0; i < metadata->catalog_size; i ++) {
        n.buckets_pm_address[i].fileId = catalog.buckets_pm_address[i].fileId;
        n.buckets_pm_address[i].offset = catalog.buckets_pm_address[i].offset;
        n.buckets_virtual_address[i] = catalog.buckets_virtual_address[i];
    }
    for (int i = 0; i < metadata->catalog_size; i ++) {
        n.buckets_pm_address[i + metadata->catalog_size].fileId = catalog.buckets_pm_address[i].fileId;
        n.buckets_pm_address[i + metadata->catalog_size].offset = catalog.buckets_pm_address[i].offset;
        n.buckets_virtual_address[i + metadata->catalog_size] = catalog.buckets_virtual_address[i];
    }

    
    // 修改metadata信息
    metadata->catalog_size = size;
    metadata->global_depth ++;

    
    // 释放原空间，并用新的空间替换
    delete catalog.buckets_pm_address;
    delete catalog.buckets_virtual_address;
    catalog.buckets_pm_address = n.buckets_pm_address;
    catalog.buckets_virtual_address = n.buckets_virtual_address;


    WriteMetaToFile(metadata);
    WriteCatalogToFile(&catalog);

    // 成功
    return ;
}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
    if (free_list.empty()) allocNewPage();

    pm_bucket* bp;
    bp = free_list.front();
    free_list.pop();
    new_address = vAddr2pmAddr[bp];
    UseBucketSlot(&new_address);
    return bp;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
    this->metadata->max_file_id ++;
    CreateNewPage(this->metadata->max_file_id);
    for (int i = 0; i < DATA_PAGE_SLOT_NUM; i ++) {
        pm_address addr;
        addr.fileId = this->metadata->max_file_id;
        addr.offset = i;
        pm_bucket* vaddr = &(PageList[this->metadata->max_file_id]->kv_slot[i]);

        this->pmAddr2vAddr[addr] = vaddr;
        this->vAddr2pmAddr[vaddr] = addr;
        free_list.push(vaddr);
    }
    
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
    // ReadCatalogFromFile(&this->catalog);
    // ReadMetaFromFile(this->metadata);

    for (uint32_t k = 1; k <= this->metadata->max_file_id; k ++) {
        data_page* p = ReadPageFromFile(k);
        PageList.push_back(p);
    }
    mapAllPage();
}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
    data_page* p;
    int err;

    /*

    for (uint32_t i = 1; i <= this->metadata->max_file_id; i ++) {
        p = ReadPageFromFile(i);
        if (! p) {
            printf("Error: Couldn't read Page #%d!", i);
            return ;
        }
        PageList.push_back(p);
    }
    */

    for (uint32_t k = 1; k <= this->metadata->max_file_id; k ++) {
        p = PageList[k];

        for (int i = 0; i < DATA_PAGE_SLOT_NUM; i ++) {
            pm_address addr;
            addr.fileId = k;
            addr.offset = i;
            pm_bucket* vaddr = &(p->kv_slot[i]);

            this->pmAddr2vAddr[addr] = vaddr;
            this->vAddr2pmAddr[vaddr] = addr;

            // 把空的桶地址push进队列
            if (! p->bitmap[i]) free_list.push(vaddr);
        }
    }

    // 给目录项创建映射
    /*
    for (int i = 0; i < this->metadata->catalog_size; i ++) {
        catalog.buckets_virtual_address[i] = pmAddr2vAddr[catalog.buckets_pm_address[i]];
    }
    */
}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
    // delete DataPage file
    for (uint32_t i = 1; i <= this->metadata->max_file_id; i ++) {
        DeletePage(i);
    }

    // delete catalog file
    char *buf1 = new char[strlen(PM_EHASH_DIRECTORY) + strlen(CATALOG_NAME) + 1];
    sprintf(buf1, "%s%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    const char* path1 = buf1;
    if (access(path1, F_OK) == 0) std::remove(path1);

    // delete catalog file
    char *buf2 = new char[strlen(PM_EHASH_DIRECTORY) + strlen(META_NAME) + 1];
    sprintf(buf2, "%s%s", PM_EHASH_DIRECTORY, META_NAME);
    const char* path2 = buf2;
    if (access(path2, F_OK) == 0) std::remove(path2);
    metadata = NULL;
}


// to Honghui, path = PM_EHASH_DIRECTORY+META_NAME
// only read/write pm_address
int PmEHash::WriteCatalogToFile(ehash_catalog* cat) {
// return 0;
    char *buf = new char[strlen(PM_EHASH_DIRECTORY) + strlen(CATALOG_NAME) + 1];
    sprintf(buf, "%s%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    const char* path = buf;

    pm_address *tmp = (pm_address*)pmem_map_file(path, metadata->catalog_size*sizeof(pm_address),
                                                             PMEM_FILE_CREATE, 0777, NULL, NULL);
    for (int i=0; i<metadata->catalog_size; i++) {
        tmp[i].offset = cat->buckets_pm_address[i].offset;
        tmp[i].fileId = cat->buckets_pm_address[i].fileId;
    }                                           
    pmem_persist(tmp, metadata->catalog_size*sizeof(pm_address));
    pmem_unmap(tmp, metadata->catalog_size*sizeof(pm_address));
    delete buf;
    return 0;               
}

//return: 0-success, 1-has no catalog file
int PmEHash::ReadCatalogFromFile(ehash_catalog* cat) {
    if (0 == metadata->catalog_size) return 1;

    char *buf = new char[strlen(PM_EHASH_DIRECTORY) + strlen(CATALOG_NAME) + 1];
    sprintf(buf, "%s%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    const char* path = buf;

    // if no this file
    if (access(path, F_OK) == -1) {
        return 1;
    } 

    pm_address *tmp = (pm_address*)pmem_map_file(path, metadata->catalog_size*sizeof(pm_address),
                                                             PMEM_FILE_CREATE, 0777, NULL, NULL);
                                                             
    cat->buckets_pm_address = new pm_address[metadata->catalog_size];
    for (int i=0; i<metadata->catalog_size; i++) {
        cat->buckets_pm_address[i].fileId = tmp[i].fileId;
        cat->buckets_pm_address[i].offset = tmp[i].offset;
    }                                           
    pmem_unmap(tmp, metadata->catalog_size*sizeof(pm_address));    
    delete buf;          
    return 0;                                                         
}

// toHonghui, path = CATALOG_NAME+PM_EHASH_DIRECTORY
int PmEHash::WriteMetaToFile(ehash_metadata* meta) {
    pmem_persist(this->metadata, sizeof(ehash_metadata));
    //pmem_unmap(this->metadata, sizeof(ehash_metadata));
    return 0;
}

// return: 0-success, 1-has no this file
int PmEHash::ReadMetaFromFile(ehash_metadata* meta) {
    char *buf = new char[strlen(PM_EHASH_DIRECTORY) + strlen(META_NAME) + 1];
    sprintf(buf, "%s%s", PM_EHASH_DIRECTORY, META_NAME);
    const char* path = buf;

    if (access(path, F_OK) == -1) {
        this->metadata = (ehash_metadata*)pmem_map_file(path, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, NULL, NULL);
        this->metadata->catalog_size = DEFAULT_CATALOG_SIZE;
        this->metadata->global_depth = 4;
        this->metadata->max_file_id = 0;
        delete buf;
        return 1;
    } else {
        this->metadata = (ehash_metadata*)pmem_map_file(path, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, NULL, NULL);
        delete buf;
        return 0;
    }
    
}
/**/

//
// Method for bucket
//

// Just whether bk[index] is free or not. return 0 as not and otherwise free.
int IsEmptySlot(pm_bucket* bk, int index)
{
    uint32_t n = index/8;
    uint32_t m = index%8;
    uint8_t r = 1 << m; // 用来取bit值
    return  !((bk->bitmap[n] & r) > 0);
}

int IsFullBucket(pm_bucket* bk)
{
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if (IsEmptySlot(bk, i)) return 0;
    }
    return 1;
}

int IsEmptyBucket(pm_bucket* bk) 
{
    int n = BUCKET_SLOT_NUM/8 +1;
    for (int i = 0; i < n; i ++) {
        if (bk->bitmap[i]) return 0;
    }
    return 1;
}

// return 0 if success
int BucketInsertKv(pm_bucket* bk, int index, kv* newkv)
{
    // change bitmap
    int n = index/8;
    int m = index%8;
    uint8_t r = 1 << m;
    bk->bitmap[n] = bk->bitmap[n] | r;

    // insert key
    bk->slot[index].key = newkv->key;
    bk->slot[index].value = newkv->value;
    return 0;
}

// return -1 if the bucket is full
int BucketGetFreeSlot(pm_bucket* bk)
{
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if (IsEmptySlot(bk, i)) return i;
    }
    return -1;
}

// return index
int BucketSearch(pm_bucket* bk, uint64_t key)
{
    for (int i = 0; i < BUCKET_SLOT_NUM; i ++) {
        if (!IsEmptySlot(bk, i) && bk->slot[i].key == key) {
            return i;
        }
    }
    return -1;
}

// return 0 if success
int BucketDelete(pm_bucket* bk, int index)
{
    uint32_t n = index/8;
    uint32_t m = index%8;
    uint8_t r = ~(1 << m); // 用来取bit值

    bk->bitmap[n] = bk->bitmap[n] & r;
    return 0;
}

void PmEHash::showCatalog() {
    std::cout << "$catalog$\n";
    for (int i = 0; i < metadata->catalog_size; i ++) {
        std::cout << i << ": " << "fid=" << catalog.buckets_pm_address[i].fileId << " offset=" << catalog.buckets_pm_address[i].offset << " local_depth=" << catalog.buckets_virtual_address[i]->local_depth << std::endl;
    }
    std::cout << "\n";
}

void printPage() {
    for (int i = 1; i < PageList.size(); i ++) {
        data_page *p = PageList[i];
        std::cout << "page #" << i << std::endl;
        for (int j = 0; j < DATA_PAGE_SLOT_NUM; j ++) {
            if (p->bitmap[j]) {
                std::cout << "    slot #" << j << ": ";
                for (int k = 0; k < BUCKET_SLOT_NUM; k ++) {
                    if (IsEmptySlot(& p->kv_slot[j], k)) {
                        std::cout << "N ";
                    } else {
                        std::cout << p->kv_slot[j].slot[k].key /*<< "-" << p->kv_slot[j].slot[k].value*/ << " ";
                    }
                }
                std::cout << std::endl;
            }
        }
       std::cout << std::endl;
    }
}

