import hashlib
import os
import sys
from collections import defaultdict


# Algorithm to find duplicates
# 1. get all files that have the same size - they are the duplicates candidates
# 2. For all files with the same file size, get their hash on the 1st 1024 bytes only
# 3. For all files with the hash on the 1st 1024 bytes and the same file size, We will check their hash on the full file
# 4. print the duplicates

def check_for_duplicates(parentFolder):
    dict_by_size , index_cells= get_all_sizes(parentFolder)
    dict_1kb_hash = get_all_1kb_hashes(dict_by_size, index_cells)
    dict_hashes_full = get_all_hashes(dict_1kb_hash)
    printDuplicates(dict_hashes_full)


# get all files that have the same size - they are the duplicates candidates
def get_all_sizes(path):
    # initialize dictionary. k = size of file, v = list of filenames with the same size
    dict_by_size = defaultdict(list)
    index_cells = []
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            try:
                file_size = os.path.getsize(full_path)
            except OSError:
                continue
            dict_by_size[file_size].append(full_path)
            if len(dict_by_size[file_size]) == 2: # more than 1 files with the same size
                index_cells.append(file_size)     # add the size to the index
    return dict_by_size , index_cells


def get_all_1kb_hashes(dict_by_size: dict, index_cells: list):
    # initialize dictionary. k = hash code of first 1KB, v = list of filenames with the same hash
    dict_1kb_hash = defaultdict(list)
    # For all files with the same file size, get their hash on the 1st 1024 bytes only
    for file_size in index_cells:
        for filename in dict_by_size[file_size]:
            small_hash = get_hash(filename, first_chunk_only=True)
            if small_hash is None:  # couldn't open the file or get hash, skip it
                continue

            # the key is the hash on the first 1024 bytes plus the size - to
            # avoid collisions on equal hashes in the first part of the file
            dict_1kb_hash[(small_hash, file_size)].append(filename)
    return dict_1kb_hash


def get_all_hashes(dict_1kb_hash: dict):
    # initialize dictionary. k = full hash code , v = list of filenames with the same hash
    dict_hashes_full = defaultdict(list)
    # For all files with the hash on the 1st 1024 bytes, We will check their hash on the full file
    for files_list in dict_1kb_hash.values():
        if len(files_list) > 1:  # it's a duplicate by hash on the 1st 1KB and size
            for filename in files_list:
                full_hash = get_hash(filename, first_chunk_only=False)
                dict_hashes_full[full_hash].append(filename)
    return dict_hashes_full


def printDuplicates(dict_hashes_full: dict):
    for list_of_files in dict_hashes_full.values():
        if len(list_of_files) > 1:    # it's a duplicate
            list_of_files = [os.path.relpath(file, parentFolder) for file in list_of_files]
            print('----------\nduplicate files: ' + ' , '.join(list_of_files) + '\n')


#Generator that reads a file in chunks of 1KB
def chunk_reader(fobj, chunk_size=1024):
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return None
        yield chunk


# get hash for a file - either on the first 1KB or on the full file
def get_hash(filename, first_chunk_only=False, hash=hashlib.md5):       # md5 is the hash function
    hashobj = hash()
    try:
        with open(filename, 'rb') as file_object:
            if first_chunk_only:
                hashobj.update(file_object.read(1024))
            else:
                for chunk in chunk_reader(file_object):
                    hashobj.update(chunk)   # update the hash with the next chunk
            hashed = hashobj.digest()     # get the hash value
    except IOError:
        return
    return hashed



if __name__ == '__main__':
    assert len(sys.argv) == 2, 'Only one argument is expected'
    parentFolder = sys.argv[1]
    check_for_duplicates(parentFolder)
