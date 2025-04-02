"""
This script is the implamentation of the solution to question 3 of the assignment.
Distributed File System
uploads, downloads, and searches, with built-in load balancing and replication

"""

from mpi4py import MPI
import time
import threading

class MasterNode:
    def __init__(self, comm, size):
        """
        array to hold which storage nodes are running 
        False for not running, True for running
        index 0 is the master node itself (rest are 1 indexed)
        
        dictionary to hold the files and their locations
        each file is divided into chunks of 32 bytes 
        each chunk is stored in 3 different storage nodes
        dictionary will hold file name as key and
        value as the indidces of the storage nodes where the chunks are stored
        0 chunk is at nodes 1,2,3; 1 chunk is at nodes 4,5,6 and so on
        also store the index of the chunk in the storage node
        {0: [(1,45), (2, 23), (3, 12)], 1: [(4, 45), (5, 23), (6, 12)]}
        for a file, the 0th chunk is stored in storage node 1 at index 45, 
        in storage node 2 at index 23, and in storage node 3 at index 12 and so on

        to know a track of how full a storage node is
        an array to hold the current number of chunks stored in each storage node
        index 0 is the master node itself (rest are 1 indexed)
        new chunks are uploaded to the storage node with the least number of chunks

        """
        self.comm = comm
        self.rank = 0
        self.size = size

        self.stop_event = threading.Event()

        self.storage_nodes_available = [True for _ in range(size)]
        self.storage_nodes_available[0] = False 
        self.files = {}
        self.storage_occupied = [0 for _ in range(size)]

        curr_time = time.time()
        self.last_heartbeat = {i: curr_time for i in range(1, size)}
        self.heartbeat_threshold = 2
        self.heartbeat_thread = threading.Thread(target=self.handle_heartbeat, daemon=True)
        self.heartbeat_thread.start()


    def handle_heartbeat(self):
        while not self.stop_event.is_set():
            status = MPI.Status()
            if comm.Iprobe(source=MPI.ANY_SOURCE, tag=6, status=status):
                source = status.Get_source()
                received_message = comm.recv(source=source)
                if received_message == 'heartbeat' and self.storage_nodes_available[source] == False:
                    self.storage_nodes_available[source] = True
                    print('1')
                self.process_heartbeat(source)

            else:            
                curr_time = time.time()
                for i in range(1, self.size):
                    if self.storage_nodes_available[i]:
                        if curr_time - self.last_heartbeat[i] > self.heartbeat_threshold:
                            self.storage_nodes_available[i] = False
                            print('1')

    def process_heartbeat(self, rank):
        """
        process the heartbeat from the storage node
        """
        self.last_heartbeat[rank] = time.time()

    def stop_threads(self):
        self.stop_event.set()
        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()


    def get_input(self):
        """
        Get the input from the user
        convert the input to a list of strings and return
        """
        input_string = input()
        input_list = input_string.split()
        input_list = list(map(str, input_list))
        return input_list


    def process_input_list(self, input_list):
        """ 
        Handles the input correctness 
        upload file_name absolute_file_path
        retrieve file_name
        search file_name word
        list_file file_name
        failover rank
        recover rank
        exit
        """

        command = input_list[0]
        if command == 'upload':
            if len(input_list) != 3:
                print('-1')
                return
            file_name = input_list[1]
            file_path = input_list[2]
            self.upload_file(file_name, file_path)
            
        elif command == 'retrieve':
            if len(input_list) != 2:
                print('-1')
                return
            file_name = input_list[1]
            self.retrieve_file(file_name)

        elif command == 'search':
            if len(input_list) != 3:
                print('-1')
                return
            file_name = input_list[1]
            word = input_list[2]
            self.search(file_name, word)

        elif command == 'list_file':
            if len(input_list) != 2:
                print('-1')
                return
            file_name = input_list[1]
            self.list_file(file_name)

        elif command == 'failover':
            if len(input_list) != 2:
                print('-1')
                return
            rank = input_list[1]
            self.failover(rank)

        elif command == 'recover':
            if len(input_list) != 2:
                print('-1')
                return
            rank = input_list[1]
            self.recover(rank)

        else:
            print('-1')
            return

    def get_least_occupied_storage_nodes(self, num_nodes):
        """
        returns the indices of the num_nodes least occupied storage nodes
        """
        occupied = self.storage_occupied
        sorted_indices = sorted(range(len(occupied)), key=lambda k: occupied[k])
        return sorted_indices[1:num_nodes+1]
    
    def num_available_storage_nodes(self):
        """
        returns the number of available storage nodes
        """
        num_available_storage_nodes = 0
        for j in range(1, self.size):
            if self.storage_nodes_available[j]:
                num_available_storage_nodes += 1
        return num_available_storage_nodes
        
    def upload_file(self, file_name, file_path):
        """ 
        Uploads the file to the storage nodes
        Divides the file into 32 byte chunks
        Stores each chunk in 3 different storage nodes (least occupied) (if available)
        """
        
        num_available_storage_nodes = self.num_available_storage_nodes()
        if num_available_storage_nodes == 0:
            print('-1')
            return

        file_data = open(file_path, 'r').read()
        file_size = len(file_data)
        num_chunks = file_size // 32
        if file_size % 32 != 0:
            num_chunks += 1
        
        chunks = [file_data[i:i+32] for i in range(0, file_size, 32)]

        self.files[file_name] = {}
        
        for i in range(num_chunks):
            
            storage_node_indices = self.get_least_occupied_storage_nodes(min(3, num_available_storage_nodes))

            if len(storage_node_indices) == 0:
                print('-1')
                return
            
            self.files[file_name][i] = []

            for storage_node_idx in storage_node_indices:
                self.comm.send(chunks[i], dest=storage_node_idx, tag=1)
                chunk_idx_in_storage_node = self.comm.recv(source=storage_node_idx, tag=1)
                
                self.files[file_name][i].append((storage_node_idx, chunk_idx_in_storage_node))

                self.storage_occupied[storage_node_idx] += 1

        print('1')
        self.list_file(file_name)

    def check_if_all_chunks_available(self, file_name):
        """
        This checks if storage nodes for all the chunks of the file are available
        If for any chunk, there is no storage node available which has the chunk, then return False
        """
        for file_chunk_idx in self.files[file_name]:
            found = False
            for storage_node_idx, chunk_idx_in_storage_node in self.files[file_name][file_chunk_idx]:
                if self.storage_nodes_available[storage_node_idx]:
                    found = True
            if not found:
                print('-1')
                return False

        return True

    def retrieve_file(self, file_name):
        """
        Retrieve the file from the storage nodes
        Get the chunks from the storage nodes and concatenate them to get the file
        """
        if file_name not in self.files:
            print('-1')
            return

        if not self.check_if_all_chunks_available(file_name):
            return
            
        file_data = ''
        for file_chunk_idx in self.files[file_name]:
            for storage_node_idx, chunk_idx_in_storage_node in self.files[file_name][file_chunk_idx]:
                if self.storage_nodes_available[storage_node_idx]:
                    self.comm.send(chunk_idx_in_storage_node, dest=storage_node_idx, tag=2)
                    chunk = self.comm.recv(source=storage_node_idx, tag=2)
                    file_data += chunk
                    break
        print(file_data)


    def list_file(self, file_name):
        """
        List the information of the chunks of the file
        chunk_number num_nodes node_ranks
        """
        if file_name not in self.files:
            print('-1')
            return
        
        if not self.check_if_all_chunks_available(file_name):
            return

        file_info = []
        for file_chunk_idx in self.files[file_name]:
            node_ranks = [storage_node_idx for storage_node_idx, _ in self.files[file_name][file_chunk_idx]]
            node_ranks = [storage_node_idx for storage_node_idx in node_ranks if self.storage_nodes_available[storage_node_idx]]
            node_ranks = sorted(node_ranks)
            num_nodes = len(node_ranks)
            string_to_append = f'{file_chunk_idx} {num_nodes} {" ".join(map(str, node_ranks))}'
            file_info.append(string_to_append)

        print('\n'.join(file_info))

    
    def search(self, file_name, word):
        """
        Search for the word in the file
        Each word will be 32 bytes at max - so need to search only within a chunk and in 2 chunks at max
        """

        if file_name not in self.files:
            print('-1')
            return

        if not self.check_if_all_chunks_available(file_name):
            return

        offsets = []
        prev_chunk = ''
        for file_chunk_idx in self.files[file_name]:
            for storage_node_idx, chunk_idx_in_storage_node in self.files[file_name][file_chunk_idx]:
                if self.storage_nodes_available[storage_node_idx]:
                    self.comm.send((chunk_idx_in_storage_node, word), dest=storage_node_idx, tag=3)
                    start_word, end_word, curr_offsets = self.comm.recv(source=storage_node_idx, tag=3)
                    curr_offsets = [offset + 32*file_chunk_idx for offset in curr_offsets]
                    offsets.extend(curr_offsets)

                    if prev_chunk + start_word == word:
                        offsets.append(32*file_chunk_idx - len(prev_chunk))
                    prev_chunk = end_word
                    break

        if prev_chunk == word:
            offsets.append(32*file_chunk_idx - len(prev_chunk))

        if len(offsets) == 0:
            print('0')
            print()
            return
        
        offsets = sorted(offsets)
        print(len(offsets))
        print(' '.join(map(str, offsets)))


    def failover(self, rank):
        """
        Failover the storage node
        Send the storage server a message to stop runnning
        It will stop sending heartbeat to the master node
        When the master node does not receive a heartbeat for 3 seconds, it will mark the storage node as not available
        """
        rank = int(rank)
        if rank >= self.size or rank <= 0:
            print('-1')
            return
        
        if not self.storage_nodes_available[rank]:
            print('-1')
            return
        
        self.comm.send('failover', dest=rank, tag=4)


    def recover(self, rank):
        """
        Recover the storage node
        Send the storage server a message to start running
        It will start sending heartbeat to the master node
        When the master node receives a heartbeat, it will mark the storage node as available
        """
        rank = int(rank)
        if rank >= self.size or rank <= 0:
            print('-1')
            return

        if self.storage_nodes_available[rank]:
            print('-1')
            return

        self.comm.send('recover', dest=rank, tag=5)
    

class StorageNode:
    def __init__(self, comm, rank):
        """
        rank 0 is the master node
        rank 1 to size-1 are the storage nodes
        """
        self.comm = comm
        self.rank = rank
        self.file_chunks = []
        self.is_running = True

        self.stop_event = threading.Event()

        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        self.heartbeat_thread.start()

    def send_heartbeat(self):
        while not self.stop_event.is_set():
            if self.is_running:
                self.comm.send('heartbeat', dest=0, tag=6)
            time.sleep(1)

    def stop_threads(self):
        self.stop_event.set()
        self.is_running = False
        if self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()

    def process_received_message(self, received_message, tag):
        """
        processes the received message

        upload - just append the chunk to the file_chunks
        tag = 1

        retrieve - send the chunk back to the master node
        tag = 2

        search - search for the word in the chunk and send the offsets back to the master node
        also send start and end words to search for words that are split across chunks
        tag = 3
        
        failover - stop running - stop sending heartbeat
        tag = 4

        recover - start running - resume sending heartbeat
        tag = 5

        """

        if tag == 1:
            self.upload_chunk(received_message)

        elif tag == 2:
            self.retrieve_chunk(received_message)

        elif tag == 3:
            self.search_word(received_message)

        elif tag == 4:
            self.failover()

        elif tag == 5:
            self.recover()

        else:
            pass

    def upload_chunk(self, chunk):
        self.file_chunks.append(chunk)
        chunk_idx = len(self.file_chunks) - 1
        self.comm.send(chunk_idx, dest=0, tag=1)

    def retrieve_chunk(self, chunk_idx):
        chunk = self.file_chunks[chunk_idx]
        self.comm.send(chunk, dest=0, tag=2)

    def search_word(self, received_message):
        chunk_idx, word = received_message
        chunk = self.file_chunks[chunk_idx]
        if len(chunk) < 32:
            chunk += ' ' * (32 - len(chunk))
        offsets = []
        words = chunk.split()
        start_word = words[0] if chunk[0] != ' ' else ''
        end_word = words[-1] if chunk[-1] != ' ' else ''
        
        search_word = ' ' + word + ' '
        for i in range(len(chunk) - len(search_word) + 1):
            if chunk[i:i+len(search_word)] == search_word:
                offsets.append(i+1)

        list_to_send = [start_word, end_word, offsets]
        self.comm.send(list_to_send, dest=0, tag=3)

    def failover(self):
        self.is_running = False

    def recover(self):
        self.is_running = True


if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        master = MasterNode(comm, size)
        while True:

            input_list = master.get_input()
            if input_list[0] == 'exit':
                for i in range(1, size):
                    comm.send('exit', dest=i)

                master.stop_threads()

                break
            master.process_input_list(input_list)

    else:
        storage_node = StorageNode(comm, rank)
        while True:
            status = MPI.Status()
            received_message = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            if received_message == 'exit':
                storage_node.stop_threads()

                break
            storage_node.process_received_message(received_message, tag)

    MPI.Finalize()


