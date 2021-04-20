import time 
import pickle
import os
import pandas as pd


BLOCK_SIZE = 256
MAX_TIME = 1000000
MAX_BLOCK_NUM = 1024
table_list = {}
max_record_num = {}

def read_block_from_disk(table_name,block_ID):
    df = pd.read_csv(r'./data/' + self.tablename  + r'.csv', header=None,engine='python')
    templist = df.values.tolist()
    block_data = []
    for i in range (BLOCK_SIZE):
        if (block_ID* BLOCK_SIZE + i) >= len(templist):
            break
        block_data.append(templist[block_ID*BLOCK_SIZE + i])
    return block_data

def insert_new_record(table_name, record):
    temp_list = [] 
    temp_list.append(record)
    df = pd.DataFrame(temp_list)
    df.to_csv(r'./data/' + table_name + r'.csv', mode='a', header=False, index=False)


def create_table(table_name):
    max_record_num[table_name] = 0
    table_list[table_name] = []
    f = open(r'./data/' + table_name + r'.csv', 'w')
    f.close()

def initialize_buffer():    
    #Read from disk
    if (os.path.exists(r'./data/tableList.txt')):
        f = open(r'./data/tableList.txt', 'rb')
        table_list = pickle.load(f)
        f.close()
    if (os.path.exists(r'./data/maxrecordNum.txt')):
        f = open(r'./data/maxrecordNum.txt', 'rb')
        max_record_num = pickle.load(f)
        f.close()


    
    
class Block:
    def __init__ (self, pin = False, isdirty = False, tablename = "", ID = -1, content = [], time = MAX_TIME):
        self.pin = pin
        self.isdirty = isdirty
        self.tablename = tablename
        self.ID = ID
        self.content = content
        self.last_access_time = time 

    def get_size(obj):
        return len(pickle.dumps(obj,protocol = pickle.DEFAULT_PROTOCOL))


    def get_occupied_size(self):
        return self.get_size(self.content)

    def write_back_to_disk(self):
        record_length = len(self.content)
        df1 = pd.read_csv(r'./data/' + self.tablename + r'.csv', header=None, engine='python')
        temp_list = df1.values.tolist()
        for i in range(record_length):
            temp_list[self.ID * BLOCK_SIZE] = self.content[i]


        df2 = pd.DataFrame(temp_list)

        #Write it to csv file 
        df2.to_csv(r'./data/' + self.tablename + r'.csv', mode='w', header=False, index=False)





# Buffer manager is for allocating space to the buffer
class Buffer_Manager:
    def __init__(self):
        #Buffer is a list of blocks
        self.buffer = list()
        self.block_num = 0 



    #If no spcae is left in the buffer, need to remove an exisiting block from the buffer
    #Before allocating the new one, which used LRU.
    #LRU: the block that was least recently used is removed from the buffer and writtien back to the disk
    def LRU_replace(self):
        access_time = MAX_TIME
        for i in range(len(self.buffer)):
            if self.buffer[i].pin == FALSE and self.buffer[i].last_access_time < access_time:
                access_time = self.buffer[i].last_access_time

        block_to_be_replaced = self.buffer[i]
        #Write back the block to disk
        Block.write_back_to_disk(block_to_be_replaced)

        #Remove from the list
        self.buffer.remove(block_to_be_replaced)



    def insert_record(self,tablename,record):
        # Table appeared in table_list, which means we had that table's record before
        if tablename in table_list.keys():
            if len(table_list[tablename] != 0 ):

                old_record_position = table_list[tablename][0]
                #remove the old record position from the table_list
                table_list[tablename].remove(table_list[tablename][0])

                block_ID = old_record_position // BLOCK_SIZE

                offset = old_record_position % BLOCK_SIZE
                is_insert = False

                for i in range(len(self.buffer)):
                    # Block is inside of the buffer 
                    if self.buffer[i].tablename == tablename and self.buffer[i].ID == block_ID:
                        self.buffer[i].content[offset] = record
                        self.buffer[i].last_access_time = time.time()
                        #Dirty Block now! Need to resync
                        self.buffer[i].isdirty = True

                        return block_ID * BLOCK_SIZE + offset

                # Block is outside of the buffer but on the disk
                if is_insert == False:
                    # Buffer reaches its capacity, need to replace an existing block in the buffer
                    if self.block_num == MAX_BLOCK_NUM:
                        LRU_replace()
                        self.block_num = self.block_num - 1

                    block_content = read_block_from_disk(tablename,block_ID)

                    block_content_length = len(block_content)

                    if block_content_length > offset:
                        block_content[offset] = record 
                    else:
                        block_content.append(record)

                    self.buffer.append(Block(False, True, tablename, block_ID, block_content, time.time()))
                    self.block_data += 1
                    return block_ID * BLOCK_SIZE + offset

        # Table not appeared in table_list
        block_ID = max_record_num[tablename] // BLOCK_SIZE
        offset = max_record_num[tablename] % BLOCK_SIZE


        max_record_num[tablename] += 1 

        insert_new_record(tablename,record)

        if offset == 0:
            if slef.block_num == MAX_BLOCK_NUM:
                LRU_replace()
                self.block_num -= 1 

            self.buffer.append(Block(False, True, tablename, block_ID, [record], time.time()))
            self.block_data += 1

        else:
            is_insert = False
            for i in range(len(self.buffer)):

                if self.buffer[i].tablename == tablename and self.buffer[i].ID == block_ID:
                    self.buffer[i].content.append(record)
                    self.buffer[i].last_access_time = time.time()
                    self.buffer[i].isdirty = False
                    is_insert = False
                    return block_ID * BLOCK_SIZE + offset

            #Otherwise, is_insert is still False
            if self.block_num == MAX_BLOCK_NUM:
                LRU_replace()
                self.block_num -= 1
                block_content = read_block_from_disk(tablename,block_ID)
                self.buffer.append(Block(False, False, tablename, block_ID, block_content, time.time()))
                self.block_data += 1
                return block_ID *BLOCK_SIZE + offset
    

        
                                   
        
        
        
    def close(self):
        for block in self.buffer:
            if block.isdirty:
                block.write_back_to_disk()
        f = open(r'./data/maxrecordNum.txt', 'wb')
        pickle.dump(max_record_num, f)   
        f.close()
    
        f = open(r'./data/tableList.txt', 'wb')
        pickle.dump(max_record_num, f)   
        f.close()      



if __name__ == '__main__':


    initialize_buffer()
    
    buf = Buffer_Manager()
    



    buf.close()
