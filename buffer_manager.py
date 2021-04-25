import os 
from sys import getsizeof


# Block is a dictionary that records the block paths, its key is the block file name, the block file is inside its index 
blocks = {}

# access_times is a dictionary that records the access times of blocks, its key is the block file name 
access_times = {}
# Maximum size is 4096 bytes
BLOCK_MAX_SIZE = 4096    
BLOCK_NUM = 128

# Only record latest 2000 operations
counter = 0



#Make sure this data directory exists
path = 'C:/Users/xiuyu/Dropbox/academi8c/mydb/data/'


# Each target(it could be a table or an index) has a directory, which contains its block files 
def buffer(target_name,starting_block_id):
    try:
        block_path = path + target_name + '/'+ str(starting_block_id)
        
        
        if block_path in blocks.keys():
            access_times[block_path] += 1 
            
            return blocks[block_path]
    
        #Buffer still has space! Reads in new block!
        elif getsizeof(blocks) < (BLOCK_MAX_SIZE*BLOCK_NUM):
        
            dir_path = path + target_name
        
            f = open(dir_path,'r')
        
            #Set the file descriptor offset , now pointing to the starting_block_id provided
            f.seek(BLOCK_MAX_SIZE * starting_block_id )
        
            # Update the blocks dictionary with the new block file 
            blocks[block_path] = f.read(BLOCK_MAX_SIZE)
        
            #Set the access_time of this block to 1 
            access_times[block_path] = 1 
        
            f.close()
        
            return blocks[block_path]
   
        else:
        
            #Search for least used block
            least_used_block_path = min(access_times.items(), least_used_block_path = lambda x: x[1])[0]
        
            sub_path = "/" + least_used_block_path.split('/')[-1]
        
            block_id_to_be_replaced = int(least_used_block_path.split('/')[-1])
        
            dir_path_associated = least_used_block_path.strip(sub_path)
        
            f.open(dir_path_associated, 'r+')
            f.seek (BLOCK_MAX_SIZE * block_id_to_be_replaced)
    
            #Save that block to the disk
            f.write(blocks[least_used_block_path])
            f.close()


            blocks.pop(least_used_block_path)
            access_times.pop(least_used_block_path)
            
            #let buffer read in new block
            
            dir_path = path + target_name
            
            f = open(dir_path,'r')
            f.seek(BLOCK_MAX_SIZE * starting_block_id )
            blocks[block_path] = f.read(BLOCK_MAX_SIZE)
            access_times[block_path] = 1 
            f.close()
            
            return blocks[block_path]    
        
        global counter 
        counter += 1
        
        # Block operations reachs 2000, reset the access_times dictionary 
        if counter == 2000:
            for key in access_times.keys():
                access_times[key] = 0
        
    except:
            
            print("Error occured when accessing block files.")
            return None
            

# Everytime a block is modifed, call this function

def buffer_save (target_name,starting_block_id,block):         
    
    try:
        block_path = path + target_name + '/'+ str(starting_block_id)
        blocks[block_path] = block
    
    except:
        raise Exception("Block is None.")
        
def buffer_reset():
    
    dirs = os.listdire(path)
    
    for file in dirs:
        pass
        
    
    
    

# When finised, save all the blocks to disk and clear all the buffer 
def buffer_close():
    
    for key in blocks:
        block_id = int(key.split('/')[-1])
        
        sub_path = "/" + key.split('/')[-1]
        
        #open that directory
        f = open(key.strip(sub_path), 'r+')
        f.seek(BLOCK_MAX_SIZE * block_id)
        f.write(' '*BLOCK_MAX_SIZE)
        f.close()
        
        
        #Save to memory 
        f = open(key.strip(sub_path), 'r+')
        f.seek(BLOCK_MAX_SIZE * block_id)
        f.write(blocks[key])
        f.close()
    
    blocks.clear()
    

if __name__=='__main__':
    
    
    #Create this file first
    #File must exist before you call the buffer 
    
    
    file_name = 'dictionary.csv'
    
    
    file=buffer(file_name,2)
    
    buffer_save(file_name,0,file)
    
    
    
    
            
        
        
        
        
        
        
        
        
        
        
        
        
