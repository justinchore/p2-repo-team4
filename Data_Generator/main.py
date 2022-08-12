import csv
from faker import Faker
import csv
import uuid ##txn_code
from timeit import default_timer as timer ##performance measuring



#### SETUP #####

#Faker instances
name_faker = Faker(["en-US","fr_FR", "it_IT", "de_DE", "es_CO","de_CH", "de_DE", "es_MX" ])

#Timer start for performance testing
start = timer()

#All "base" data will go in here
master_list = []


#### ID_GENERATION ####

#Dictionary for dynamic id printing (reference):
id_dict = {
        "o": 0, #orderid
        "c": 0, #customerid
        "p": 0, #productid
        "t": 0 #txnid
        }


#For incremented IDs: (return -> int) When calling, see id_dict for arguments
def incr_id(id_type): 
        id = id_dict[id_type]
        id_dict[id_type] += 1
        return id
    
#For unique txn_code/id (return -> string)
def uuid_id_generator():
        return uuid.uuid4()

        
#### PRODUCT_GENERATOR ### (return -> string)
def product_function(): 
    pass
    #return product

#### NAME_GENERATION ####

def get_first_name():
        return name_faker.unique.first_name()

def get_last_name():
        return name_faker.unique.last_name()



#### MASTER_LIST_CONSTRUCTION #####

def construct_master_list(row_count):
    for _ in range(1, row_count):
        row = []
        customer_id = incr_id("c")
        row.append(customer_id)
    
        first_name = get_first_name()
        last_name = get_last_name()
        full_name = f"{first_name} {last_name}"
        row.append(full_name)
        
        product = 'Pencil'
        row.append(product)
    
        master_list.append(row)
         
    

#### WRITE_TO_FILE ######




#Call construct_master_list
construct_master_list(500)

#Timer end
end = timer()

#Print elapsed time (seconds)
print(f"Approximate Processing Time: {end - start}")
#Show Master List
print(master_list)