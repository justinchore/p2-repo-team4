import csv
import names
from collections import Counter
import uuid
from faker import Faker
from timeit import default_timer as timer

start = timer()
faker = Faker(["en-US","fr_FR", "it_IT", "de_DE", "es_CO","de_CH", "de_DE"])
id_dict = {
        "o": 0, #orderid
        "c": 0, #customerid
        "p": 0, #productid
        "t": 0 #txnid
        }

users_dict = {
        
}

def incr_id(id_type): 
        id = id_dict[id_type]
        id_dict[id_type] += 1
        return id

def uuid_id_generator():
        return uuid.uuid4()

#Faker:
def faker_first_name():
        return faker.unique.first_name()

def faker_last_name():
        return faker.unique.last_name()

test_list = []
for num in range(0, 1000):
        first_name = faker_first_name()
        last_name = faker_last_name()
        test_list.append(last_name)
        
        
end = timer()
print(Counter(test_list))
for i in Counter(test_list).items():
        if i[1] > 1:
                print(i)

print(f"Approximate Processing Time: {end - start}")