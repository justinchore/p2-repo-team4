import csv
from faker import Faker
import csv
import random
import uuid ##txn_code
from timeit import default_timer as timer ##performance measuring

from random import randrange
from datetime import timedelta
from datetime import datetime



#### SETUP #####

#Faker instances
name_faker = Faker(["en-US","fr_FR", "it_IT", "de_DE", "es_CO","de_CH", "de_DE", "es_MX" ])
fake = Faker()

#Timer start for performance testing
start = timer()

#All "base" data will go in here
master_list = []

###### LOCATION DICTIONARY #########
DICT =  {'United States' : ["New York","Chicago","San Diego","San Jose","Dallas","Florida","Washington DC","Orlando","Phoenix","Houston"],
        'Italy' :["Rome","Florence","Milan","Naples","Verona","Genoa","Turin","Perugia","capri","Bologna"],
        'Nigeria':["lagos","Abuja","Jos","Ilorin","Ibadan","Enugu","Benin city","Port Harcourt","lokoja","Uyo"],
        'England': ["Manchester","London","Birmingham","Liverpool","Bristol","Oxford","cambridge","cardiff","Brighton","Leeds"],
        'Spain':["Madrid","Bilbao","Barcelona","Seville","Granada","Valencia","Salamanca","Toledo","Malaga","Cordoba"],
         'France' : ["Paris","Nice","Bordeaux","Toulouse","Nantes","Marseille","Lille","Strasbourg","Nice","Lyon"],
        'Germany' :["Berlin","Cologne","Frankfurt","Munich","Hamburg","Leipzig","Stuttgart","Bremen","Nuremberg","mainz"],
        'Portugal':["Lisbon","Braga","Porto","Guimaraes","Aviero","Faro","Tomar","Elvas","Tavira","Evora"],
        'Japan': ["Tokyo","Osaka","Nagasaki","Hiroshima","Bristol","Kobe","Yokohama","Toyama","Kamakura","Kure"],
        'Mexico':["Mexico City","Monterrey","Merida","Cancun","Tijuana","Leon","Tampico","Bacalar","Vallodolid","Zopopan"]
        }

### Ecommerce list ###
ecommerce_fake_lst = ['Ebay.com', 'Abebooks.com', 'Albris.com', 'Bookoutlet.com', 'Kidsbooks.com', 'Walmart.com','Amazon.com','Booktopia.com',
'Thriftbooks.com','Booksamillion.com','Amazonbooks.com','Barnesandnoble.com','Target.com','Costco.com','Daedalus Books','Powellsbooks.com',
'Thestrand.com','Hudsonbooksellers.com','Wordery.com','Hive.com','Waterstones.com','Chapters.indigo.ca','Mcnallyrobinson.com','Audible.com',
'Audiobooks.com','Audiobooksnow.com','Indiebound.com','ChronicleBooks.com']


#### ID_GENERATIOR ####

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
def productGen():
    fake = Faker()
    f = open("books.csv")
    lst = [[]] #id, name, category, price
    random.seed(0)
    Faker.seed(0)
    c=0
    for x in f:
        inList = []
        line = x.split(',')
        if(c == 0):
            c = 1
            continue
        inList.append(str(fake.ean(13)))
        inList.append(line[0])
        inList.append(line[-3])
        inList.append(str("$"+str(random.randint(15,40))+".99"))
        lst.append(inList)
    f.close()
    return lst

    #return product
    
def get_quantity():
        return random.randint(1, 10)

#### NAME_GENERATOR ####

def get_first_name():
        return name_faker.unique.first_name()

def get_last_name():
        return name_faker.unique.last_name()


### Random date generator 2021 ###
def random_date(start, end):
    
    #returns a random datetime between two datetime objects.
    
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def print_date():    
    #prints/returns a random datetime object within the year 2021
    d1 = datetime.strptime('1/1/2021 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2021 11:59 PM', '%m/%d/%Y %I:%M %p')
    randomized_date2021 = random_date(d1, d2)
#     print(randomized_date2021)
    return randomized_date2021

    ### Ecommerce randomizer ###
def ecommerce_randomizer_lst():
    ecommerce_random_output = (random.choice(ecommerce_fake_lst))
#     print(ecommerce_random_output)
    return ecommerce_random_output
    
#### CITY_COUNTRY _GENERATOR ####
def getCountry():
    random_country = random.choice(list(DICT.keys()))
    return random_country

def getCity(country):
    random_city = DICT[country][random.randint(0, len(DICT)-1)]
    return  random_city

#### PAYMENTS ####
def get_success_or_fail():
    lst = ["Card declined", "Failed to connect to server"]
    succ = random.choices(['Y', 'N'], weights=(90,30))[0]
    if succ =='Y':
        return ('Y',None)
    return (succ, random.choice(lst))

#### MASTER_LIST_CONSTRUCTOR #####

def construct_master_list(row_count):
    for _ in range(1, row_count):
        row = []

        customer_id = incr_id("c")
        row.append(customer_id)

        first_name = get_first_name()
        last_name = get_last_name()
        full_name = f"{first_name} {last_name}"
        row.append(full_name)

        #This goes after all the function calls/value assignments

        country = getCountry()
        city = getCity(country)
        row.append(city)
        row.append(country)

        master_list.append(row)



#### WRITE_TO_FILE ######
def create_csv_data():
        construct_master_list(500)

        header = ['order_id',
                  'customer_id', 
                  'customer_name', 
                  'product_id', 
                  'product_name', 
                  'product_category',
                  'payment_type',
                  'qty',
                  'price',
                  'datetime',
                  'city',
                  'country',
                  'ecommerce_website_name',
                  'payment_txn_id',
                  'payment_txn_success',
                  'failure_reason'
                  ]
        
        with open('order_data.csv', 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                products_list = productGen()
                # write the header
                writer.writerow(header)
                random_order = []
                for _ in range(0, 10000):
                        
                        #Randomly generate a user from master list
                        random_user = random.choice(master_list)
                        userid = random_user[0]
                        username = random_user[1]
                        usercity = random_user[2]
                        usercountry = random_user[3]
                        #Generate incremented order id, insert into random_order[0]
                        orderid = incr_id("o")
                        #[0, 0, Agnes, ###PRODUCT STUFF #####, Starsburg, France]
                        #Randomly generate productid, product_name, product_category
                        random_product = random.choice(products_list)
                        while(not len(random_product)>3):
                            random_product = random.choice(products_list)
                        
                        # [452345435, "NAME OF PRODUCT", "Category", price]
                        # #We need to insert the contents of random_product into INDEX 3 inside random_order
                        productid = random_product[0]
                        productname = random_product[1]
                        productcat = random_product[2]
                        productprice = random_product[3]
                        #Quantity
                        quantity = get_quantity()
                        #Datetime
                        datetime = print_date()
                        #Ecommerce website
                        ecom_website = ecommerce_randomizer_lst()
                        #Payment TXN id
                        txnid = uuid_id_generator()
                        #Payment Success
                        paymentSuc = get_success_or_fail()#(Y/N, reason for N)
                        paymenttype = random.choice(["1", "2", "3"])
                        random_order = [orderid, userid, username, productid, productname, productcat, paymenttype, quantity, 
                                        productprice, datetime, usercity, usercountry, ecom_website, txnid, paymentSuc[0], paymentSuc[1]]
                                
                                
                        # 

                        # write 1 row
                        writer.writerow(random_order)
        
        
                


#Call construct_master_list

create_csv_data()
#Timer end
end = timer()

#Print elapsed time (seconds)
print(f"Approximate Processing Time: {end - start}")

#Show Master List
# print(master_list[:s50])
