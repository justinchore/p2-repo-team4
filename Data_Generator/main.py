from faker import Faker
import random


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