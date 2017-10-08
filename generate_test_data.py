import random
import datetime
import pymongo

"""
Note that the documents are for sample data. So, no care has been taken to map a district to state or pincode
or location etc. Data generated is purely random and fictitious based on the sample field data.

Sample Document:

{
	"_id" : "123456789012",
	"first_name" : "ANKUR",
	"last_name" : "RAINA",
	"registered_on" : ISODate("2017-10-07T18:42:57.741Z"), #DATE
	"pan_card" : "DELSD1357N",
	"phone_numbers" : [         #ARRAY
		"7042912626",
		"9813269824"
	],
	"permanent_address" : {     #SUB-DOCUMENT / EMBEDDING
		"house_no" : "16-9A",
		"street" : "DLF PHASE 2",
		"landmark" : "NEAR DLF PHASE 3 METRO STATION",
		"locality" : "DLF CYBER CITY",
		"district" : "GURUGRAM",
		"state" : "HARYANA",
		"pincode" : "122002",
		"map" : {
			"location" : {          #GEO-JSON
				"type" : "Point",
				"coordinates" : [
					77.0577449,
					28.4967829
				]
			}
		}
	}
}
"""

try:
    client = pymongo.MongoClient("mongodb://admin:<PASSWORD>@cluster0-shard-00-00-ydjii.mongodb.net:27017,cluster0-shard-00-01-ydjii.mongodb.net:27017,cluster0-shard-00-02-ydjii.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin")
    db = client['demo1']
    collection = db['citizendata']
    collection.create_index([("pan_card",pymongo.ASCENDING)],unique=True)

except Exception as e:
    print(e)
    exit()

first_names = ['ANKUR','ANKIT','ASHISH','NISHANT','AKSHAT','POOJA','DINESH','RAVI','VIVEK','NIRANJAN','ABHISHEK'
               ,'SONU','NEHA','PRIYA','AMITABH','AKSHAY','AJAY','VIJAY','NEELAM','KAREENA','KATRINA','PRIYANKA,'
                'PARINEETI','PARUL','PARAS','VIPUL','SHRESTH','SANDEEP','IMRAN','DANISH','SIBA','URMILLA','SANJIV',
               'SHYAM','RAM','MAHESH','SURESH','HRITIK','AYUSH','ABEER','ANANYA','REVA','GURPREET','ISHITA','SUVEEN',
               'RUCHI','SHWETA','SAKSHI']

last_names = ['RAINA','KOUL','BHAT','GUPTA','SHARMA','KHAN','MEHRA','VERMA','KUMAR','BACHHAN','KAPOOR','KHER','SINGH',
              'KOUR','SEVTA','MEHTA','GABA','ARORA','PACHIALA','KHAJURIA','PANDEY','DUBEY','SHUKLA','AGARWAL','BANSAL','CHAUDHARY',
              'CHOUHAN']

streets = ['LAMBI GALI','KACHA DANGA','FARIDABAD ROAD','YAMUNA EXPRESSWAY','MANDIR ROAD','DLF PHASE 1','DLF PHASE 2','MUNICIPAL ROAD']

landmarks = ['ERICSSON BUILDING','SHIP BUILDING','HANUMAN MANDIR','GURUDWARA','CHURCH','GARDEN','FACTORY','DAIRY']

localities = ['DLF CYBER CITY','PARYAVARAN COMPLEX','ROOP NAGAR','DURGA NAGAR','MAYUR VIHAR','ASHOK NAGAR','PREET VIHAR','TATA NAGAR']

districts = ['UDHAMPUR','JAMMU','GURUGRAM','NOIDA','MEHRAULI','BHIWANI','HISAR','SIRSA','ROHTAK','AMBALA']

states = ['JAMMU AND KASHMIR','DELHI','HARYANA','UTTAR PRADESH','HIMACHAL PRADESH']

documents = []

START = datetime.datetime.utcnow()

for i in range(10001):
    _id = random.choice(['1234','5678','2345','6789']) + random.choice(['4321','8765','5489','2367']) + str(random.randrange(1000,9999))
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    registered_on = datetime.datetime.utcnow() - datetime.timedelta(random.randrange(1,1000))
    pan_card = str(random.choice(first_names)[1:4]) + str(random.choice(last_names)[-2:]) + str(random.randint(100,999)) + str(random.choice(['M','N','P','R']))
    phone_number1 = str(random.randint(1000,9999)) + str(random.randint(5000,9999)) + str(random.randint(10,99))
    phone_number2 = str(random.randint(1000,9999)) + str(random.randint(5000,9999)) + str(random.randint(10,99))
    house_no = str(random.randint(100,999)) + str(random.choice(['A','B','C','D']))
    street = random.choice(streets)
    landmark = random.choice(landmarks)
    locality = random.choice(localities)
    district = random.choice(districts)
    state = random.choice(states)
    pincode = str(random.randint(10,99)) + str(random.randint(10,99)) + str(random.randint(10,99))
    longitude = 77.0577449 + (random.randrange(-1000,9999)/10000)
    latitude = 28.4967829 + (random.randrange(-1000,9999)/10000)
    doc = {
                "_id" : _id,
                "first_name" : first_name,
                "last_name" : last_name,
                "registered_on" : registered_on, #DATE
                "pan_card" : pan_card,
                "phone_numbers" : [         #ARRAY
                    phone_number1,phone_number2
                ],
                "permanent_address" : {     #SUB-DOCUMENT / EMBEDDING
                    "house_no" : house_no,
                    "street" : street,
                    "landmark" : landmark,
                    "locality" : locality,
                    "district" : district,
                    "state" : state,
                    "pincode" : pincode,
                    "map" : {
                        "location" : {          #GEO-JSON
                            "type" : "Point",
                            "coordinates" : [
                                longitude,
                                latitude
                            ]
                        }
                    }
                }
            }

    documents.append(doc)

    if i%1000 == 0:
        try:
            collection.insert_many(documents,ordered=False)
            print("Number of documents " + str(collection.count()))
            documents = []
        except Exception as e:
            print(e)

    print(" === Progress === " + str(i / 100) + "%")

END = datetime.datetime.utcnow()

print("Time taken by this program: "+str(END-START))

