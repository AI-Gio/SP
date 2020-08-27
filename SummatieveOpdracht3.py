import psycopg2
import csv
import random
from tqdm import tqdm
import pymongo
# bron: https://www.psycopg.org/docs/usage.html
conn = psycopg2.connect("dbname=recommendation_engine user=postgres password= D@t@b@s3!")
cur = conn.cursor()

"""__==.. Database section ..==__"""
def PostgresExecute(query):
    cur.execute(f"{query}")
    conn.commit()

def CreateTable(TableName, DBcolumn1name, DBcolumn2name):
    try:
        PostgresExecute(f"""
        CREATE TABLE {TableName} (
        {DBcolumn1name} varchar(64),
        {DBcolumn2name} varchar(64)
        );
        """)
    except:
        print("Sorry but this table already exists")

def DropTable(Table):
    try:
        PostgresExecute(f"DROP TABLE {Table}")
    except:
        print("Sorry this table doesn't exist or you have spelled it incorrect")

"""__==.. Content Recommendation section ..==__"""

def ProductsCSV():
    """
    Collects the id and its sub category from postgresql and puts it in a csv file
    """
    cur.execute("select _id, sub_category from products where sub_category is not NULL")
    dataset = cur.fetchall()
    with open("productsrec.csv", "w", newline='') as f:
        thewriter = csv.writer(f)
        for values in dataset:
            thewriter.writerow([values[0],values[1]])

def ProductSortSubCat():
    """
    Sorts all of the products by sub category
    :return: A dictionary with all of the sub categories with the products that belong to that sub category
    """
    SameSubCatDict = {}
    with open('productsrec.csv') as f:
        reader = csv.reader(f, delimiter=",")
        for row in reader:
            if row[1] not in SameSubCatDict:
                SameSubCatDict[row[1]] = [row[0]]
            elif row[1] in SameSubCatDict:
                SameSubCatDict[row[1]].append(row[0])
    return SameSubCatDict

def ContentRecDict():
    """
    Gives every product a list of 4 products that are in the same sub category
    :return: Dictionary with products with lists of 4 recommendations
    """
    RecDict = {}
    SortedDict = ProductSortSubCat()
    for subcat in SortedDict:
        for prod in SortedDict[subcat]:
            RecDict[prod] = random.choices(SortedDict[subcat], k=4)
    return RecDict

def toPostgres(TableName, RecommendDict, DBcolumn1name, DBcolumn2name):
    """
    Puts all of data into Postgresql database
    :param TableName: Name of the table
    :param RecommendDict: Requires a Dictionary with string as key and list with strings as value
    :param DBcolumn1name: Name of the first column in the table
    :param DBcolumn2name: Name of the second column in the table
    """
    RD = RecommendDict
    for key in tqdm(RD):
        for value in RD[key]:
            try:
                PostgresExecute(f"""
                INSERT INTO {TableName} ({DBcolumn1name}, {DBcolumn2name})
                VALUES ('{key}','{value}');
                """)
            except:
                continue


"""__==.. Collaborative Recommendation section ..==__"""
def HighestFreq(lst):
    """
    Calculates the item in a list that is most frequent
    :param lst: takes a list with items
    :return: The item that is most frequent out of the list
    """
    freq = {}
    for item in lst:
        if item in freq:
            freq[item] += 1
        else:
            freq[item] = 1
    # bron:https://kite.com/python/answers/how-to-find-the-max-value-in-a-dictionary-in-python#:~:text=Use%20max()%20and%20dict.,paired%20with%20the%20max%20value.
    max_key = max(freq, key=freq.get)
    return max_key

def PrevRec2CSV():
    """
    puts profiles with previously recommended products in csv file
    This function is necessary, to put the data into a csv file,
    transferring it too a database would take too long
    """
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["huwebshop"]
    mycol = mydb["profiles"]
    PrevRecomDict = {}
    with open('prev_rec', 'w', newline='') as f:
        thewriter = csv.writer(f)
        for x in mycol.find({"previously_recommended":{'$exists':True}}):
            PrevRecomProd = x['previously_recommended']
            if len(PrevRecomProd) != 0:
                for prod in PrevRecomProd:
                    thewriter.writerow([str(x['_id']), prod])

def PreviouslyRecomDict():
    """
    Retrieves information from csv file and writes all profile id's with previously recommended products in a dictionary
    Takes about 35 seconds to create dictionary
    :returns: Dictionary with key = profile id and value = list of product id's
    """
    PRD = {}
    with open('prev_rec') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if row[0] not in PRD:
                PRD[row[0]] = [row[1]]
            elif row[0] in PRD:
                PRD[row[0]].append(row[1])
    return PRD


def ProductWithSubCatDict():
    """
    Retrieves the information from csv file and makes a dictionary with products and its sub category
    :return: Dictionary with key = product id and value = sub category
    """
    PWSCD = {}
    with open('productsrec.csv') as f:
        reader = csv.reader(f, delimiter=",")
        for x, row in enumerate(reader):
            if x == 0:
                continue
            elif row[1] != "":
                PWSCD[row[0]] = row[1]
    return PWSCD

def SubCatProfileDict():
    """
    Creates a dictionary with all of the sub categories with profile id's
    that have that sub category as most frequent sub category of their products
    :return: Dictionary with key = Sub Category and value = list of profile id's
    """
    PRD = PreviouslyRecomDict()
    PWSCD = ProductWithSubCatDict()
    SCPD = {}
    for prof in tqdm(PRD):
        lst = []
        for prod in PRD[prof]:
            try:
                lst.append(PWSCD[prod])
            except:
                continue
        if lst == []:
            continue
        elif HighestFreq(lst) not in SCPD:
            SCPD[HighestFreq(lst)] = [prof]
        elif HighestFreq(lst) in SCPD:
            SCPD[HighestFreq(lst)].append(prof)
    return SCPD

def CollabRecCSV():
    """
    Creates a csv file with profile id's and their recommended products
    based on similar profiles their previously recommended products
    """
    SCPD = SubCatProfileDict()
    PRD = PreviouslyRecomDict()
    with open('profrec.csv', 'w',newline='') as f:
        writer = csv.writer(f, delimiter=";")
        for SubCat in tqdm(SCPD):
            for prof in SCPD[SubCat]:
                RecProf = random.choice(SCPD[SubCat])
                RecProds = random.choices(PRD[RecProf], k=4)
                for item in RecProds:
                    writer.writerow([prof, item])

def CSVtoPostgres(TableName, DBcolumn1name, DBcolumn2name):
    """
    Retrieves profiles and its recommended products from csv file and puts it in postgresql
    :param TableName: Name of the table
    :param DBcolumn1name: Name of the first column in the table
    :param DBcolumn2name: Name of the second column in the table
    """
    with open('profrec.csv') as f:
        reader = csv.reader(f, delimiter=";")
        pbar = tqdm(total=4147136)
        for row in reader:
            pbar.update(1)
            PostgresExecute(f"""
                            INSERT INTO {TableName} ({DBcolumn1name}, {DBcolumn2name})
                            VALUES ('{row[0]}','{row[1]}');
                            """)
        pbar.close()

# run this first
# ProductsCSV()
# CollabRecCSV()

# run this after
# CreateTable("ContentRec", "Product", "RecommendedProd")
# toPostgres("contentrec", ContentRecDict(), 'Product', 'RecommendedProd')
# CreateTable("CollabRec", "Profile", "RecommendationProd")
# CSVtoPostgres("CollabRec", "Profile", "RecommendationProd")

# run this to drop the tables
# DropTable("contentrec")
# DropTable("CollabRec")

cur.close()
conn.close()
