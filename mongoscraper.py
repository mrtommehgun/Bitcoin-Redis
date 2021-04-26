import requests, re, time, pymongo,redis,json
from bs4 import BeautifulSoup
#setting up Redis connection
conn=redis.Redis('localhost')

while True:
    cmc = requests.get("https://www.blockchain.com/btc/unconfirmed-transactions")
    soup = BeautifulSoup(cmc.text,"html.parser")
    HASHLIST=soup.findAll("a",attrs={'class':"sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK"})
    BTCDOLLIST=soup.findAll("span",attrs={'class':"sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC"})

    #MongoDB
    #myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    #ScrapDB = myclient["Scrap"]
    #col = ScrapDB["BTC"]

#variabelen declareren
#initiele lijsten
    hashlijstje=[]
    Transactielijst=[]

#iterators voor de verschillende lijsten + lijsten, dictionaries
    tijdteller=0
    bitteller=1
    usdteller=2
    tijdstip=[]
    bitcoinlijst=[]
    usdlijst=[]
    HASHDICBTC={}
    HASHDICUSD={}

#regexfilter voor html elements weg te filteren
    htmlfilter="(<)(?<=<)(.+?)(?=>)(>)"
#hashes filteren
    for HashItem in HASHLIST:
        Hashre = re.sub(htmlfilter," ", str(HashItem))
        hashlijstje.append(Hashre.strip(" "))
#Listing the parts of transaction (and appending them to seperate lists)
    for BTCINFO in BTCDOLLIST:
        BTCre = re.sub(htmlfilter," ",str(BTCINFO))
        Transactielijst.append(BTCre.rstrip(" ").lstrip(" ").lstrip("$"))
#wegens formatprobleem, heb ik de 1000-separator verwijderd
    kommafilter=","
    for i in range(int(len(Transactielijst)/3)):
        tijdstip.append(Transactielijst[tijdteller])
        bitcoinlijst.append(Transactielijst[bitteller])
        filtergetal=re.sub(kommafilter,"",Transactielijst[usdteller])
        usdlijst.append(float(filtergetal))
        try:
            tijdteller+=3
            bitteller+=3
            usdteller+=3
        except:
            break

#Assigning bitcoin and USD values to the hash so comparing is easier   
    for i in range(len(hashlijstje)):
        #print(hashlijstje[i],"met waarde:",bitcoinlijst[i])
        HASHDICBTC[hashlijstje[i]]=bitcoinlijst[i]
        HASHDICUSD[hashlijstje[i]]=usdlijst[i]


#looking for the highest value from the USD and BTC list and comparing them to the hash in the dictionary

    usdlijst.sort()

    #print("Hoogste waarde: ",usdlijst[-1],"om {} GMT ".format(tijdstip[0]))
    for key, value in HASHDICUSD.items():
        if usdlijst[-1]==HASHDICUSD[key]:
            print("deze hash:",key,"met waarde:",value,"in USD en dit in bitcoin:",HASHDICBTC[key],"en is op {} GMT opgehaald".format(tijdstip[0]))
            #naar logfile afprinten!
            f = open("transaction.log","a")
            s = open("transaction.json","a")
            tekst= "[{}]".format(tijdstip[0])+" De hash: {} is de grootste transactie deze minuut met een waarde van {} en is omgerekend ${}\n".format(key,HASHDICBTC[key],value)
            jsontext="[{\"hash\":\""+key+"\",\"BTC\":\""+HASHDICBTC[key]+"\",\"USD\":"+str(value)+",\"Time\":\""+tijdstip[0]+"\"}]"
            f.write(str(tekst))
            s.write(str(jsontext))
            mongoformat={"hash":key,"BTC":HASHDICBTC[key],"USD":str(value),"Time":tijdstip[0]}
            f.close()
            s.close()

            #MongoDB Compass
            #x = col.insert_one(mongoformat)
            #REDIS
            for key, val in mongoformat.items():
                conn.hset("big", key, val)
            
            
            print(conn.hgetall("big"))
    time.sleep(60)
    print("slaaptijd is over, let's go again!")