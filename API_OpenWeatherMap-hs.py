#!/usr/bin/env python
# coding: utf-8

# # Application : Meteo en ligne via l’API de OpenWeatherMap
# OpenWeatherMap est un service en ligne qui fournit des données météorologiques, y compris des données météorologiques actuelles, des prévisions et des données historiques aux développeurs de services Web et d'applications mobiles.
# 

# In[147]:


#Image(url= "weather.png")
#get_ipython().system('pip install termcolor')


# 
# 
# Dans cet atelier, nous exploitions cette api afin d’extraire les données météorologique sous forme JSON et on le stocke dans un fichier CSV respectant un format bien défini.
# #### Pour cela nous suivons les étapes suivantes
# Depuis 2015, utiliser openweathermap nécessite de s'enregistrer. On s'authentifie ensuite en utilisant une clé. Chaque requête doit sonc être complétée par : &appid=XXXXX où XXX est la valeur de la clé à utiliser. Cette clé est disponible sur Updago.
# Nous allons utiliser le service OpenWeatherMap pour récolter des prévisions météo. Ces prévisions pourront être resservies par un site Web, par exemple.
# Le site http://openweathermap.org propose une API pour récolter les informations qu'il diffuse.
# Prendre connaissance des possibilités de l'API
# La page de documentation de l'API est accessible ici : http://openweathermap.org/api
# #### Voici quelques exemples :
# http://api.openweathermap.org/data/2.5/weather?q=Tunis,Tunisia&appid=XXX
# Remplacez XXX par la clé API. Dans la suite, pensez à ajouter le champ appid=XXX.
# Remplacer ?q=Tunis,Tunisia par la villes et le pays recherchés.
# La commande weather employée ici indique que nous souhaitons obtenir les conditions météo actuelles.
# Le résultat est par défaut fourni au format json. Dans le cas de la commande weather, voici les informations qui sont obtenues : Weather Data
# Il y a bien sûr d'autres commandes disponibles, qu'on trouvera dans la documentation. Notez en particulier la commande forecast qui permet d'obtenir des prévisions.
# ## Outils Python
# #### Accès à l'API
# Comme indiqué dans le cours, nous allons utiliser Python pour interroger openweathermap. Le module requests sera utilisé à la place d'urllib 
# 

# #### Chargement des modules importantes
# 

# In[148]:


#from IPython.display import Image
#from IPython.core.display import HTML
import os
import csv
from termcolor import colored, cprint
import datetime
import json
import urllib.request
import pandas as pd
#os.chdir("C:\\users\\msellami\\PythonTraining\\")


# #### Configuration de API et genration de url d'accès à l'API
# Définition d'un fonction qui prend en parametres id de la ville, la ville, le pays pour generer un url respectant l'appel de l'<b>API OpenWeatherMAP</b> et aussi configurer l'APPID recuperé après enregistrement sur le site.
# En effet, l'accès à l'api est effectué soit en utilisant ID de la ville (Tunis) récuperé via la site ( https://openweathermap.org/city/2464470) ou via un fichier json conteant la liste des pays et leur villes accessible via http://bulk.openweathermap.org/sample/city.list.json.gz.
# #### Accès direct avec ID Ville/Pays
# *- http://api.openweathermap.org/data/2.5/weather?id=2464470&mode=json&units=metric&APPID
# #### Accès avec recherche de Ville et Pays
# *-  http://api.openweathermap.org/data/2.5/weather?q=Tunis,Tunisia&mode=json&units=metric&APPID
# 
# Il faut specifier aussi les données sous forme JSON ou XML et l'unité de temperature (°C,F).
# Pour Fahrenheit, on utilise unité=imperial, pour Celsius, on utilise unité= metric, et  par defaut Kelvin. 

# In[149]:


def url_builder(city_id,city_name,country):
    user_api = '0e7a872c16502901b3548b5b13bf0250'  # Obtain yours form: http://openweathermap.org/
    unit = 'metric'  # For Fahrenheit use imperial, for Celsius use metric, and the default is Kelvin.
    if(city_name!=""):
        api = 'http://api.openweathermap.org/data/2.5/weather?q=' # "http://api.openweathermap.org/data/2.5/weather?q=Tunis,fr
        full_api_url = api + str(city_name) +','+ str(country)+ '&mode=json&units=' + unit + '&APPID=' + user_api
    else:
        api = 'http://api.openweathermap.org/data/2.5/weather?id='     # Search for your city ID here: http://bulk.openweathermap.org/sample/city.list.json.gz
        full_api_url = api + str(city_id) + '&mode=json&units=' + unit + '&APPID=' + user_api
   
    return full_api_url


# Maintenant on passe à definir une fonction qui permet de récuperer le fichier JSON a partir de cette URL en utilisant <b>urllib.request.urlopen()</b>, <b>str.read.decode('utf-8')</b> pour l'encodage et <b>json.load() </b>pour charger une structire <b>SJON</b> a partir des fichier

# In[150]:


def data_fetch(full_api_url):
    url = urllib.request.urlopen(full_api_url)
    output = url.read().decode('utf-8')
    raw_api_dict = json.loads(output)
    url.close()
    return raw_api_dict


# ### Convertion et formatage de heure et date
# Definir une fonction qui permet de convertir un timestamp en Heure de la forme HH:MM AM/PM

# In[151]:


def time_converter(time):
    converted_time = datetime.datetime.fromtimestamp(int(time)).strftime('%I:%M %p')
    return converted_time


# ### Extraction des champs à partir des fichiers JSON 
# Mainetant, nous avons besoins de creer une fonction <b>data_organizer</b> qui prend une structure json complexe et créee une dictionnaire de données
# contenant les attributs suivants:
# <b>
#     
# *-city : La ville
#     
# *-country: le pays
# 
# *-temp: Temperature actuelle
# 
# *-temp_max: Temperature temp_max
# 
# *-temp_min :Temperature Min 
# 
# *-humidity ; Humidité
# 
# *-pressure ; Pression
# 
# *-sky       : Etat de Ciel 
# 
# *-sunrise  : Lever du soleil  
#  
#  
# *-sunset : Coucher du soleil 
# 
# *-wind : Vistesse de Vent
# 
# *-wind_deg
# 
# *-dt : Date
# 
# *-cloudiness : Nuageux
# 
# </b>
# 

# In[152]:


def data_organizer(raw_api_dict):
    data = dict(
        city=raw_api_dict.get('name'),
        country=raw_api_dict.get('sys').get('country'),
        temp=raw_api_dict.get('main').get('temp'),
        temp_max=raw_api_dict.get('main').get('temp_max'),
        temp_min=raw_api_dict.get('main').get('temp_min'),
        humidity=raw_api_dict.get('main').get('humidity'),
        pressure=raw_api_dict.get('main').get('pressure'),
        sky=raw_api_dict['weather'][0]['main'],
        sunrise=time_converter(raw_api_dict.get('sys').get('sunrise')),
        sunset=time_converter(raw_api_dict.get('sys').get('sunset')),
        wind=raw_api_dict.get('wind').get('speed'),
        wind_deg=raw_api_dict.get('deg'),
        dt=time_converter(raw_api_dict.get('dt')),
        cloudiness=raw_api_dict.get('clouds').get('all')
    )
    #print (data)
    return data


# # Recuperer des coordonnées des villes à partir de json avec pandas
#   Maintenant on a besoin d'une fonction qui permet recuperer les coordonnées des villesd'un fichier JSON 
#   

# In[153]:


def WriteCSV(data):
    with open('weatherOpenMap.csv', 'a') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, data.keys())
        #w.writeheader()
        w.writerow(data)


# In[154]:


villes = pd.read_json("city.list.json")
villes_france = villes[villes['country'] == 'FR' ]
villes_france = villes_france.reset_index() 
villes_france = villes_france[['id','name','country','coord']]
villes_france.tail(5)


# In[155]:


ids_france       = list(villes_france ['id'])
countries_france = list(villes_france ['country'])


# In[156]:


if __name__ == '__main__':
    try:
        for i in range(10):
            #start = datetime.now()
            import time 
            #if i in [j for j in range(2,10,2)]:  #duration = datetime.now()- start
            #    print(i, "=> delay of 1 seconds")
            #    time.sleep(1)
            #country   = 'France'
            #city_name =  countries_france[i]
            city_id    =  ids_france[i]
            #Generation de l url
            print(colored('Generation de l url ', 'red',attrs=['bold']))
            url=url_builder(city_id,'','')
            #Invocation du API afin de recuperer les données
            print(colored('Invocation du API afin de recuperer les données', 'red',attrs=['bold']))
            data=data_fetch(url)

            data_orgnized=data_organizer(data)
            #Enregistrement des données à dans un fichier CSV 
            WriteCSV(data_orgnized)
        print("Les temperatures de toutes les villes sont scrappees :D")
    except IOError:
        print('no internet')
        print("Seullement " + str(i) + " villes ont pu étre scrapper")

df = pd.read_csv("weatherOpenMap.csv",
                     usecols    = [i for i in range(0,11)], 
                     names      = ['city','country','Temperature','temp_max','temp_min','humidity','pressure','sky','sunrise','sunset','wind_speed'],    
                     index_col  = None)
df.to_csv("weatherOpenMap.csv",index = False) 
df      


# In[146]:


#get_ipython().system('pip install cassandra-driver')
#!pip install pandas==0.20.0.
#get_ipython().system('pip install pandas')


# In[10]:


import pandas as pd
import warnings


# In[157]:


df = pd.read_csv("weatherOpenMap.csv")
df


# In[101]:



df = df [['city','Temperature','temp_max' ,'temp_min']]
#df=df.head(21)
df


# In[4]:


#from pandas_profiling import ProfileReport
#profile = ProfileReport(df, title="Résumé Descriptif", explorative=True)
#profile


# In[5]:


df.describe().transpose()


# In[33]:


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
CASSANDRA_HOST = ['192.168.64.4'] 
CASSANDRA_PORT = 9042
CASSANDRA_DB = "weather"
CASSANDRA_TABLE = "countries_france"
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
def pandas_factory(colnames,rows):
    return pd.DataFrame(rows,columns=colnames)
#on force ici a repecter le datframe de pandas lors de la recuperation des données
try:
    cluster = Cluster(protocol_version=3,
                      contact_points=CASSANDRA_HOST,
                      load_balancing_policy=None,
                      port=CASSANDRA_PORT,
                      auth_provider=auth_provider)
    session = cluster.connect()
except ValueError:
    print("Oops!  échec de connexion cluster.  Try again...")


# In[2]:


#creation du key space
session.execute("CREATE KEYSPACE IF NOT EXISTS weather_frApp WITH REPLICATION={'class':'SimpleStrategy','replication_factor':3};")


# In[126]:



session.execute("create table weather_frApp.weather_fr(city TEXT primary key, weather map<text,float>);")
#session.execute("insert into weather_frApp.weather_fr(city,weather) values ('Paris',{'temp_max': '90','temp_min': '90'});")


# In[127]:


rows=session.execute('Select * from weather_frApp.weather_fr;')
df_results = rows._current_rows
df_results


# In[132]:


df_city = df.drop_duplicates(subset = ['city'])[['city']]
df_city['city'] = df_city['city'].str.replace("'",' ')
df_city = df_city.head(10)
df


# In[133]:


query_insert="INSERT INTO weather_frApp.weather_fr (city,weather) VALUES ({},{});"
#session.execute("insert into weather_frApp.weather_fr(city,weather) values ('Paris',{'temp_max': '90','temp_min': '90'});")


# In[134]:


for ct in df_city.index:
        row_dict = {}
        row_dict["Temperature"] = df['Temperature'][ct]
        row_dict["temp_max"]    = df['temp_max'][ct]
        row_dict["temp_min"]    = df['temp_min'][ct]
        CQL_query = query_insert.format("'"+df_city['city'][ct]+"'",row_dict)
        print(CQL_query)
        session.execute(CQL_query)


# In[135]:


#Executer une requete pour tester
rows = session.execute('SELECT * FROM weather_frApp.weather_fr; ')
df_cities = rows._current_rows
df_cities

