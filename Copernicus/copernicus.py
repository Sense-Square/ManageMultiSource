from sentinelsat import read_geojson, geojson_to_wkt
from datetime import datetime,timedelta,timezone
from netCDF4 import Dataset
import numpy as np
import os
from builtins import dir
import cdsapi
import json
import time

errorilist=[]

# creazione della directory per la sorgente Copernicus

c=''
dir = os.path.join(c,"Dati_Copernicus")
if not os.path.exists(dir):
    os.mkdir(dir)

# Barra di caricamento
def loadingBar(i, total, title):
    if i == 0:
        print("\n\n" + title + "\n")
    percentage = round((100*i)/total, 2)
    blocks = int(percentage)//5
    complete = ""
    for i in range(blocks):
        complete += "█"
    for i in range(20 - blocks):
        complete += "-"
    print("Progress: |{}| {}% Complete".format(complete, percentage), end="\r")

#RICHIESTA DEL FILE NC CON FILE GEOJSON

def richiesta_geo(path_file, data_inizio, data_fine, time):
    os.chdir("Dati_Copernicus") #CAMBIO DIRECTORY PER SALVARE IL FILE NC SCARICATO

    lista = geojson_to_coordinate(path_file)

    data = datetime.now()
    delay= timedelta(hours=int(time.split(":")[0]))
    timestampDatetime= data + delay

    namefile = timestampDatetime.strftime("%Y-%m-%d %H")
    file_nc = namefile + ':00' + '.nc'

    c = cdsapi.Client()

    c.retrieve(
        'cams-europe-air-quality-forecasts',
        {
            'variable': [
                'carbon_monoxide', 'nitrogen_dioxide', 'ozone',
                'particulate_matter_10um', 'particulate_matter_2.5um', 'sulphur_dioxide',
             ],
            'model': 'ensemble',
            'date': data_inizio+'/'+data_fine,
            'format': 'netcdf',
            'level': '0',
            'type': 'forecast',
            'time': '00:00',
            'leadtime_hour': int(time.split(":")[0]),
            'area': [
                lista[0], lista[1], lista[2],
                lista[3],
            ],
        },
        file_nc)
    export_to_json(file_nc, namefile, data_inizio, data_fine, time)


# RICHIESTA DEL FILE NC

def richiesta(data_inizio, data_fine, lat, long, time):

    namefile = data_inizio+' '+time
    file_nc = namefile+'.nc'

    c = cdsapi.Client()

    c.retrieve(
        'cams-europe-air-quality-forecasts',
        {
            'model': 'ensemble',
            'date': data_inizio+'/'+data_fine,
            'format': 'netcdf',
            'level': '0',
            'variable': [
                'carbon_monoxide', 'nitrogen_dioxide', 'ozone',
                'particulate_matter_10um', 'particulate_matter_2.5um', 'sulphur_dioxide',
             ],
            'type': 'forecast',
            'time': time,
            'leadtime_hour': int(time.split(":")[0]),
            'area': [
                lat+0.1, long, lat,
                long+0.1,
            ],
        },
        file_nc)
    export_to_json(file_nc, namefile, data_inizio,data_fine, time)    

#SCRITTURA DEI VALORI IN UN FILE JSON

def export_to_json(nc_file, nfile, date_begin, date_end, ora):

    dir = os.getcwd()
    file_dir = os.path.join(dir, nfile+'.json')
    
    fh = Dataset(nc_file, mode='r')  # lettura del file nc in un dataframe

    #SI PRELEVANO I DATI DI LATITUDINE E LONGITUDINE PER L'INQUINANTE
    lat=fh.variables['latitude'][:]
    long=fh.variables['longitude'][:]

    lista = []
    a_file = open(file_dir, "w")

    dim_lo =long.size
    dim_la = lat.size
    
    co=fh.variables['co_conc'][:]
    no2=fh.variables['no2_conc'][:]
    o3=fh.variables['o3_conc'][:]
    pm10=fh.variables['pm10_conc'][:]
    pm25=fh.variables['pm2p5_conc'][:]
    so2=fh.variables['so2_conc'][:]

    insert(co, no2, o3, pm10, pm25, so2, lat, long, dim_la, dim_lo, lista, date_begin, date_end, ora)
    
    json.dump(lista,a_file)

    a_file.close()

    

def insert(co, no2, o3, pm10, pm25, so2, lat, long, size_lat, size_lon, lista, d_inizio, d_fine, ora):

    la=0
    lo=0
    
    time = datetime.now()
    delay= timedelta(hours=int(ora.split(":")[0]))

    for c, n, o, p, q, s in np.nditer([co, no2, o3, pm10, pm25, so2]):

        diz = {}
        try:
            
            if(round(float(n), 2)>0 or round(float(c)/1000, 2)>0 or round(float(o), 2)>0 or round(float(p), 2)>0 or round(float(q), 2)>0 or round(float(s), 2)>0):
                timestampDatetime=time+delay
                seconds = datetime.timestamp(timestampDatetime.replace(tzinfo = timezone.utc)) + 3600
                diz['timestamp'] = int(seconds)
                diz['latitude'] = round(float(lat[la]),2)
                diz['longitude'] = round(float(long[lo]),2)
                if(round(float(c)/1000, 2)>0):
                    diz['co'] = round(float(c)/1000, 2)
                if(round(float(n), 2)>0):
                    diz['no2'] = round(float(n), 2)
                if(round(float(o), 2)>0):
                    diz['o3'] = round(float(o), 2)
                if(round(float(p), 2)>0):
                    diz['pm10'] = round(float(p), 2)
                if(round(float(q), 2)>0):
                    diz['pm2_5'] = round(float(q), 2)
                if(round(float(s), 2)>0):
                    diz['so2'] = round(float(s), 2)
            
                # diz['nazione'] = query['nazione']
                # diz['regione'] = query['regione']
                # diz['provincia'] = query['provincia']
                # diz['comune'] = query['comune']
                # diz['squareID'] = query['squareID']
                diz['ID'] = 'copernicus'
                diz['fonte'] = 'CP'
                diz['tp'] = 0.8
                diz['sn'] = 1
            
                lista.append(diz)

        except:
            print('errore')

 
        lo=lo+1
        if(lo >= size_lon and la < size_lat):
            lo=0
            la=la+1



# LA FUNZIONE RAGIONA SUL FATTO CHE UN FILE GEOJSON ,COSTRUITO SELEZIONANDO UN'AREA QUADRATA O RETTANGOLARE,
# CI RESTITUISCE 5 COPPIE DI COORDINATE IN CUI L'ULTIMA È UGUALE ALLA PRIMA. PER LA NOSTRA REQUEST
# SERVONO SOLO LAT E LONG INIZIALE E FINALE, QUINDI 4 VALORI. QUESTI SONO : PRIMO E TERZO PER LATITUDINE 
# E SECONDO SESTO PER LONGITUDINE

def geojson_to_coordinate(file_geojson):

    footprint = geojson_to_wkt(read_geojson(file_geojson)) #SI INTENDE IL PATH

    #ELIMINO DAL FOOTPRINT I CARATTERI SUPERFLUI E FACCIO UNO SPLIT 
    
    stringa1 = footprint.replace(",", " ")
    stringa2 = stringa1.replace("POLYGON", "")
    stringa3 = stringa2.replace("((", "")
    new_footprint = stringa3.replace("))", "")

    x = new_footprint.split(' ')

    lista = []

    #RACCHIUDO GLI ELEMENTI IN UNA LISTA

    for i in x :

        lista.append(float(i))

    #RIMUOVIAMO GLI ULTIMI 2 ELEMENTI DELLA LISTA CHE SONO UNA COPIA DELLA PRIMA COPPIA

    lista.pop() 
    lista.pop()
    
    #ANDIAMO A PRENDERCI I 4 ELEMENTI CHE CI SERVONO 

    coordinate = []

    coordinate.append(lista[5])
    coordinate.append(lista[0])
    coordinate.append(lista[1])
    coordinate.append(lista[2])

    return coordinate
        
if __name__ == '__main__':
    
    try:
        today=datetime.now()
        todaystr=today.strftime("%Y-%m-%d")
        hour=today.strftime("%H")

        print(today)

        for i in range(0,24):
            hour2=str(i)+":00"
            #futures.append(executor.submit(richiesta_geo,"/home/algorithm/Scrivania/MultiSorgente/Multisorgente/Copernicus/italia.geojson", todaystr, todaystr, hour2))
            richiesta_geo("italia.geojson", todaystr, todaystr, hour2)
            #Terminazione Thread
            index=0
            time.sleep(120)
            #for future in concurrent.futures.as_completed(futures):
            print(datetime.now())
            print("COMPLETED")
        
        
    except:
        print("ERRORE")

    
