import geopandas as gpd
from bs4 import BeautifulSoup
import requests
import os
from typing import Literal
from concurrent.futures import ThreadPoolExecutor

# shp_path = r'C:\RUIZ\NMS\state_boundary\stats_itrf92.shp'

# stats_file = gpd.read_file(shp_path, crs='EPSG:6362')
# gto_stats = stats_file[['Name', 'geometry']]

# gto_stats.to_file('stats_gto_itrf92.gpkg', driver='GPKG')

# points = gpd.read_file('stats_gto_itrf92.gpkg')
# kml_path = r'C:\RUIZ\NMS\StatsUnzipped\doc.kml'

def seekData(kml:str, shp:str, **kwargs):
    # Open and read kml file content
    with open(kml, 'r', encoding='utf-8') as kml:
        content = kml.read()
    # Parse content as xml content
    xml_content = BeautifulSoup(content, 'xml')
    
    stats2seek = gpd.read_file(shp)
    
    with ThreadPoolExecutor(max_workers=2) as exec:
        stats2seek['Name'].apply(lambda row: exec.submit(getData, xml_content, stat=row, **kwargs))
    # stats2seek['Name'].apply(lambda row: getData(xml_content, stat=row, **kwargs))




def getURL(xml, stat:str, period:Literal['DIARIOS', 'MENSUALES', 'EXTREMOS', 'NORMALES_1961_1990', 'NORMALES_1971_2000', 'NORMALES_1981_2010', 'NORMALES_1991_2020']):
    # Look for all SimpleData tags with content equal to station number
    target = xml.find_all('SimpleData', text=stat)
    try:
        assert target.__len__() == 1
    except AssertionError:
        print('There is more than one SimpleData tag that contains %s'%(stat))
    else:
        # Get the complete SchemaData tag
        schemaData = target[0].find_parent('SchemaData')
        # Get the URL for monthly data
        link = schemaData.find('SimpleData', {'name':period}).text
        return link




def export(content:str, stat:str, directory='RawData', **kwargs):
    if not os.path.exists((directory)):
        os.mkdir(directory)
    filename = '%s.txt'%(stat)
    
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(content)




def getData(kml, **kwargs):
    url = getURL(xml=kml, **kwargs)
    response = requests.get(url)
    try:
        assert response.status_code == 200
    except AssertionError:
        print('Failed the connection')
    else:
        data = BeautifulSoup(response.content, 'html.parser').text
        # data = content.find('pre').text
        export(content=data, **kwargs)




if __name__=='__main__':
    seekData(r'C:\RUIZ\NMS\StatsUnzipped\doc copy.kml', r'C:\RUIZ\NMS\stats.gpkg', period='MENSUALES')
    print('DONE!!')