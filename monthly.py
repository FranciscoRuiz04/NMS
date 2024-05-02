import pandas as pd
import shelve
import os
from typing import Literal



___author__ = 'B.E. Francisco Ruiz'
__status__ = 'Data Analyst'
__email__ = 'fruizeng04@gmail.com'

__credits__ = 'Guanajuato University'
__version__ = '1.0.0'




__phrases__ = ('LLUVIA MÁXIMA 24H',
               'LLUVIA TOTAL MENSUAL',
               'EVAPORACIÓN MENSUAL',
               'TEMPERATURA MÁXIMA EXTREMA',
               'TEMPERATURA MÁXIMA PROMEDIO',
               'TEMPERATURA MÍNIMA EXTREMA',
               'TEMPERATURA MÍNIMA PROMEDIO',
               'TEMPERATURA MEDIA MENSUAL')

__topics__ = ('pmax24',
              'p',
              'evo',
              'tmax_x',
              'tmax_mean',
              'tmin_x',
              'tmin_mean',
              't')

__dbname__ = 'meteodata'

__colnames__ = ['year',
                'jan',
                'feb',
                'mar',
                'abr',
                'may',
                'jun',
                'jul',
                'ago',
                'sep',
                'oct',
                'nov',
                'dec',
                'acum',
                'mean',
                'months_amount',
                'stat']



class TabularFile:
    __ct__ = None
    glosary = {__phrases__[i]:__topics__[i] for i in range(0, 8)}
    
    
    def __init__(self, file_path:str, sep='\t', encoding='utf-8'):
        """Multidimentional text file with data about one or more
        topics.
        This class is especially designed to interpret data from
        the monthly meteorological normals provided in the official
        web portal of the National Meteorological Service of Mexico.

        Args:
            file_path (str): path to the file with the data.
            sep (str, optional): pattern to consider as field separator.
            Defaults to '\t'.
            encoding (str, optional): encoding of the file. Defaults to 'utf-8'.
        """
        self.filepath = file_path
        self.sep = sep
        self.encoding = encoding
        self.topics = {}
        
        self.__sortContent__()
    
    
    def __structureLine__(self, line:str):
        """Structure a binary text line with values separated by <sep>,
        creating a list with the values within the line as the list elements.
        This method discriminates among different data topics within the
        entire file, sorting each line according to the current topic.
        
        Args:
            line (str): a binary text line separated by <self.sep>.
        """
        line = line.decode(self.encoding)
        
        # Separate and clean values assigning None to '' fields
        line = [None if element == '' else element for element in line.strip().split(self.sep)]
        
        lenght = line.__len__()
        # Set the topic to which the record belongs
        if (lenght == 1) and (line[0] in self.glosary.keys()):
            topic = self.glosary[line[0]] # Type of data (e.g. weekly rain)
            self.topics[topic] = [] # Create the space destinated for data
            self.__ct__ = topic   # Replace for the current topic
        # Store records by the topic
        elif lenght == 16:
            line.append(self.nstat) # Add station to record
            self.topics[self.__ct__].append(line) # Add row to respective dataset
    
    
    def __sortContent__(self):
        """Analyze the tabular file content line by line to discriminate
        among records, table header and blank text lines. To achieve that,
        the method reads the entire file as binary mode.
        """
        # Get the station number from file name
        self.nstat = os.path.basename(self.filepath)[:-4] # Get the filename without its extention
        
        with open(self.filepath, 'br') as file:
            content = file.readlines()
        # Analyze data by line (record)
        for line in content:
            # Ignore blank lines
            if line != b'\r\n':
                # Ignore colnames line
                if b'ENE' not in line:
                    self.__structureLine__(line)




def createDB(feeder_dir:str, **kwargs):
    """Create a persistent, dictionary-like object (shelve) in the current
    directory with the data fetched from files stored within the directory
    passed into feeder_dir.
    Once the files that make up the shelve were created is possible to get
    its content using the same syntax as a dictionary.
    
    Args:
        feeder_dir (str): root path where the raw meteorological data is stored.
    
    WARNING:
        If already exists, the content will be overwritten.
    """
    db = shelve.open(__dbname__, flag='n')  # Creates the shelve file. Overwrite if already exists
    
    files = os.listdir(feeder_dir)
    for topic in __topics__:
        full_data = []
        
        # Map the TabularFile instance for each file
        stations_data = map(lambda file: TabularFile(os.path.join(feeder_dir, file), **kwargs), files)
        
        # Get the data corresponding to the current topic for each TabularFile instance
        gen = (station.topics[topic] for station in stations_data)
        
        for individual_element in gen:
            # Group data by topic, creating a unique list records from each file
            full_data += individual_element
        
        # Save pair key,value into shelve file
        db[topic] = full_data
    db.close()




def loadData(variable:Literal['pmax24', 'p', 'evo', 'tmax_x', 'tmax_mean', 'tmin_x', 'tmin_mean', 't']) -> pd.DataFrame:
    """Load data from a shelve format file by variable of interest.

    Args:
        variable ('pmax', 'p', 'evo', 'tmax_x', tmax_mean', &#39;tmin_x&#39;, &#39;tmin_mean&#39;, &#39;t&#39;]): meteorological variable of interest.

    Returns:
        pd.DataFrame: two-dimensional, size-mutable, potentially heterogeneous tabular data.
    """
    db = shelve.open(__dbname__, flag='r')
    try:
        data = db[variable]
    except KeyError:
        print('There is not data for the requested variable')
    finally:
        db.close()
        df = pd.DataFrame(data, columns=__colnames__)
        return df