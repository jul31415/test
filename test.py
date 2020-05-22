from elasticsearch import Elasticsearch
from datetime import datetime, timedelta 
from osgeo import gdal
import click
import logging
import json

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'test',
    'title': 'Test process',
    'description': 'produce data for rdpa graph',
    'keywords': ['rdpa'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://canada.ca/climate-services',
        'hreflang': 'en-CA'
    }, {
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://canada.ca/services-climatiques',
        'hreflang': 'fr-CA'
    }],
    'inputs': [{
        'id': 'layer',
        'title': 'layer name',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    },
    {
        'id': 'date',
        'title': 'end date',
        'description' : 'final date of the graph',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    },
    {
        'id': 'nb_days',
        'title': 'length of the graph',
        'input': {
            'literalDataDomain': {
                'dataType': 'int',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    },
    {
        'id': 'x',
        'title': 'x coordinate',
        'input': {
            'literalDataDomain': {
                'dataType': 'float',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    },
    {
        'id': 'y',
        'title': 'y coordinate',
        'input': {
            'literalDataDomain': {
                'dataType': 'float',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }],
    'outputs': [{
        'id': 'test-response',
        'title': 'output test',
        'output': {
            'formats': [{
                'mimeType': 'application/json'
            }]
        }
    }]
}



def searchT(es_object, index_name, date, layer, jour):

    """
    find documents that fit with search param

    es_object : ES server
    index_name : index name in ES server to look into
    date : max forecast hour datetime value to match docs
    jour : nb of days to look into before the date param
    layer : layers to match docs

    return : json of the search result

    """

    if(len(date)!=20):
       date = date + 'T12:00:00Z'    #if no hour specified
   
    try:   

        s_object =  {
            'size': 100,              #result limit     
            'query':
            {
                'bool':
                {
                    'must': 
                    {
                        'range':
                        {
                            'properties.forecast_hour_datetime':
                            {
                                'lte': date,
                                'gte': date_change(date, jour)
                            }
                        }
                    },
                    'filter': 
                    {
                        'term': {"properties.layer.raw": layer}
                    }
                }   
            }     
        }

        res = es_object.search(index=index_name, body=s_object)
    except:
        print('impossible research')
        res = None

    return res



def date_change(date, x):

    """
    takes a date and calculate the date x days before

    date : end date
    x : days to substract

    return : date x days before end date
    """
    
    try :

        date, time = date.split('T')  
        date = datetime.strptime(date, '%Y-%m-%d')

        if x > 0:
            date = date - timedelta(days = x)
        else:
            print('mauvaise valeur de pas de temps')

        date = str(date)
        date = date[0:10] + 'T' + time
        return date

    except:
        print('invalid date')
       


def coord_2_pix_info(path,x,y):

    """
    convert coordinate to x, y raster value and
    return the x,y value of the raster

    path : where the grib raster file is located
    x : x coordinate
    y : y coordinate

    return : raster value in x, y position
    """

    try:
        grib = gdal.Open(path)
        
        transform=grib.GetGeoTransform()

        Xorg = transform[0]
        Yorg=transform[3]
         
        pixW = transform[1]
        pixH = transform[5]

        x = int((x - Xorg) / pixW)
        y = int((y - Yorg) / pixH)
        
        try:
            band1 =grib.GetRasterBand(1).ReadAsArray()
            return band1[y][x]
            

        except:
            print('Invalid coordinates')
            return 0
        
    except:
        print('Open failed')





def _24_or_6(file):
    """
    find if a rdpa file is for a 24h or 6h prediction

    file : filepaht with file name

    return : true if 24h, false if 6h
    """

    nb = file.rfind('/')

    if file[(nb+15):(nb+18)]=='024' :
        return True
    else :
        if file[(nb+15):(nb+18)]=='006':
            return False



def get_24_info(res, x, y):

    """
    output json arrays of rpad data compiled in 24 for the 24h format:
    24h prediction 2 times a day

    res : ES response 
    x : x coordinate
    y : y coordinate

    return : data : json that contains 3 arrays :
                        daly_values : rdpa value for 24h
                        total_value : total rdpa value since the begin date
                        date : date of rdpa values
    """

    data = {
        'daily_values': [],
        'total_values': [],
        'dates': []
    }

    date1 = res['hits']['hits'][0]['_source']['properties']['forecast_hour_datetime']
    date1, time1 = date1.split('T')  
    total = 0
    for doc in res['hits']['hits']:
        
        
        file_path = doc['_source']['properties']['filepath']
        date = doc['_source']['properties']['forecast_hour_datetime']
        date, time = date.split('T')  

        if time == time1 :
            val = coord_2_pix_info(file_path, x, y)
            total += val
            
            data['daily_values'].append(val)
            data['total_values'].append(total)
            data['dates'].append(date) 


    return data


def get_6_info(res, x, y):

    """
    output json arrays of rpad data compiled in 24 for the 6h format:
    6h prediction 4 times a day

    res : ES response 
    x : x coordinate
    y : y coordinate

    return : data 
    """

    data = {
        'daily_values': [],
        'total_values': [],
        'dates': []
    }

    date_c = ''
    total = 0
    cmpt = -1

    for doc in res['hits']['hits']:
        
        
        file_path = doc['_source']['properties']['filepath']
        date = doc['_source']['properties']['forecast_hour_datetime']
        date, time = date.split('T')  
        val = coord_2_pix_info(file_path, x, y)
        total += val

        if date == date_c:
            data['daily_values'][cmpt] += val
            data['total_values'][cmpt] += val
        else :
            data['daily_values'].append(val)
            data['total_values'].append(total)
            data['dates'].append(date)
            date_c = date 
            cmpt +=1

    return data





def get_rpda_info(layer, date, nb_days, x, y):
    """
    output information to produce graph about rain accumulation for 
    given location and number of days

    layer : layer to search the info in
    date : end date
    nb_days : number of days to look for data beforme the end date
    x : x coordinate
    y : y coordinate

    return : data 
    """

    es = Elasticsearch(['localhost:9200'])
    index = 'geomet-data-registry-tileindex'

   
    if es is not None:
        res = searchT(es, index, date, layer, nb_days)

        if res is not None:
            print('%d documents found' % res['hits']['total']['value'])
            
            if res['hits']['total']['value'] > 0 :
                if _24_or_6(res['hits']['hits'][0]['_source']['properties']['filepath']) :
                    #24H                                                                            
                    data = get_24_info(res, x, y)
                else :
                    #6H
                    data = get_6-info(res, x, y)  
                return data
        else :
            print('failed to extract data')  
    else:
        print('not connected')

    return None



def test(layer, date, nb_days, x, y):
    
    data = get_rpda_info(layer, date, nb_days, x, y)
    return data


@click.command('test')
@click.pass_context
@click.option('--layer', help='layer name', type=str)
@click.option('--date', help='end date of the graph', type=str)
@click.option('--nb_days', help='length of the graph', type=int)
@click.option('--x', help='x coordinate', type=float)
@click.option('--y', help='y coordinate', type=float)


def cli(ctx, layer, date, nb_days, x, y):
    output = get_rpda_info(layer, date, nb_days, x, y)
    click.echo(json.dumps(output, ensure_ascii=False))
    

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class TestProcessor(BaseProcessor):
        """test Processor"""

        def __init__(self, provider_def):
            """
            Initialize object
            :param provider_def: provider definition
            :returns: pygeoapi.process.weather.test.TestProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            
            text = data['layer']
            date = data['date']
            nb_days = data['nb_days']
            x = data['x']
            y = data['y']
            ditc_ = get_rpda_info(layer, date, nb_days, x, y)
            return dict_

        def __repr__(self):
            return '<TestProcessor> {}'.format(self.name)
except (ImportError, RuntimeError):
    pass

