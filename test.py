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
        'id': 'date1',
        'title': 'end date (yyyy-mm-dd)',
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
        'id': 'date2',
        'title': 'begin date (yyyy-mm-dd)',
        'description' : 'first date of the graph',
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
    },
    {
        'id': 'time_step',
        'title': 'time step',
        'description': 'time step for the graph in hours',
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



def searchT(es_object, index_name, date1, date2, layer):

    """
    find documents that fit with search param

    es_object : ES server
    index_name : index name in ES server to look into
    date1 : max forecast hour datetime value to match docs
    date2 : min forecast hour datetime value to match docs
    layer : layers to match docs

    return : json of the search result

    """
  
   
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
                                'lte': date1,
                                'gte': date2
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
    find if a rdpa file is for a 24h or 6h accumulation

    file : filepaht with file name

    return : true if 24h, false if 6h
    """

    nb = file.rfind('/')

    if file[(nb+15):(nb+18)]=='024' :
        return 24
    elif file[(nb+15):(nb+18)]=='006' :
        return 6
    else:
        print('layer error')
        return 0




def get_values(res, x, y, cumul):
    """
    get the raw raster values at (x, y) for each document
    found by ES

    res : ES search result
    x : x coordinate
    y : y coordinate
    cummul : 24h or 6h accumulation files

    return : (x, y) raster values and dates

    """
         
    data = {
    'daily_values': [],
    'dates': []
    }

    if cumul == 6:
        for doc in res['hits']['hits']:
            file_path = doc['_source']['properties']['filepath']
            date = doc['_source']['properties']['forecast_hour_datetime']
            val = coord_2_pix_info(file_path, x, y)
            data['daily_values'].append(val)
            data['dates'].append(date)

    elif cumul == 24 :      #use half of the documents

        date1 = res['hits']['hits'][-1]['_source']['properties']['forecast_hour_datetime']
        date1, time1 = date1.split('T') 

        for doc in res['hits']['hits']:

            file_path = doc['_source']['properties']['filepath']
            date = doc['_source']['properties']['forecast_hour_datetime']
            date2, time = date.split('T')  

            if time == time1 :
                val = coord_2_pix_info(file_path, x, y)
                data['daily_values'].append(val)
                data['dates'].append(date)
     
    return data


def get_graph_arrays(values, time_step):
    """
    Produce the arrays for the graph accordingly to the time step

    values : raw (x, y) raster values and dates
    time_step : time step for the graph in hours

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

    date_c = values['dates'][0]
    date_c = datetime.strptime(date_c, '%Y-%m-%dT%H:%M:%SZ')
    total = 0
    cmpt = -1

    for i in range(len(values['dates'])):
        val = values['daily_values'][i]
        date = values['dates'][i]
        date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
        total += val

        if date != date_c:
            data['daily_values'][cmpt] += val
            data['total_values'][cmpt] += val
        else :
            data['daily_values'].append(val)
            data['total_values'].append(total)
            data['dates'].append(date)
            date_c = date + timedelta(hours = time_step)  
            cmpt +=1

    if time_step >= 24 and (time_step%24) == 0:                
       for i in range(len(data['dates'])):
            date, time = datetime.strftime(data['dates'][i], '%Y-%m-%dT%H:%M:%SZ').split('T')
            data['dates'][i] = date
    else :
        for i in range(len(data['dates'])):
            data['dates'][i] = datetime.strftime(data['dates'][i], '%Y-%m-%dT%H:%M:%SZ')
            

    return data



def get_rpda_info(layer, date1, date2, x, y, time_step):
    """
    output information to produce graph about rain accumulation for 
    given location and number of days

    layer : layer to search the info in
    date1 : end date
    date2 : begin date
    x : x coordinate
    y : y coordinate
    time_step : time step for the graph in hours

    return : data 
    """

    es = Elasticsearch(['localhost:9200'])
    index = 'geomet-data-registry-tileindex'

    if len(date1)!=20 :
        date1 = date1 + 'T12:00:00Z'    #if no hour specified

    if len(date2)!=20 :
        date2 = date2 + 'T12:00:00Z'  

   
    if es is not None:
        res = searchT(es, index, date1, date2, layer)
        cumul = _24_or_6(res['hits']['hits'][0]['_source']['properties']['filepath'])
        
        if res is not None:
            if res['hits']['total']['value'] > 0 :
                if (time_step%cumul) == 0:
                   
                    values = get_values(res, x, y, cumul)
                    data = get_graph_arrays(values, time_step)
                    return data

                else :
                    print('invalid time step')
            else:
                print('no data found')
        else:     
            print('failed to extract data')  
    else:
        print('not connected')

    return None


@click.group('execute')
def test_execute():
    pass

@click.command('test')
@click.pass_context
@click.option('--layer', help='layer name', type=str)
@click.option('--date1', help='end date of the graph', type=str)
@click.option('--date2', help='end date of the graph', type=str)
@click.option('--x', help='x coordinate', type=float)
@click.option('--y', help='y coordinate', type=float)
@click.option('--time_step', help='graph time step', type=int, default=0)


def cli(ctx, layer, date1, date2, x, y, time_step):
    output = get_rpda_info(layer, date1, date2, x, y, time_step)
    click.echo(json.dumps(output, ensure_ascii=False))
    

test_execute.add_command(cli)

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
            layer = data['layer']
            date1 = data['date1']
            date2 = data['date2']
            x = data['x']
            y = data['y']
            time_step = data['time_step']
            dict_ = get_rpda_info(layer, date1, date2, x, y, time_step)
            return dict_

        def __repr__(self):
            return '<TestProcessor> {}'.format(self.name)

except (ImportError, RuntimeError):
    pass

