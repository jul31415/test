# =================================================================
#
# Author: Julien Roy-Sabourin <julien.roy-sabourin.eccc@gccollaboration.ca>
#
# Copyright (c) 2020 Julien Roy-Sabourin
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from elasticsearch import Elasticsearch, exceptions
from datetime import datetime, timedelta 
from osgeo import gdal
import click
import logging
import json

LOGGER = logging.getLogger(__name__)                #ne pas oublier logger level est a debug:



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
        'id': 'date_end',
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
        'id': 'date_begin',
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


def valid_dates(date):
    """
    validate that date is in the correct format (raise ValueError)
    and add deafaut hour(12Z) if not specified

    date : date to validate

    retunr validated date
    """


    if len(date)==10 :
        dates_test = datetime.strptime(date, '%Y-%m-%d')
        date = date + 'T12:00:00Z'    

    else:
        date_test = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')

    return date



def query_es(es_object, index_name, date_end, date_begin, layer):

    """
    find documents that fit with search param

    es_object : ES server
    index_name : index name in ES server to look into
    date_end : max forecast hour datetime value to match docs
    date_begin : min forecast hour datetime value to match docs
    layer : layers to match docs

    return : json of the search result

    """
   
   

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
                            'lte': date_end,
                            'gte': date_begin
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


    try :
        res = es_object.search(index=index_name, body=s_object)

    except exceptions.ElasticsearchException as error:
        msg = 'ES search error: {}' .format(error)
        LOGGER.error(msg)
        return None
   

    return res




def xy_2_raster_data(path,x,y):

    """
    convert coordinate to x, y raster coordinate and
    return the x,y value of the raster

    path : where the grib raster file is located
    x : x coordinate
    y : y coordinate

    return : raster value in x, y position
    """

    try:
        grib = gdal.Open(path)
        
        transform=grib.GetGeoTransform()

        org_x = transform[0]
        org_y = transform[3]
         
        pix_w = transform[1]
        pix_h = transform[5]

        x = int((x - org_x) / pix_w)
        y = int((y - org_y) / pix_h)
        
        try:
            band1 =grib.GetRasterBand(1).ReadAsArray()
            return band1[y][x]

        except IndexError as error:
            msg = 'Invalid coordinates : {}' .format(error)
            LOGGER.error(msg)
           
        
    except RuntimeError as error:
        msg = 'can\'t open file : {}' .format(error)
        LOGGER.error(MSG)

    return 0





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
            val = xy_2_raster_data(file_path, x, y)
            data['daily_values'].append(val)
            data['dates'].append(date)

    elif cumul == 24 :      #use half of the documents

        date_ = res['hits']['hits'][-1]['_source']['properties']['forecast_hour_datetime']
        date_, time_ = date_.split('T') 

        for doc in res['hits']['hits']:

            file_path = doc['_source']['properties']['filepath']
            date = doc['_source']['properties']['forecast_hour_datetime']
            tmp, time = date.split('T')  

            if time == time_ :
                val = xy_2_raster_data(file_path, x, y)
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



def get_rpda_info(layer, date_end, date_begin, x, y, time_step):
    """
    output information to produce graph about rain accumulation for 
    given location and number of days

    layer : layer to search the info in
    date_end : end date
    date_begin : begin date
    x : x coordinate
    y : y coordinate
    time_step : time step for the graph in hours

    return : data 
    """

    es = Elasticsearch(['localhost:9200'])
    index = 'geomet-data-registry-tileindex'

    try:

        date_begin = valid_dates(date_begin)
        date_end = valid_dates(date_end)

    except ValueError as error:
        msg = 'invalid date : {}' .format(error)
        LOGGER.error(msg) 
        return None

   
    if es is not None:
        res = query_es(es, index, date_end, date_begin, layer)
        
        if res is not None:
            if res['hits']['total']['value'] > 0 :

                cumul = _24_or_6(res['hits']['hits'][0]['_source']['properties']['filepath'])

                if (time_step%cumul) == 0:
                   
                    values = get_values(res, x, y, cumul)
                    data = get_graph_arrays(values, time_step)
                    return data

                else :
                    LOGGER.error('invalid time step')
            else:
                LOGGER.error('no data found')
        else:     
            LOGGER.error('failed to extract data')  
    else:
        LOGGER.error('not connected')

    return None


@click.group('execute')
def test_execute():
    pass

@click.command('test')
@click.pass_context
@click.option('--layer', help='layer name', type=str)
@click.option('--date_end', help='end date of the graph', type=str)
@click.option('--date_begin', help='end date of the graph', type=str)
@click.option('--x', help='x coordinate', type=float)
@click.option('--y', help='y coordinate', type=float)
@click.option('--time_step', help='graph time step', type=int, default=0)


def cli(ctx, layer, date_end, date_begin, x, y, time_step):
    output = get_rpda_info(layer, date_end, date_begin, x, y, time_step)
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
            date_end = data['date_end']
            date_begin = data['date_begin']
            x = data['x']
            y = data['y']
            time_step = data['time_step']
            dict_ = get_rpda_info(layer, date_end, date_begin, x, y, time_step)
            return dict_

        def __repr__(self):
            return '<TestProcessor> {}'.format(self.name)

except (ImportError, RuntimeError):
    pass


