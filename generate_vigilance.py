# =================================================================
#
# Author: Julien Roy-Sabourin
#         <julien.roy-sabourin.eccc@gccollaboration.ca>
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

import click
from datetime import datetime
import json
import logging

from elasticsearch import Elasticsearch, exceptions
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from osgeo import gdal, osr
from PIL import Image
from pyproj import Proj, transform


LOGGER = logging.getLogger(__name__)

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

CANADA_BBOX = '-158.203125, 36.879621, -44.296875, 83.215693'
WORLD_BBOX = '-180.25, -90.25, 179.75, 89.75'

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'generate-vigilance',
    'title': 'Generate vigilance process for weather data',
    'description': 'Generate vigilance process for weather data',
    'keywords': ['generate vigilance weather'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': [{
        'id': 'layers',
        'title': '3 layers to produce vigilence',
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
    }, {
        'id': 'forecast-hour',
        'title': 'forecast hour to use',
        'input': {
            'literalDataDomain': {
                'dataType': 'timestamp',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'model-run',
        'title': 'model run to use',
        'input': {
            'literalDataDomain': {
                'dataType': 'timestamp',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1
    }, {
        'id': 'bbox',
        'title': 'bounding box',
        'description': '"x_min, y_min, x_max, y_max"',
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
    }, {
        'id': 'format',
        'title': 'output format',
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
    }],
    'outputs': [{
        'id': 'generate-vigilance-response',
        'title': 'output wms vigilance product',
        'output': {
            'formats': [{
                'mimeType': 'application/json'
            }, {
                'mimeType': 'text/csv'
            }]
        }
    }],
    'example': {
        'inputs': [{
                "id": "layers",
                "value":
                "GEPS.DIAG.24_T8.ERGE15," +
                "GEPS.DIAG.24_T8.ERGE20,GEPS.DIAG.24_T8.ERGE25"
            },
            {
                "id": "forecast-hour",
                "value": "2020-06-22T00:00:00Z"
            },
            {
                "id": "model-run",
                "value": "2020-06-21T00:00:00Z"
            },
            {
                "id": "bbox",
                "value": CANADA_BBOX
            },
            {
                "id": "format",
                "value": "png"
            }]
    }
}


def valid_layer(layers):

    """
    validate if the layers are ERGE or ERLE

    layers : layers to validate

    return : sufix (ERGE or ERLE) if the layers are valid
    """

    prefix = []
    sufix = []
    for layer in layers:

        pos = layer.rfind('.')

        layer_ = layer.split('.')
        sufix_ = layer_[-1]
        sufix_ = sufix_[0:4]

        # if ERGE2.5
        if sufix_ == '5':
            sufix_ = layer_[-2]
            sufix_ = sufix_[0:4]
            pos -= 6

        prefix.append(layer[0:pos])
        sufix.append(sufix_)

        if sufix_ == 'ERGE' or sufix_ == 'ERLE':
            pass
        else:
            LOGGER.error('invalid layer, type need to be ERGE or ERLE')
            return None

    if prefix[0] == prefix[1]:
        if prefix[1] == prefix[2]:

            if sufix[0] == sufix[1]:
                if sufix[1] == sufix[2]:
                    return sufix[0]

    LOGGER.error('invalid layers, weather variables need to match')
    return None


def get_files(layers, fh, mr):

    """
    ES search to find files names

    layers : arrays of three layers
    fh : forcast hour datetime
    mr : reference datetime = model run

    return : files : arrays of threee file paths
    """

    es = Elasticsearch(['localhost:9200'])
    index = 'geomet-data-registry-tileindex'
    files = []

    for layer in layers:

        s_object = {
            'query':
            {
                'bool':
                {
                    'must':
                    {
                        'match': {'properties.layer.raw': layer}
                    },
                    'filter':
                    [
                        {'term': {'properties.forecast_hour_datetime':
                                  fh.strftime(DATE_FORMAT)}},
                        {'term': {'properties.reference_datetime':
                                  mr.strftime(DATE_FORMAT)}}
                    ]
                }
            }
        }

        try:
            res = es.search(index=index, body=s_object)

            try:
                files.append(res['hits']['hits'][0]
                             ['_source']['properties']['filepath'])

            except IndexError as error:
                msg = 'invalid input value: {}' .format(error)
                LOGGER.error(msg)
                return None

        except exceptions.ElasticsearchException as error:
            msg = 'ES search failed: {}' .format(error)
            LOGGER.error(msg)
            return None
    return files


def get_bands(files):

    """
    extract the band number from the file path

    files : arrays of three file paths

    return : paths : file paths
             bands : grib bands numbers
    """
    p_tempo = files[0].split('?')
    p = p_tempo[0]
    paths = p[6:]
    bands = []

    for file_ in files:
        b = file_.split('=')
        bands.append(int(b[-1]))
    return paths, bands


def band_ordre_G(bands):
    """
    sort thresholds bands in ascending order  for ERGE

    bands : bands to sort

    return : bands : sorted band arrays
    """

    if bands[0] > bands[1]:
        temp = bands[0]
        bands[0] = bands[1]
        bands[1] = temp

    if bands[1] > bands[2]:
        temp = bands[1]
        bands[1] = bands[2]
        bands[2] = temp

    if bands[0] > bands[1]:
        temp = bands[0]
        bands[0] = bands[1]
        bands[1] = temp
    return bands


def band_ordre_L(bands):
    """
    sort thresholds bands in descending order  for ERGE

    bands : bands to sort

    return : bands : sorted band array
    """

    if bands[0] < bands[1]:
        temp = bands[0]
        bands[0] = bands[1]
        bands[1] = temp

    if bands[1] < bands[2]:
        temp = bands[1]
        bands[1] = bands[2]
        bands[2] = temp

    if bands[0] < bands[1]:
        temp = bands[0]
        bands[0] = bands[1]
        bands[1] = temp
    return bands


def transform_coord(file, bbox):
    """
    transform a lat long coordinate into the projection of the given file

    file : file to extract th projection from
    x : x coordinate (long)
    y : y coordinate (lat)

    return : _x _y : coordinata in transformed projection
    """
    g_bbox = []
    ds = gdal.Open(file)

    srs = osr.SpatialReference()
    srs.ImportFromWkt(ds.GetProjection())
    inProj = Proj('epsg:4326')
    outProj = Proj(srs.ExportToProj4())
    print(outProj)
    x, y = transform(inProj, outProj, float(bbox[0]), float(bbox[1]))
    g_bbox.append(x)
    g_bbox.append(y)
    x, y = transform(inProj, outProj, float(bbox[2]), float(bbox[3]))
    g_bbox.append(x)
    g_bbox.append(y)
    print(g_bbox)
    return g_bbox


def read_croped_array(band, geotransform, bbox):

    xinit = geotransform[0]
    yinit = geotransform[3]

    xsize = geotransform[1]
    ysize = geotransform[5]

    p1 = (float(bbox[0]), float(bbox[3]))
    p2 = (float(bbox[2]), float(bbox[1]))

    row1 = int((p1[1] - yinit)/ysize)
    col1 = int((p1[0] - xinit)/xsize)

    row2 = int((p2[1] - yinit)/ysize)
    col2 = int((p2[0] - xinit)/xsize)

    array = band.ReadAsArray(col1, row1, col2 - col1 + 1, row2 - row1 + 1)
    return array


def get_new_array(path, bands, bbox):

    """
    combines 3 file into one array for vigilance

    paths : arrays of three file paths
    band : array of three grib band number

    return : max_array : the combined array for vigilance
    """

    try:
        ds = gdal.Open(path)
    except RuntimeError as err:
        msg = 'Cannot open file: {}, assigning NA'.format(err)
        LOGGER.error(msg)

    geotransform = ds.GetGeoTransform()
    gdal_bbox = bbox
    band = bands[0]
    srcband = ds.GetRasterBand(band)
    array1 = read_croped_array(srcband, geotransform, gdal_bbox)
    array1[array1 < 40] = 0
    array1[(array1 >= 40) & (array1 < 60)] = 1
    array1[array1 >= 60] = 2

    band = bands[1]
    srcband = ds.GetRasterBand(band)
    array2 = read_croped_array(srcband, geotransform, gdal_bbox)
    array2[(array2 >= 1) & (array2 < 20)] = 3
    array2[(array2 >= 20) & (array2 < 40)] = 4
    array2[(array2 >= 40) & (array2 < 60)] = 6
    array2[array2 >= 60] = 7

    band = bands[2]
    srcband = ds.GetRasterBand(band)
    array3 = read_croped_array(srcband, geotransform, gdal_bbox)
    array3[(array3 >= 1) & (array3 < 20)] = 5
    array3[(array3 >= 20) & (array3 < 40)] = 8
    array3[(array3 >= 40) & (array3 < 60)] = 9
    array3[array3 >= 60] = 10

    max_array = np.maximum(array1, array2)
    max_array = np.maximum(max_array, array3)
    return max_array


def create_file(new_array, path, bbox):

    """
    create a geo tiff file from the compiled array for the vigilance product

    new_array : vigilance array
    path : file paths

    return : True if the file is created succesfully
    """
    print('ok')
    try:

        ds = gdal.Open(path)
        driver = gdal.GetDriverByName('GTiff')
        ysize, xsize = new_array.shape

        do = driver.Create('v.tif',
                           xsize, ysize, 1, gdal.GDT_Byte)
    except RuntimeError as err:
        msg = 'failed to create vigilance raster: {}'.format(err)
        LOGGER.error(msg)
        return False

    srs = osr.SpatialReference()
    wkt = ds.GetProjection()
    srs.ImportFromWkt(wkt)
    do.SetProjection(srs.ExportToWkt())
    gt = [float(bbox[0]), 0.5, 0.0, float(bbox[3]), 0.0, -0.5]
    do.SetGeoTransform(gt)

    outband = do.GetRasterBand(1)
    outband.SetStatistics(np.min(new_array), np.max(new_array),
                          np.average(new_array), np.std(new_array))
    outband.WriteArray(new_array)

    # set colors
    colors = gdal.ColorTable()
    colors.SetColorEntry(0, (255, 255, 255))
    colors.SetColorEntry(1, (246, 255, 0))
    colors.SetColorEntry(2, (255, 230, 0))
    colors.SetColorEntry(3, (255, 200, 0))
    colors.SetColorEntry(4, (255, 180, 0))
    colors.SetColorEntry(5, (255, 160, 0))
    colors.SetColorEntry(6, (255, 160, 0))
    colors.SetColorEntry(7, (255, 120, 0))
    colors.SetColorEntry(8, (255, 80, 0))
    colors.SetColorEntry(9, (255, 40, 0))
    colors.SetColorEntry(10, (255, 0, 0))

    # apply colors
    outband.SetRasterColorTable(colors)
    outband.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)


def add_basemap(data, bbox):
    """
    add the basemap spacified by the bbox to the vigilance data

    data : vigilance data
    bbox : geo exetent of the data

    return : map : png in bytes of the produced vigilance map
    with the bsaemap
    """
    ny, nx = data.shape
    lons = np.linspace(float(bbox[0]), float(bbox[2]), nx)
    lats = np.linspace(float(bbox[3]), float(bbox[1]), ny)
    lons, lats = np.meshgrid(lons, lats)

    ax = plt.axes(projection=ccrs.PlateCarree())
    plt.contourf(lons, lats, data, 60, transform=ccrs.PlateCarree(),
                 cmap='OrRd')
    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linestyle='-', edgecolor='black')
    states_provinces = cfeature.NaturalEarthFeature(category='cultural', name='admin_1_states_provinces_lines', scale='50m', facecolor='none')
    ax.add_feature(states_provinces, edgecolor='black')

    plt.title('vigilance')
    plt.savefig('vigi.png', bbox_inches='tight', dpi=200)


def generate_vigilance(layers, fh, mr, bbox, format_):
    """
    generate a vigilance file (with specified format)
    according to the thresholds

    layers : 3 layer of the 3 different thresholds
    fh : forcast hour
    mr : model run
    bbox : bounding box
    format_ : output format

    return : validation of the process
    """

    gdal.UseExceptions()

    if len(layers) == 3:
        prefix = valid_layer(layers)
        if prefix is None:
            return None

        files = get_files(layers, fh, mr)

        if files is None:
            return None

        if len(files) == 3:
            path, bands = get_bands(files)

            if prefix == 'ERGE':
                bands = band_ordre_G(bands)

            if prefix == 'ERLE':
                bands = band_ordre_L(bands)

            vigi_data = get_new_array(path, bands, bbox)
            add_basemap(vigi_data, bbox)
            return format_ + ' basemaped file on disk'
        else:
            LOGGER.error('invalid layer')
            return None

    else:
        LOGGER.error('Invalid number of layers')
        return None


@click.command('generate-vigilance')
@click.pass_context
@click.option('--layers', 'layers', help='3 layers for vigilance')
@click.option('--forecast-hour', 'fh',
              type=click.DateTime(formats=[DATE_FORMAT]),
              help='Forecast hour to create the vigilance')
@click.option('--model-run', 'mr',
              type=click.DateTime(formats=[DATE_FORMAT]),
              help='model run to use for the time serie')
@click.option('--bbox', 'bbox', default=CANADA_BBOX, help='bounding box')
@click.option('--format', 'format_', help='output format')
def cli(ctx, layers, fh, mr, bbox, format_):

    output = generate_vigilance(layers.split(','), fh, mr, bbox.split(','),
                                format_)
    click.echo(json.dumps(output))


try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class GenerateVigilanceProcessor(BaseProcessor):
        """Vigilance product Processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns:
            pygeoapi.process.weather.generate_vigilance.GenerateVigilanceProcessor
             """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data):
            layers = data['layers']
            fh = datetime.strptime(data['forecast-hour'],
                                   DATE_FORMAT)
            mr = datetime.strptime(data['model-run'],
                                   DATE_FORMAT)
            bbox = data['bbox']
            format_ = data['format']

            if bbox == '':
                bbox = CANADA_BBOX

            try:
                output = generate_vigilance(layers.split(','),
                                            fh, mr, bbox.split(','), format_)
                return output
            except ValueError as err:
                msg = 'Process execution error: {}'.format(err)
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

        def __repr__(self):
            return '<GenerateVigilanceProcessor> {}'.format(self.name)
except (ImportError, RuntimeError):
    pass
