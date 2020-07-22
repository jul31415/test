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
from matplotlib.colors import ListedColormap
import matplotlib.image as image
from matplotlib.offsetbox import AnchoredText, OffsetImage, AnnotationBbox
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from osgeo import gdal

LOGGER = logging.getLogger(__name__)

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
CANADA_BBOX = '-140, 35, -44, 83'
LAMBERT_BBOX = [-170, 15, -40, 90]
COLOR_MAP = [[1, 1, 1, 1],
             [1, 1, 0, 1],
             [1, 0.5, 0, 1],
             [1, 0, 0, 1]]

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


def convert_bbox(bbox):
    """
    validate and convert (string to float) the bounding box

    bbox : bounding box to validate/convert

    return : bbox : converted boundaray box
    """
    i = 0
    for x in bbox:
        bbox[i] = float(x)
        i += 1

    if bbox[0] >= -180 and bbox[0] < bbox[2] and bbox[2] <= 180:
        if bbox[1] >= -90 and bbox[1] < bbox[3] and bbox[3] <= 90:
            return bbox

    return None


def valid_layer(layers):

    """
    validate the layers and return the layer data

    layers : layers to validate

    return : sufix : (ERGE or ERLE) if the layers are valid
             prefix : weather variable
             model : GEPS or REPS
             threholds : list of thresholds
    """

    prefix = []
    sufix = []
    models = []
    tresholds = []
    for layer in layers:

        layer_ = layer.split('.')
        model = layer_[0]
        prefix_ = layer_[2]
        sufix_ = layer_[3]
        sufix_ = sufix_[0:4]

        pos = layer_[3].rfind('E')
        if len(layer_) == 5:
            treshold = float(layer_[3][pos + 1:] + '.' + layer_[4])
        else:
            treshold = int(layer_[3][pos + 1:])

        prefix.append(prefix_)
        sufix.append(sufix_)
        models.append(model)
        tresholds.append(treshold)

        if sufix_ == 'ERGE' or sufix_ == 'ERLE':
            pass
        else:
            LOGGER.error('invalid layer type, need to be ERGE or ERLE')
            return None, None, None, None

    if prefix[0] == prefix[1]:
        if prefix[1] == prefix[2]:

            if sufix[0] == sufix[1]:
                if sufix[1] == sufix[2]:
                    return sufix[0], prefix[0], models[0], tresholds

    LOGGER.error('invalid layers, weather variables need to match')
    return None, None, None, None


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
    weather_variables = []

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
                weather_variables.append(res['hits']['hits'][0]
                                         ['_source']['properties']
                                         ['weather_variable'])

            except IndexError as error:
                msg = 'invalid input value: {}' .format(error)
                LOGGER.error(msg)
                return None, None

        except exceptions.ElasticsearchException as error:
            msg = 'ES search failed: {}' .format(error)
            LOGGER.error(msg)
            return None, None
    return files, weather_variables


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


def read_croped_array(band, geotransform, bbox):

    xinit = geotransform[0]
    yinit = geotransform[3]

    xsize = geotransform[1]
    ysize = geotransform[5]

    p1 = (bbox[0], bbox[3])
    p2 = (bbox[2], bbox[1])

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
    band = bands[0]
    srcband = ds.GetRasterBand(band)
    array1 = read_croped_array(srcband, geotransform, bbox)
    array1[array1 < 40] = 0
    array1[(array1 >= 40) & (array1 < 60)] = 1
    array1[array1 >= 60] = 1

    band = bands[1]
    srcband = ds.GetRasterBand(band)
    array2 = read_croped_array(srcband, geotransform, bbox)
    array2[(array2 >= 1) & (array2 < 20)] = 1
    array2[(array2 >= 20) & (array2 < 40)] = 1
    array2[(array2 >= 40) & (array2 < 60)] = 2
    array2[array2 >= 60] = 2

    band = bands[2]
    srcband = ds.GetRasterBand(band)
    array3 = read_croped_array(srcband, geotransform, bbox)
    array3[(array3 >= 1) & (array3 < 20)] = 1
    array3[(array3 >= 20) & (array3 < 40)] = 2
    array3[(array3 >= 40) & (array3 < 60)] = 2
    array3[array3 >= 60] = 3

    max_array = np.maximum(array1, array2)
    max_array = np.maximum(max_array, array3)
    return max_array


def find_best_projection(bbox):
    """
    find whether the LCC or the plateCarree projection is better
    according to the bbox

    bbox : bonding box

    return : project : best projection for the given bbox
    """

    project = ccrs.PlateCarree()

    if bbox[0] >= LAMBERT_BBOX[0] and bbox[0] <= LAMBERT_BBOX[2]:
        if bbox[2] >= LAMBERT_BBOX[0] and bbox[2] <= LAMBERT_BBOX[2]:
            if bbox[1] >= LAMBERT_BBOX[1] and bbox[1] <= LAMBERT_BBOX[3]:
                if bbox[3] >= LAMBERT_BBOX[1] and bbox[3] <= LAMBERT_BBOX[3]:
                    project = ccrs.LambertConformal()

    return project


def get_data_text(variable, tresholds, mr, model, fh):

    """
    Provide the text string of the metedata for the png output

    predix : weather variable
    sufix : ERGE or ERLE
    tresholds : list of 3 user specified thresholds
    mr : model run
    model : GEPS or REPS
    fh : forcast hour

    return : textstr : formated string for the png
    """
    mr = mr.strftime(DATE_FORMAT)
    fh = fh.strftime(DATE_FORMAT)
    trh = [tresholds[0], tresholds[1], tresholds[2]]
    textstr = '\n'.join(('{} {} - {}'. format(variable, trh, model),
                         'Émis/Issued: {} '.format(mr),
                         'Prévision/Forecast: {} '.format(fh)))

    return textstr


def add_basemap(data, bbox, textstr):
    """
    add the basemap spacified by the bbox to the vigilance data

    data : vigilance data
    bbox : geo exetent of the data

    return : map : png in bytes of the produced vigilance map
    with the bsaemap
    """

    project = find_best_projection(bbox)
    # ajout des données de vigilance
    ny, nx = data.shape
    lons = np.linspace(bbox[0], bbox[2], nx)
    lats = np.linspace(bbox[3], bbox[1], ny)
    lons, lats = np.meshgrid(lons, lats)
    ax = plt.axes(projection=project)
    colors = ListedColormap(COLOR_MAP)
    plt.contourf(lons, lats, data, 60, transform=ccrs.PlateCarree(),
                 cmap=colors)

    # ajout de la basemap
    ax.coastlines(linewidth=0.35)
    ax.add_feature(cfeature.BORDERS, linestyle='-',
                   edgecolor='black',
                   linewidth=0.35)
    state = cfeature.NaturalEarthFeature(category='cultural',
                                         name='admin_1_states_provinces_lines',
                                         scale='50m', facecolor='none')
    ax.add_feature(state, edgecolor='black', linewidth=0.35)

    # ajout des données de la carte
    text_box = AnchoredText(textstr, frameon=True, loc=4, pad=0.5,
                            borderpad=0.05, prop={'size': 5})
    plt.setp(text_box.patch, facecolor='white', alpha=1, linewidth=0.35)
    ax.add_artist(text_box)

    # ajout du logo
    str1 = '                Environnement et Changement climatique Canada'
    str2 = '                Environment and Climate Change Canada'
    textstr2 = '\n'.join((str1, str2))
    text_box2 = AnchoredText(textstr2, frameon=True, loc=2, pad=0.5,
                             borderpad=0, prop={'size': 4})
    plt.setp(text_box2.patch, facecolor='white', alpha=1, linewidth=0.35,
             zorder=1)
    ax.add_artist(text_box2)

    im = image.imread('msc_pygeoapi/process/weather/canada flag.png')
    imagebox = OffsetImage(im, zoom=0.07)
    ab = AnnotationBbox(imagebox, (0.005, 0.99), xycoords=ax.transAxes,
                        frameon=False, box_alignment=(0, 1))
    ab.set_zorder(10)
    ax.add_artist(ab)

    # ajout de la légende
    str1 = 'Be aware / Soyez Attentif'
    str2 = 'Be prepared / Soyez très vigilant'
    str3 = 'Be extra cautious / Vigilance absolue'
    y_patch = mpatches.Patch(color='yellow', label=str1)
    o_patch = mpatches.Patch(color='orange', label=str2)
    r_patch = mpatches.Patch(color='red', label=str3)
    leg = plt.legend(handles=[y_patch, o_patch, r_patch], loc='lower left',
                     bbox_to_anchor=(0, 0), fancybox=False, fontsize=4,
                     framealpha=1, borderaxespad=0.05, edgecolor='black',
                     borderpad=0.5)
    leg_frame = leg.get_frame()
    plt.setp(leg_frame, linewidth=0.35)

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
    bbox = convert_bbox(bbox)

    if bbox is not None:

        if len(layers) == 3:
            sufix, prefix, model, tresholds = valid_layer(layers)
            if sufix is None:
                return None

            files, variables = get_files(layers, fh, mr)

            if files is None:
                return None

            if len(files) == 3:
                path, bands = get_bands(files)

                if sufix == 'ERGE':
                    bands = band_ordre_G(bands)

                if sufix == 'ERLE':
                    bands = band_ordre_L(bands)

                vigi_data = get_new_array(path, bands, bbox)
                textstr = get_data_text(variables[0], tresholds, mr, model,
                                        fh)
                add_basemap(vigi_data, bbox, textstr)
                return format_ + ' basemaped file on disk'
            else:
                LOGGER.error('invalid layer')
                return None

        else:
            LOGGER.error('Invalid number of layers')
            return None

    else:
        LOGGER.error('Invalid bbox')
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
