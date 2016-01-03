'''
Created on Nov 25, 2015

@author: theo
'''

defkey=r'cbfee8a09865749b6a3a6781c1acfcca'
defurl=r'http://api.openweathermap.org/data/2.5/forecast/daily?lat={lat}&lon={lon}&cnt={cnt}&units=metric&appid={appid}' 
debilt=(5.177,52.101) # lonlat
defcnt=16

import datetime, pytz, json, StringIO
from acacia.data.generators.generator import Generator
import pandas as pd

class Forecast(Generator):

    def download(self, **kwargs):
        # format url and pass to generator
        url = kwargs.get('url',defurl)
        lon,lat = kwargs.get('lonlat',debilt)
        cnt = kwargs.get('cnt', defcnt)
        appid=kwargs.get('key', defkey)
        url = url.format(lat=lat,lon=lon,cnt=cnt,appid=appid)
        kwargs['url'] = url
        if not 'filename' in kwargs:
            kwargs['filename'] = 'openweather_{lon}_{lat}.txt'.format(lon=lon,lat=lat)
        return super(Forecast, self).download(**kwargs)
        
    def get_data(self,fil,**kwargs):
        o = json.load(fil)
#         if o['cod'] != '200':
#             return None
        index = []
        data = {'rain': [], 'tmin': [], 'tmax': []}
        for rec in o['list']:
            dt = rec['dt']
            index.append(datetime.datetime.fromtimestamp(dt,tz=pytz.utc))
            data['rain'].append(rec.get('rain', 0))
            data['tmin'].append(rec['temp'].get('min', None))
            data['tmax'].append(rec['temp'].get('max', None))
        return pd.DataFrame(data, index=index)

    def get_parameters(self,fil):
        return {'rain': {'description': 'neerslag', 'unit': 'mm/d'},
                 'tmin': {'description': 'minimum temperatuur', 'unit': 'oC'},
                 'tmax': {'description': 'maximum temperatuur', 'unit': 'oC'},
#                 'pressure': {'description': 'luchtdruk', 'unit': 'oC'},
#                 'speed': {'description': 'wind speed', 'unit': '-'},
#                 'direction': {'description': 'wind direction', 'unit': 'deg'},
                 }

from acacia.data.models import Datasource
if __name__ == '__main__':
    gen = Forecast()
    ds = Datasource.objects.get(pk=68)
    result = ds.download()
    for key,contents in result.items():
        print gen.get_data(StringIO.StringIO(contents))
#     with open('/media/sf_F_DRIVE/acaciadata.com/openweather.json') as fil:
#         print gen.get_parameters(fil)
#         print gen.get_data(fil)
        
        