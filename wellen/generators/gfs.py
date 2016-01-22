'''
Created on Jan 24, 2014

@author: theo
'''
import datetime
from pydap.client import open_url
from pydap.exceptions import DapError
from netcdftime import utime
import pandas as pd

from acacia.data.generators.generator import Generator

defurl = 'http://nomads.ncep.noaa.gov:9090/dods/gens/gens{date}/gep_all_00z'
debilt = (5.177,52.101) # lonlat

class GEFS(Generator):
    vars = [
        'albdosfc', # surface albedo [%]
        'apcpsfc',  # surface total precipitation [kg/m^2] 
        'tmp2m',    # 2 m above ground temperature [k] 
        'tmax2m',   # 2 m above ground maximum temperature [k]
        'tmin2m',   # 2 m above ground minimum temperature [k]
        'rh2m',     # 2 m above ground relative humidity [%]
        'dpt2m',    # 2 m above ground dew point temperature [k]
        'dswrfsfc', # surface downward short-wave radiation flux [w/m^2] 
        'dlwrfsfc', # surface downward long-wave rad. flux [w/m^2] 
        'uswrfsfc', # surface upward short-wave radiation flux [w/m^2]  
        'ulwrfsfc', # surface upward long-wave rad. flux [w/m^2] 
        'ugrd10m',  # 10 m above ground u-component of wind [m/s] 
        'vgrd10m'   # 10 m above ground v-component of wind [m/s] 
    ]            
    def download(self, **kwargs):
        url = kwargs.get('url',defurl)
        lon,lat = kwargs.get('lonlat',debilt)
        date = datetime.date.today().strftime('%Y%m%d')
        url = url.format(date=date)
        lat = 90 + int(lat)
        lon = int(lon)
        dataset = open_url(url)
        timeunit = dataset.time.attributes['units']
        cdftime = utime(timeunit)
        for var in self.vars:
            print var
            grid = dataset[var]
            missing_value = grid.attributes['missing_value']
            ens = grid.ens
            time = grid.time
            index = [cdftime.num2date(t) for t in time]
            data = grid[var][:,:,lat,lon]
            dataframe = None
            for i,e in enumerate(ens):
                name=var + str(int(e))
                print int(e),
                series = pd.Series(data=[data[i][j][0][0] for j,t in enumerate(time)],index=index,name=name).replace(missing_value,None)
                if dataframe is None:
                    dataframe = pd.DataFrame({name:series},index=index)
                    dataframe.name = var
                else:
                    dataframe[name] = series
            print
            yield dataframe
            
if __name__ == '__main__':
    gefs = GEFS()
    result = gefs.download()
    writer = pd.ExcelWriter('gefs.xls')
    for df in result:
        df.to_excel(writer,df.name)
    writer.save()
