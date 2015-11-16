'''
Created on Nov 13, 2015

@author: theo
'''

import logging, re, zipfile, csv, datetime, dateutil, StringIO
from acacia.data.generators.generator import Generator
import pandas as pd

logger = logging.getLogger(__name__)

class RWSHistory(Generator):
    
    def get_header(self, f):
        header = {}
        descr = {}
        header['DESCRIPTION'] = descr
        header['COLUMNS'] = []
        f.seek(0)
        line = f.readline()
        self.skiprows = 0
        while line != '':
            if re.match('^\d{2}\-\d{2}\-\d{4}\s+\d+',line):
                # data start 
                break
            colon = line.find(':')
            if colon>0:
                key = line[:colon].strip()
                val = line[colon+1:].strip()
                descr[key]=val
                if key == 'Parameter':
                    header['COLUMNS'].append(val[:15].strip())
            line = f.readline()
            self.skiprows += 1
        return header
    
    def get_data(self, f, **kwargs):
        header = self.get_header(f)
        columns = header.get('COLUMNS',[])
        skiprows = self.skiprows if self.engine == 'python' else 0
        buf = StringIO.StringIO(f.read())
        data = self.read_csv(buf, header=None, names=columns, skiprows = skiprows, sep = r'\s+', skipinitialspace=True, comment = '#', index_col = 0, parse_dates = True)
        return data

    def get_columns(self, hdr):
        return hdr.get('COLUMNS',[])
    
    def get_parameters(self, fil):
        header = self.get_header(fil)
        names = self.get_columns(header)
        data = header['DESCRIPTION']
        params = {}
        for name in names:
            # there is only 1 parameter
            unit = data['Eenheid']
            descr = data['Parameter'][16:]
            params[name] = {'description' : descr, 'unit': unit}
        return params

class LMW10(Generator):
    # zip bestand met 10 minuten metingen van Landelijk Meetnet Water (https://www.rijkswaterstaat.nl/rws/opendata/meetdata/meetdata.zip)
    def __init__(self,*args,**kwargs):
        super(LMW10,self).__init__(*args,**kwargs)
        self.parameter = kwargs.get('parameter','H10') 
        self.locatie = kwargs.get('locatie', 'AMRO')
        self.index = -1 # row number in file
        
    def get_header(self, fil):
        header = {}
        descr = {}
        cols = []
        header['DESCRIPTION'] = descr
        header['COLUMNS'] = cols
        with zipfile.ZipFile(fil,'r') as z:
            with z.open('update.adm') as f:
                reader = csv.reader(f,delimiter=',')
                index=-1
                for row in reader:
                    index+=1
                    row = [r.strip() for r in row]
                    if self.locatie == row[1] and self.parameter == row[3]:
                        keys = ['meetnet_id','locatie_code','leeg','parameter_code','locatie','parameter','unit','from_date', 'to_date', 'format']
                        descr[self.parameter] = dict(zip(keys,row))
                        cols.append(self.parameter)
                        break
                self.skiprows = index
        return header
        
    def get_data(self, fil, **kwargs):
        if 'parameter' in kwargs:
            self.parameter = kwargs['parameter'] 
        if 'locatie' in kwargs:
            self.locatie = kwargs['locatie']
        header = self.get_header(fil)
        descr = header['DESCRIPTION'][self.parameter]
        
        fromDate = descr['from_date']
        fromDate = dateutil.parser.parse(fromDate)
        
        toDate = descr['to_date']
        toDate = dateutil.parser.parse(toDate)

        with zipfile.ZipFile(fil,'r') as z:
            with z.open('update.dat') as f:
                reader = csv.reader(f,delimiter=',')
                for _ in range(self.skiprows):
                    reader.next()
                row = reader.next()
                date = fromDate
                data = []
                index = []
                for d in row:
                    try:
                        d = float(d)
                    except:
                        d = None
                    data.append(d)
                    index.append(date)
                    date = date + datetime.timedelta(minutes=10)
        return pd.DataFrame(index=index, data={self.parameter:data}) 
    
    def get_parameters(self, fil):
        header = self.get_header(fil)
        cols = header['COLUMNS']
        desc = header['DESCRIPTION']
        params = {}
        for colname in cols:
            data = desc[colname]
            unit = data['unit']
            descr = '{param} {loc}'.format(param=data['parameter'],loc=data['locatie'])
            params[colname] = {'description' : descr, 'unit': unit}
        return params
    
if __name__ == '__main__':
    
#     gen = RWSHistory()
#     with open(r'/media/sf_F_DRIVE/acaciadata.com/nederrijnlekenwaal/ambo.txt') as f:
#         print gen.get_parameters(f)
#         print gen.get_data(f)
        
    gen = LMW10()
    datafile = r'/media/sf_F_DRIVE/acaciadata.com/meetdata.zip'
    print gen.get_parameters(datafile)
    print gen.get_data(datafile)
            