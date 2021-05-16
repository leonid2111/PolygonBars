import requests
import numpy as np
import getopt, sys
import datetime
import time

#test = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2020-06-01/2020-06-17?apiKey=vHXfUcXWb7dn_qMBuFXe_Rna3SFuE7nJ'
'''
Polygon bars: response is limited to 50,000 underlying 1-minute bars (need to specify limit=50000 in the query, otherwise it's 5,000)
This is at most 50,000/60/24 = 34 days, in reality more than this, since not every minute has a bar
Discounting weekends 34*7/5=47.
Free subscription is limited to 5 API calls per minute, using 14 sec wait between the calls 
'''
def get_polygon_bars(api_key, ticker, sday, eday, interval, max_days_step=45, max_pages=24, api_wait=14):
    _s = _c = datetime.datetime.strptime(sday,'%Y-%m-%d')
    _e = datetime.datetime.strptime(eday,'%Y-%m-%d')
    _step = datetime.timedelta(days=max_days_step)
    data = []
    for k in range(max_pages):
        _c2 = min(_c + _step, _e)
        start = _c.strftime('%Y-%m-%d')
        end = _c2.strftime('%Y-%m-%d')
        print('Requesting ', start, end)
        req_string = 'https://api.polygon.io/v2/aggs/ticker/'+ticker+'/range/'+interval+'/'+start+'/'+end+'?limit=50000&apiKey='+api_key
        response = requests.get(req_string)
        if not response.status_code == 200:
            print('response status code: ', response.status_code)
            return None
        r = response.json()
        print('resultsCount: ', r['resultsCount'])
        
        for x in r['results']:
            #print(x)
            data.append(( np.datetime64(x['t'], 'ms').astype(str), x['t'],x['o'], x['h'],x['l'],x['c'],x.get('vw',np.nan),x['v'],x.get('n',0)))

        _c = _c2
        if _c2 >= _e: break
        print('api wait ', api_wait, ' sec.')
        time.sleep(api_wait)

    X = np.array(data, dtype=np.dtype([('time', 'U16'), ('t', int),('o',float),('h',float),('l',float),('c',float),('vw',float),('v',float),('n',int)]))
    return X




    
if __name__=='__main__':
    ticker = 'AA'
    sday = '2020-01-01'
    eday = '2020-04-15'
    multiplier = 1
    timespan = 'day'
    out_dir = '.'
    
    try: opts, args = getopt.getopt(sys.argv[1:], "s:e:T:m:t:D:")
    except getopt.GetoptError as err: print(err); sys.exit(2)
    for o, a in opts:
        if o == "-s":       sday = a
        elif o == "-e":     eday = a
        elif o == "-T":     ticker = a
        elif o == "-m":     multiplier = int(a)
        elif o == "-t":     timespan = a
        elif o == "-D":     out_dir = a
        else:              assert False, "unhandled option"

    api_key = open('api_key.txt','r').readline().strip()
    interval = str(multiplier)+'/'+timespan
    
    X = get_polygon_bars(api_key, ticker, sday, eday, interval)
    
    if X is not None:
        fname = out_dir+'/'+ticker+'.'+sday+'.'+eday+'.csv'
        np.savetxt(fname, X, fmt='%16s,%d,%f,%f,%f,%f,%f,%e,%d',
                   header='time,t,o,h,l,c,vw,v,n', delimiter=',')
            
