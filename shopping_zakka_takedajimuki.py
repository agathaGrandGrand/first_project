from fake_useragent import UserAgent
import time
import pendulum
utc_time = pendulum.now()
indonesia = utc_time.in_timezone('Asia/Bangkok')
from bs4 import BeautifulSoup,Tag
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
import json
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class ShoppingZakkaTakedajimuki(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = 'shopping_zakka_takedajimuki.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "https://www.takedajimuki.co.jp/index.php/menu1/"
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None
    self.df_clean = self.clean_data(x)
    mask = self.df_clean.columns.str.contains('%')
    if from_main:
      self.df_clean.loc[:, ~mask].to_csv(f'C:/Users/wiraw/Documents/csv/wirawan_{self.file_name}.csv', index=False)
    else:
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      client = storage.Client()
      bucket = client.get_bucket('scrapingteam')
      bucket.blob(f'rokesumaalternative/all_data/wirawan_{self.file_name}.csv').upload_from_string(
        self.df_clean.loc[:, ~mask].to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    total = 0
    page = self.get_response(url)
    soup = BeautifulSoup(page.html.html,'html.parser')

    stores =soup.select('td > h4 > a') #get all stores url from soup

    for store in stores:

      self.content.append(self.get_data(store))
      print(total)
      total += 1
    
    print("============= total data",total,"=================")

  def get_data(self, store):
    open_hour = ''
    store_url = store['href']
    store_name = ''
    tel_no = ''
    handling = ''
    remarks = ''
    reg_holiday = ''
    isSmoking = ''
    isParking =''
    address = ''
    lat = ''
    lon = '' 
    
    page = self.get_response(store_url)
    soup = BeautifulSoup(page.html.html, 'html.parser')


    print("============================================= "+store_url+" =============================================")
    store_name = store.text
    address = soup.find('td',text='所在地').find_next('td').text
    try:
      tel_no = soup.find('td',text='TEL').find_next('td').text
    except:
      tel_no = ''
    open_hour = soup.find('td',text='営業時間').find_next('td').text
    isParking = soup.find('td',text='駐車場').find_next('td').text
    try:
      reg_holiday = soup.find('td',text='定休日').find_next('td').text
    except:
      pass
    _iframe = soup.select_one('iframe[data-src*="maps"]')['data-src']
    lat,lon = self.gmaps_embed_coords(self.session,_iframe)
    # lat = re.search('(?<=latitude).+',script).group(0).replace(',','').replace(':','')
    # lon = re.search('(?<=longitude).+',script).group(0).replace(',','').replace(':','')
    print("Address :",address)
    print("store Name :",store_name)
    print("phone :",tel_no)
    print("open_hour :",open_hour)
    print("handling: ",handling)
    print("remarks: ",remarks)
    print("parking lot: ",isParking)
    print("smoking: ",isSmoking)

    _store = dict()
    _store['store_name'] = store_name
    _store['chain_name'] = "文具店TAG"
    _store['CSAR_Category'] = "drg"
    _store['chain_id'] = "shopping/zakka/takedajimuki"
    _store['e_chain'] = "TAG"
    _store['categories'] = "zakka"
    _store['業種大'] = "ショッピング"
    _store['業種中'] = "雑貨/コスメ"
    _store['address'] = address
    _store['url_store'] = store_url
    _store['url_tenant'] = ''
    _store['営業時間'] =open_hour # Open hours / Business Hours
    _store['lat'] = lat
    _store['lon'] = lon
    _store['tel_no'] = tel_no
    _store['gla'] = ''
    _store['定休日'] = reg_holiday # Regular holiday
    _store['駐車場'] = isParking # Parking lot Yes ( 有 ) , No ( 無 )
    _store['禁煙・喫煙'] = isSmoking# [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
    _store['取扱'] = handling # Handling
    _store['備考'] = remarks # Remarks
    _store['scrape_date'] = indonesia.strftime("%m/%d/%y")

    print(_store)
    return _store
  # tolak angin kalo koneksi putus putus
  def get_response(self, url,trying = 0):
    headers = {'user-agent': UserAgent().random}
    try:
      res = self.session.get(url, headers=headers,timeout=10)
      return res
    except:
      print('connection trouble at:',url)
      if trying < 5: #5x perulangan
        print('try to reconnect...')
        return self.get_response(url,trying+1)

  def get_geo_code_embed_error(self,url):
      reqmap = self.session.get(url)
      soup = BeautifulSoup(reqmap.html.html, 'html.parser')

      script = soup.select('script')[0].text
      latlon = re.search('(?<=\[null\,null\,)\d{2}.\d+\,\d{3}.\d+',script).group(0).split(',')
      print(latlon)
      return latlon
  def get_geo_code_not_embed_2(self,url):
    reqmap = self.session.get(url, allow_redirects=True)
    soup = BeautifulSoup(reqmap.html.html,'html.parser')

    _marker = soup.select_one('meta[content*="markers"]')
    if _marker:
      print(_marker['content'])
      latlon = re.search('(?<=markers\=)\d{2}\.\d+%2C\d{3}\.\d+',_marker['content']).group(0).split('%2C')

    else:
      _map = soup.select_one('link[href*="branding/product"]').find_next('script').text
      try:
        latlon = _map.split('window.APP_INITIALIZATION_STATE=')[1].split(',[0,0,0],[1024,768]')[0]
        latlon = latlon.replace('[','').replace(']','').split(',')[-2:]
        latlon = [latlon[1],latlon[0]]
        
        print(latlon)
        # _map = _map.split('!3d')[1].split('service\\')[0]
      except:
        latlon = None
      
      if latlon == None or re.search('\d+\.\d+',latlon[0]) == None:
        try:
          _map = _map.split('!3d')[1].split('service\\')[0]
          print('has marker')
          print(_map)
          latlon = re.search('\d{2}\.\d+!4d\d{3}\.\d+',_map).group(0).split('!4d')
        except:
          # _map = soup.select_one('link[href*="branding/product"]').find_next('script').text
          latlon = re.search('@\d{2}\.\d+\,\d{3}\.\d+',_map).group(0).replace('@','').split(',')
    print(latlon)
    return latlon
  def get_geo_code_not_embed(self,url):
    reqmap = self.session.get(url, allow_redirects=True)
    # # raw = re.search(r'initEmbed\((.*\])', reqmap.text)
    # # r = json.loads(raw[1])
    # url = re.search('(?<=ll\=)\d+.\d+|(?<=%2C)\d+.\d+',url).group(0)
    
    # url = re.search('(?<=place\/).+(?=,\d+z\/data)',reqmap.url).group(0)
    latlon = re.findall('(?<=!3d)\d{2}\.\d+|(?<=!4d)\d{3}\.\d+',reqmap.url)
    # latlon = url.split('/@')[1].split(',')

    # url_map = f"http://www.google.com/maps/search/{url}"

    # reqmap = self.session.get(url_map, allow_redirects=True)

    # location = reqmap.html.find('meta[property="og:image"]', first=True).attrs['content']
    # print(location)

    # if re.search('markers',location):
    #     lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
    #     latlon = lat_lon[0], lat_lon[1]
    # else:
    #   latlon = re.search('(?<=center=).+(?=&zoom)',location).group(0).split('%2C')

    return latlon
  def get_geo_code(self,url):
    reqmap = self.session.get(url, allow_redirects=True)
    address = None
    address = None
    latlon = None
    lat = None
    lon = None
    gmaps_array= None
    if 'maps/embed?' in url:
        raw = re.search(r'initEmbed\((.*\])', reqmap.text)
        r = json.loads(raw[1])
        try:
            address = r[21][3][0][1]
            latlon = r[21][3][0][2]
            
            lat, lon = latlon
        except (IndexError, TypeError):
            lat, lon = '', ''

        print(address,lat,lon)
    else:
        raw = re.search(r'window\.APP_INITIALIZATION_STATE=\[(.+)\];window', reqmap.text)
        try:
            r = json.loads(raw[0].replace('window.APP_INITIALIZATION_STATE=', '').split(';window')[0])
            gmaps_array = [x for x in r[3] if x]
            latlon = json.loads(gmaps_array[1].split('\n')[1])[4][0]
            lat = latlon[2]
            lon = latlon[1]
        except (IndexError, TypeError):
            try:
                latlon = json.loads(gmaps_array[1].split('\n')[1])[0][1][0][14][9]
                lat = latlon[2]
                lon = latlon[3]
            except (IndexError, TypeError):
                lat, lon = '', ''
    return lat, lon
    '''jika kesulitan split informasi dengan <br>'''
  def get_text_with_br(self,tag, result=''):
    for x in tag.contents:
        if isinstance(x, Tag):  # check if content is a tag
            if x.name == 'br':  # if tag is <br> append it as string
                result += str(x)
            else:  # for any other tag, recurse
                result = self.get_text_with_br(x, result)
        else:  # if content is NavigableString (string), append
            result += x

    return result
  
if __name__ == '__main__':
  ShoppingZakkaTakedajimuki(True)
  



# PINDAHKAN INI KE ATAS JIKA MENGGUNAKAN RENDER
# from requests_html import AsyncHTMLSession
# import pyppeteer, asyncio
# class AsyncHTMLSessionFixed(AsyncHTMLSession):
#   def __init__(self, **kwargs):
#     super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
#     self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])
#   @property
#   async def browser(self):
#     if not hasattr(self, "_browser"):
#       self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
#                                              handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)
#     return self._browser

# TAMBAHKAN LINE INI UNTUK DEF YANG MENGGUNAKAN RENDER
# loop = asyncio.new_event_loop()
# loop.run_until_complete()