import os
import requests
import zipfile
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
def download_all_matches(download_dir='All_matches/All_match_data'):
    os.makedirs(download_dir,exist_ok=True)
    # setting up selenium
    chrome_options=webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=chrome_options)
    driver.get('https:/cricsheet.org/downloads/')
     # parsing the entire html to readable by using Beatuiful Soup
    soup=BeautifulSoup(driver.page_source,'html.parser')
    driver.quit()
    # finding the link 'all_json.zip' link
    link=soup.find('a',href=lambda href: href and 'all_json.zip' in href)
    if not link:
        raise Exception('Link not found')
    url=link['href']
    if not url.startswith('https'):
        url='https://cricsheet.org'+url
    print(f'Find the link:{url}')
    # Downloading the file
    zip_path=os.path.join(download_dir,'all_matches.zip')
    headers={'User-Agent':'Mozilla/5.0'}
    response=requests.get(url,stream=True,headers=headers)
    response.raise_for_status()
    with open(zip_path,'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f'file downloded successfully ({os.path.getsize(zip_path)/1024:.1f} KB)')
    # Extracting the fzip file
    extract_path=os.path.join(download_dir,'all_matches')
    os.makedirs(extract_path,exist_ok=True)
    with zipfile.ZipFile(zip_path,'r') as zip_ref:
        zip_ref.extractall(extract_path)
    num_files=len([f for f in os.listdir(extract_path)if f.endswith('.json')])
    print(f'Sucessfully Filed Down loaded and extracted into {extract_path}')
    os.remove(zip_path)
    return extract_path
if __name__=='__main__':
    download_all_matches(download_dir='D:/GUVI AIML/Projects/Cricsheet Match Data Analysis/data')