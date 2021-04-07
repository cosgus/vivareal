import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import csv
import re
from random import randint
from fake_useragent import UserAgent
from time import sleep
import statsmodels.api as sm
import sys


def scrape_viva(bairro):

    header = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
        "referer": 'www.vivareal.com.br'
    }

    referer_list = ['https://www.zillow.com',
                    'https://www.google.com',
                    'https://www.yahoo.com',
                    'https://www.duckduckgo.com',
                    'https://www.vivareal.com.br']

    ua = UserAgent()
    header.update({'user-agent': ua.random})
    scanning = True
    url = 'https://www.vivareal.com.br/venda/minas-gerais/belo-horizonte/bairros/'\
          + bairro + '/apartamento_residencial/'

    master_list = []
    page = 1

    r = requests.get(url, headers=header)
    # print(r.text)
    while 'Access denied' in r.text:
        print('trying new header')
        #print(r.text)
        header.update({'user-agent': ua.random})
        header.update({'referer': referer_list[randint(0, len(referer_list)-1)]})
        sleep(randint(3, 7))
        r = requests.get(url, headers=header)

    soup = BeautifulSoup(r.text, features='lxml')
    soup = soup.find('strong', {'class': 'results-summary__count'})
    page_count = int(soup.text.replace('.', '').replace(' ', '')) / 36

    if 'temporarily' in r.text:
        print(r.text)
        exit()

    while scanning:

        print('scanning page', str(page), 'of', str(round(page_count)))

        r = requests.get(url, headers=header)

        if 'complete this security test' in r.text:
            header.update({"user-agent": ua.random})
            continue

        soup = BeautifulSoup(r.text, features='lxml')
        soup = soup.find_all('div', {'class': 'js-card-selector'})

        page += 1
        url = 'https://www.vivareal.com.br/venda/minas-gerais/belo-horizonte/bairros/' + bairro + \
            '/apartamento_residencial/?pagina=' + str(page)
        print(url)


        master_list = master_list + list(soup)

        if page == round(page_count) + 1:
            print(r.text)
            df = pd.DataFrame(master_list)
            df.to_csv('./data/' + bairro + 'raw_data-' + bairro + '.csv', encoding='utf-8')
            scanning = False

    file = open('raw code list.csv', 'w+', newline='')

    with file:
        write = csv.writer(file)
        write.writerows(master_list)

    print('Successfully Downloaded', str(len(master_list)), 'listings from VivaReal in', bairro)

    return True


def process_data(bairro):

    try:
        df = pd.read_csv('./data/'+bairro+'/'+'raw_data-'+bairro+'.csv', encoding='utf-8', error_bad_lines=False,
                         index_col=0)
        df = df.drop(['0', '2'], axis=1)
        df = df.drop_duplicates()
        df = df.reset_index(drop=True)

    except FileNotFoundError:
        print('This neighborhood has not been scraped')
        print('Would you like to scrape? (y/n)')
        response = input()
        if response == 'y':
            df = scrape_viva(bairro)
        else:
            print('Doing nothing')
            return False

    for index, data in df.iterrows():

        soup = BeautifulSoup(df.iloc[index][0], features='lxml')
        address = soup.find('span', {'class': 'property-card__address'})
        price = soup.find('div', {'class': 'property-card__price'})
        details = soup.find_all('li', {'class': 'property-card__detail-item'})

        if 'Mês' in str(price):
            price = price.text.split('Mês')
            price2 = re.sub("[^0-9]", "", price[1])
        else:
            price2 = re.sub("[^0-9]", "", price.text)

        try:
            if int(price2) > 10_000_000:
                print(price)

        except ValueError:
            print('excepted')
            pass

        print('Address:', address.text)
        df.loc[index, 'Address'] = address.text
        print('Price:', price2)
        df.loc[index, 'Price'] = price2

        for item in details:
            if 'area' in str(item):
                item = re.sub("[^0-9]", "", item.text)
                df.loc[index, 'area'] = item
                print('Area:', item)
            elif 'bathroom' in str(item):
                item = re.sub("[^0-9]", "", item.text)
                df.loc[index, 'Bathrooms'] = item
                print('Bathrooms:', item)
            elif 'room' in str(item):
                item = re.sub("[^0-9]", "", item.text)
                df.loc[index, 'room'] = item
                print('Room:', item)
            elif 'garage' in str(item):
                item = re.sub("[^0-9]", "", item.text)
                df.loc[index, 'Garages'] = item
                print('Garage:', item)
            else:
                print('New data point:', str(item))

    df.to_csv('./data/' + bairro + '/' + bairro + '-processed.csv', index=False, encoding='utf-8')


def prepare_data(bairro):

    df = pd.read_csv('./data/' + bairro + '/' + bairro + '-processed.csv', error_bad_lines=False, encoding='utf-8')
    print('\n'*5)
    df = df.drop_duplicates()
    print('after duplicate drop:', len(df))

    df = df.dropna(axis=0)

    df = df[df['Price'] != '']

    print('after nan drop:', len(df))

    df.to_csv('./data/' + bairro + '/' + bairro + '-prepared.csv', index=False, encoding='utf-8')


def create_regression(bairro):

    df = pd.read_csv('./data/' + bairro + '/' + bairro + '-prepared.csv', error_bad_lines=False, encoding='utf-8')
    df.drop(['1'], axis=1, inplace=True)
    df = df.dropna(axis=0)

    columns = df.columns.values.tolist()
    columns.remove('Address')
    columns.remove('Price')
    y = df['Price']
    X = df[columns]

    regr = sm.OLS(y, X)
    regr = regr.fit()

    refining = True


    while refining:
        highest_p = 0
        currentAdjR = regr.rsquared_adj
        for key, value in regr.params.items():
            if regr.pvalues[key] > highest_p:
                highest_p = regr.pvalues[key]
                worst_v = key

        try:
            print('Removing', worst_v, str(highest_p))
            columns.remove(worst_v)
        except TypeError:
            return 0

        X = df[columns]

        bestregr = regr
        regr = sm.OLS(y, X)
        regr = regr.fit()
        if regr.rsquared_adj <= currentAdjR:
            refining = False
            print('not!')
            # midpoint = len(columns)//2
            # columns = columns[0:midpoint] + [worstV] + columns[midpoint:]


    try:
        f = open('./data/'+bairro+'/Results_'+bairro+'.txt','w')
        f.write(bestregr.summary().as_text())
        f.close()

        print('\n')
        bestregr.save('./data/' + bairro + '/pickle_' + bairro + '.pickle')
        print('Pickling', bairro)

    except:
        return 0


def predict(bairro):

    df = pd.read_csv('./data/' + bairro + '/' + bairro + '-prepared.csv', error_bad_lines=False, encoding='utf-8')

    df = df.dropna(axis=0)
    regr = sm.load('./data/' + bairro + '/pickle_' + bairro + '.pickle')

    regr = regr.model
    regr = regr.fit()

    for index, data in df.iterrows():
        r_sum = 0
        for key, value in regr.params.iteritems():
            r_sum = r_sum + float(value)*float(data[key])
        df.loc[index, 'Prediction'] = r_sum
        df.loc[index, 'Error'] = df.loc[index, 'Prediction'] - df.loc[index, 'Price']
        df.loc[index, 'Error Percent'] = df.loc[index, 'Error']/df.loc[index, 'Price']


    df.to_csv('./data/'+bairro+'/'+bairro+'-regressed.csv', index=False, encoding='utf-8')


def dir_check(bairros):

    for item in bairros:
        if not os.path.isdir('./data/'+item):
            os.mkdir('./data/'+item)


def add_hyperlink(bairro):

    df = pd.read_csv('./data/' + bairro + '/' + bairro + '-regressed.csv', encoding='utf-8', error_bad_lines=False)

    for index, data in df.iterrows():
        soup = BeautifulSoup(data[0], features='lxml')
        soup = soup.find('a', {'class': 'property-card__labels-container'})
        url = 'http://www.vivareal.com.br'+soup['href']

        df.loc[index, 'link'] = url

    df.to_csv('./data/' + bairro + '/' + bairro + '-final.csv', index=False, encoding='utf-8')


def select_best(bairros):

    columns = ['Address', 'Price', 'area', 'room', 'Bathrooms', 'Garages', 'Prediction', 'Error', 'Error Percent', 'link']

    best_df = pd.DataFrame(columns=columns)

    for bairro in bairros:

        df = pd.read_csv('./data/' + bairro + '/' + bairro + '-final.csv', error_bad_lines=False, encoding='utf-8')
        df = df.drop(['1'], axis=1)
        df = df.sort_values(['Error Percent'], ascending=False)
        df = df.reset_index(drop=True)
        df = df[df.index < 5]

        best_df = pd.concat([best_df, df], sort=False)

    best_df.reset_index(drop=True, inplace=True)
    best_df.to_csv('./data/Best of BH.csv', encoding='utf-8', index=False)

    print(best_df)


def main():

    bairros = ['anchieta', 'buritis', 'gutierrez', 'mangabeiras', 'lourdes']

    dir_check(bairros)

    for bairro in bairros:
        scrape_viva(bairro)
        process_data(bairro)
        prepare_data(bairro)
        create_regression(bairro)
        predict(bairro)
        add_hyperlink(bairro)

    select_best(bairros)


if __name__ == "__main__":

    main()
