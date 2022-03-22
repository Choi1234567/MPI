import string
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import pandas as pd
import requests
import urllib.parse
import datetime
from lxml import etree

columns = ['Date', 'Url', 'Content']
data = []


class TodayInHistory(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Mapper function
    def mapper(self, _, line):
        reader = csv.reader([line])
        # replace or punctuation to space
        translate = str.maketrans(string.punctuation, " " * len(string.punctuation))
        for row in reader:
            zipped = zip(columns, row)
            diction = dict(zipped)
            # ratings = diction['Rating']
            # split sentences to words and each word only count 1 in one content
            contents = set(diction['Content'].lower().translate(translate).split())
            for content in contents:
                yield content, 1

    # Reducer function
    def reducer(self, key, values):
        v = sum(values)
        data.append((key, v))
        yield key, v


def create_dataset(res_file_name):
    baseurl = 'https://en.wikipedia.org/wiki/'

    begin_date = datetime.datetime.strptime('2020-01-01', "%Y-%m-%d")
    wiki_data = []
    month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
             'November',
             'December']
    for i in range(366):
        content = []
        mid_date = begin_date + datetime.timedelta(days=i)
        date = month[mid_date.month - 1] + '_' + str(mid_date.day)
        url = baseurl + urllib.parse.quote(date)
        html = requests.get(url).text.encode("utf-8")

        tree = etree.HTML(html)
        ul = tree.xpath('//li[@class="toclevel-1 tocsection-1"]/ul/li/a/span[@class="toctext"]/text()')
        num = len(ul)
        for i in range(num, 0, -1):
            records = tree.xpath('//div[@id="mw-content-text"]/div[@class="mw-parser-output"]/ul[' + str(
                i) + ']/li[descendant-or-self::text()]')
            for j in range(len(records) - 1, -1, -1):
                content.append(records[j].xpath('string(.)'))
        d = (date, url, content)
        wiki_data.append(d)
    name = ['Date', 'Url', 'Content']
    pd_data = pd.DataFrame(columns=name, data=wiki_data)
    pd_data.to_csv(res_file_name, index=False)


if __name__ == "__main__":
    # use a web spider to catch all articles of "Today in History"
    create_dataset(sys.argv[1])
    # use this shell to run this code:python_Hadoop_Dataset_and_Statistics.py res.csv
    TodayInHistory.run()
    pd_data = pd.DataFrame(columns=['Word', 'Freq'], data=data)
    pd_data = pd_data.sort_values(by=['Freq'], ascending=[False])
    print("Print top 100 words (If you want to see all the words, you can find it in the file res.scv ):")
    pd.set_option('display.max_rows', 100)
    print(pd_data[:100])
    pd_data.to_csv('res.csv', index=False)