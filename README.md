题意1：Problem MPI Approximation of the definite integral

Implement the approximation of the definite integral with trapezoidal rule (uniform grid)

https://en.wikipedia.org/wiki/Trapezoidal_rule](https://en.wikipedia.org/wiki/Trapezoidal_rule

Your programme should:

- get range [a, b] from command line arguments (sys.argv)
- get N (number of equally spaced panels) from command line arguments
- the main process should send [a, b], N, and function f via send/receive operations.

For excellent mark.

Add additional argument r — block size.

- split N for N/r blocks (possibly not equal size) approximately of size r.
- send task to processes (with rank 1+) after one finished calculation of previous block
- when calculation is finished (all blocks are calculated) send specific message to terminate that process

~~~text
```
from mpi4py import MPI
import sys


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    a, b, N, r = accept()
    dest_rank = 0
    if rank == 0:
        assert N != 0
        length = (b - a) / N
        left = a
        right = a + length
        total = 0
        i = 0
        while i < N:
            intervals = []
            for j in range(r):
                # join r intervals to a block and send it to rank 1+
                intervals.append(
                    {
                        'a': left,
                        'b': right
                    }
                )
                left += length
                right += length
                i += 1
                if i >= N:
                    break
            data = {
                'function': f,
                'intervals': intervals,
                'finish': False
            }
            comm.send(data, dest=dest_rank % (size - 1) + 1, tag=11)
            # wait for finished calculation of previous block
            res = comm.recv(source=dest_rank % (size - 1) + 1, tag=11)
            dest_rank += 1
            total += res
        print(total, flush=True)
        for i in range(1, size):
            data = {
                'finish': True
            }
            comm.send(data, dest=i, tag=11)

    else:
        while True:
            data = comm.recv(source=0, tag=11)
            if data['finish']:
                break
            intervals = data['intervals']
            answer = 0
            for i in intervals:
                answer += area(data['function'], i['a'], i['b'])
            comm.send(answer, dest=0, tag=11)


def accept():
    a = int(sys.argv[1])
    b = int(sys.argv[2])
    N = int(sys.argv[3])
    r = int(sys.argv[4])
    return a, b, N, r


def f(x):
    y = 1.25 * x + 2
    return y


def area(f, a, b):
    ar = ((f(a) + f(b)) / 2) * (b - a)
    return ar


if __name__ == '__main__':
    main()
~~~



题意2: Problem Hadoop Good review

Article (https://medium.com/geekculture/mapreduce-with-python-5d12a772d5b3) contains description how MapReduce program can be written and run in python.

Note. You should install package **mrjob**.

Write MapReduce program and provide source with commented output for next task:

- Given dataset with hotel reviews (https://www.kaggle.com/yash10kundu/hotel-reviews).
- Count number of reviews with ''positive'' words (nice, good, excellent). One number for each word.
- Count number of reviews with ''negative'' word. You should specify ''negative'' words yourself.
- Run your program locally in mrjob.
- For excellent mark run your program inside hadoop (you should install in before or find and use docker image).

Submit your source with multiline comment with your program run output.

```
import string

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# split by ,
columns = 'Review,Rating'.split(',')
positive = 'nice,good,excellent'.split(',')
negative = 'bad,terrible,dirty,horrible,messy'.split(',')
stat_words = positive + negative
positive_map = {}
negative_map = {}


class NoRatings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    # Mapper function
    def mapper_get_ratings(self, _, line):
        reader = csv.reader([line])
        # replace or punctuation to space
        translate = str.maketrans(string.punctuation, " " * len(string.punctuation))
        for row in reader:
            zipped = zip(columns, row)
            diction = dict(zipped)
            # ratings = diction['Rating']
            # split sentences to words
            reviews = diction['Review'].lower().translate(translate).split()
            for review in reviews:
                # only count positive word and negative word
                if review in stat_words:
                    yield review, 1

    # Reducer function
    def reducer_count_ratings(self, key, values):
        v = sum(values)
        if key in positive:
            positive_map[key] = v
        if key in negative:
            negative_map[key] = v
        yield key, v


if __name__ == "__main__":
    # use this shell to run this code:
    # 1, open cmd
    # 2, format hdfs
    # 3, go to \hadoop-2.8.5\sbin directory
    # 4, start-all.cmd
    NoRatings.run()
    # sort by value
    positive_key = sorted(positive_map.items(), key=lambda x: x[1], reverse=True)
    negative_key = sorted(negative_map.items(), key=lambda x: x[1], reverse=True)
    print("The result of positive words:")
    for k, v in positive_key:
        print(k, positive_map[k])
    print("The result of negative words:")
    for k, v in negative_key:
        print(k, negative_map[k])
```



题意3:Problem Hadoop Dataset and Statistics

You will prepare dataset and write mapreduce program to process it.

Write MapReduce program and provide source with commented output for next task:

- Create dataset from 10-20 any articles from wikipedia. You can do it with single copy-and-paste.
- For each word that occurs at least in one article: count number of articles with this word.
- Run your program locally in mrjob.
- For excellent mark create a program that create dataset of nnn articles (some python libraries are very useful).

Submit your source with multiline comment with your program run output.

```
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
```



​            