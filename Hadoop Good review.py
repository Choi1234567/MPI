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