import argparse
import ast
from collections import defaultdict
import csv
from heapq import nlargest
import json
import os.path
import pandas as pd 
    
class CodeComplexity:
    def __init__(self):
        self.dataFrame = pd.DataFrame()
        self.topNRegions = 5

    def run(self, parser):
        self.args = parser.parse_args()
        self.dataFrame = pd.read_csv(self.args.data_file)

        self.get_atlas_compatible_code_statistics(self.args.summary, self.args.data_file, self.args.outfile)

    def get_atlas_compatible_code_statistics(self, summaryFile, dataFile, outfile='atlas_code_complexity.json'):
        ''' Generate the Atlas compatible format report. '''
        atlas_format = {
                    'Test Name': "Code Complexity",
                    'config': {},
                    'metrics': self.get_code_complexity(summaryFile, dataFile)
                }

        dir_name = os.path.dirname(outfile)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(outfile, 'w') as outfile:
            json.dump(atlas_format, outfile, indent=4)

    def get_code_complexity(self, summaryFile, dataFile):
        ''' Generate a list of intended contents from the complexity analysis. '''
        resultList = []
        complexity_avg_dict = {}
        complexity_avg_dict['name'] = 'Average'
        complexity_avg_dict['value']=self.get_average(summaryFile)

        complexity_ranges_dict = {}
        complexity_ranges_dict['name'] = 'Stats Ranges'
        complexity_ranges_dict['value'] = self.get_complexity_ranges_list(dataFile)

        complexity_regions_dict = {}
        complexity_regions_dict['name'] = 'Top ' + str(self.topNRegions) + ' Regions'
        complexity_regions_dict['value'] = self.get_region_list(dataFile)

        resultList.append(complexity_avg_dict)
        resultList.append(complexity_ranges_dict)
        resultList.append(complexity_regions_dict)

        return (resultList)

    def get_region_list(self, dataFile):
        ''' We intend to retrieve topNRegions/functions with the highest cyclomatic complexity values. '''
        top5 = self.dataFrame.nlargest(self.topNRegions, 'std.code.complexity:cyclomatic')

        resultList = []
        for index, row in top5.iterrows():
            atlas_format = {}
            atlas_format['name'] = row["region"]
            atlas_format['value']= row["std.code.complexity:cyclomatic"]
            resultList.append(atlas_format)

        return (resultList)

    def get_complexity_ranges_list(self, dataFile):
        ''' We intend to retrieve 3 set of cyclomatic complexity ranges, above 30, above 50 and above 90. '''
        ranges_list = [20, 50, 90]

        resultList = []
        for i in ranges_list:
            atlas_format = {}
            atlas_format['name'] = 'Above ' + str(i)
            atlas_format['value']= self.get_complexity_max_limit(dataFile, i)
            resultList.append(atlas_format)
        return (resultList)

    def get_complexity_max_limit(self, complexity_data_path: str, max_limit:int):
        ''' Retrieve the number of cyclomatic complexity values above the max_limit. '''
        column_name = 'std.code.complexity:cyclomatic'
        # Select column 'std.code.complexity:cyclomatic' from the dataframe
        column = self.dataFrame[column_name]
        # Get count of values greater than max_limit in the column 'std.code.complexity:cyclomatic' 
        count = column[column > max_limit].count()
        #Pandas aggregation functions (like sum, count and mean) returns a NumPy int64 type number
        #not a Python integer.
        # Object of type int64 is not JSON serializable | pandas aggregation functions and json.dumps() error
        return (int(count))

    def get_average(self, complexity_summary_file):
        with open(complexity_summary_file, 'r') as f:
            data = f.read()

        # Below is the format of complexity_summary_file, we are intersted in extracting the "avg" value of std.code.complexity.
        """
        {
            "view": [
                {
                    "data": {
                        "info": {"path": "./", "id": 1},
                        "aggregated-data": {
                            "std.code.complexity": {
                                "cyclomatic": {
                                    "max": 31,
                                    "min": 0,
                                    "avg": 1.470895055288963,
                                    "total": 7050.0,
                                    "count": 4793,
                                    "nonzero": False,
                                    "distribution-bars": [ ],
                                    "sup": 0,
                                }
                            },
                            "std.code.lines": {
                                "code": {
                                    "max": 1083,
                                    "min": 0,
                                    "avg": 9.738573768051213,
                                    "total": 65414.0,
                                    "count": 6717,
                                    "nonzero": False,
                                    "distribution-bars": [ ],
                                    "sup": 0,
                                }
                            },
                        },
                        "file-data": {},
                        "subdirs": [
                            "CMakeFiles",
                            "_deps",
                            "bench",
                            "config",
                            "include",
                            "lang",
                            "test",
                        ],
                        "subfiles": [],
                    }
                }
            ]
        }
        """

        d = ast.literal_eval(data)
        new_dictionary = d["view"]

        return (new_dictionary[0]['data']['aggregated-data']['std.code.complexity']['cyclomatic']['avg'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--summary', required=True, help='Path of the complexity summary file in the json format')
    parser.add_argument('-o', '--outfile', help='Path of the file to write analysis output to')
    parser.add_argument('-d', '--data_file', help='Code complexity data file in the csv format')

    obj = CodeComplexity()
    obj.run(parser)
