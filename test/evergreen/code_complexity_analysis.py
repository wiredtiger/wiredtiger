# importing the module
import argparse
import json
import csv
import os.path
import ast
from heapq import nlargest
import pandas as pd 
from collections import defaultdict
    
class CodeComplexity:
    dataFrame = pd.DataFrame()
    topNRegions = 5
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-s', '--summary', required=True, help='Path to the complexity summary data file')
        self.parser.add_argument('-o', '--outfile', help='Path of the file to write test output to')
        self.parser.add_argument('-d', '--data_file', help='Code complexity data file')

    def run(self):
        self.args = self.parser.parse_args()
        self.dataFrame = pd.read_csv(self.args.data_file)

        self.get_atlas_compatible_code_statistics(self.args.summary, self.args.data_file, self.args.outfile)

    # Generate the Atlas compatible format report.
    def get_atlas_compatible_code_statistics(self, summaryFile, dataFile, outfile='atlas_code_statistics.json'):
        atlas_format = {
                    'Test Name': "Code Statistics",
                    'config': {},
                    'metrics': self.get_code_statistics(summaryFile, dataFile)
                }

        dir_name = os.path.dirname(outfile)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        with open(outfile, 'w') as outfile:
            json.dump(atlas_format, outfile, indent=4)

    def get_code_statistics(self, summaryFile, dataFile):
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
        #data = pd.read_csv(dataFile)
        list = []
        # extracting greatest 5
        large5 = self.dataFrame.nlargest(self.topNRegions, 'std.code.complexity:cyclomatic')

        resultList = []
        for index, row in large5.iterrows():
            atlas_format = {}
            atlas_format['name'] = row["region"]
            atlas_format['value']= row["std.code.complexity:cyclomatic"]
            resultList.append(atlas_format)

        print ("Region List ")
        print (resultList)
        return (resultList)

    def get_complexity_ranges_list(self, dataFile):
        ranges_list = [20, 50, 90]

        resultList = []
        for i in ranges_list:
            atlas_format = {}
            atlas_format['name'] = 'Above ' + str(i)
            atlas_format['value']= self.get_complexity_max_limit(dataFile, i)
            resultList.append(atlas_format)
        return (resultList)

    def get_complexity_max_limit(self, complexity_data_path: str, max_limit:int):
        # making data frame from csv file
        data = pd.read_csv(complexity_data_path)

        # Extrract greatest 5
        large5 = data.nlargest(5, 'std.code.complexity:cyclomatic')

        column_name = 'std.code.complexity:cyclomatic'
        # Select column 'std.code.complexity:cyclomatic' from the dataframe
        column = data[column_name]
        # Get count of values greater than max_limit in the column 'std.code.complexity:cyclomatic' 
        count = column[column > max_limit].count()
        #Pandas aggregation functions (like sum, count and mean) returns a NumPy int64 type number
        #not a Python integer.
        # Object of type int64 is not JSON serializable | pandas aggregation functions and json.dumps() error
        return (int(count))

    def get_average(self, complexity_summary_path):
        with open(complexity_summary_path) as f:
            data = f.read()

        d = ast.literal_eval(data)
        new_dictionary = d["view"]
        return (new_dictionary[0]['data']['aggregated-data']['std.code.complexity']['cyclomatic']['avg'])


if __name__ == '__main__':
    #main()
    obj = CodeComplexity()
    obj.run()
