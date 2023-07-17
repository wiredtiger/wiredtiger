import argparse
import ast
from collections import defaultdict
import csv
from heapq import nlargest
import json
import os.path
import pandas as pd

def get_atlas_compatible_code_statistics(summaryFile, dataFile, outfile='atlas_code_complexity.json'):
    ''' Generate the Atlas compatible format report. '''
    atlasFormat = {
                'Test Name': "Code Complexity",
                'config': {},
                'metrics': get_code_complexity(summaryFile, dataFile)
            }

    dirName = os.path.dirname(outfile)
    if dirName:
        os.makedirs(dirName, exist_ok=True)

    with open(outfile, 'w') as outfile:
        json.dump(atlasFormat, outfile, indent=4)

def get_code_complexity(summaryFile, dataFile):
    ''' Generate a list of intended contents from the complexity analysis. '''
    resultList = []
    complexityDict = {}
    complexityDict['Average'] = get_average(summaryFile)
    complexityDict['Stats Ranges'] = get_complexity_ranges_list(dataFile)
    complexityDict['Top ' + str(topNRegions) + ' Regions'] = get_region_list(dataFile)
    
    resultList.append(complexityDict)
    return (resultList)

def get_region_list(dataFile):
    ''' Retrieve topNRegions/functions with the highest cyclomatic complexity values. '''
    top5 = dataFrame.nlargest(topNRegions, 'std.code.complexity:cyclomatic')
    atlasFormat = {}
    for index, row in top5.iterrows():
        atlasFormat[row["region"]] = row["std.code.complexity:cyclomatic"]

    return (atlasFormat)

def get_complexity_ranges_list(dataFile):
    ''' Retrieve 3 set of cyclomatic complexity ranges, above 30, above 50 and above 90. '''
    rangesList = [20, 50, 90]
    atlasFormat = {}
    for i in rangesList:
        aboveRangeString = 'Above ' + str(i)
        atlasFormat[aboveRangeString] = get_complexity_max_limit(dataFile, i)

    return (atlasFormat)

def get_complexity_max_limit(complexity_data_path: str, max_limit:int):
    ''' Retrieve the number of cyclomatic complexity values above the max_limit. '''
    columnName = 'std.code.complexity:cyclomatic'
    # Select column 'std.code.complexity:cyclomatic' from the dataframe
    column = dataFrame[columnName]
    # Get count of values greater than max_limit in the column 'std.code.complexity:cyclomatic'
    count = column[column > max_limit].count()
    """
    Pandas aggregation functions (like sum, count and mean) returns a NumPy int64 type number not a Python integer.
    Object of type int64 is not JSON serializable so need to convert into an int.
    """
    return (int(count))

def get_average(complexity_summary_file):
    ''' Retrieve the cyclomatic code complexity value '''
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
                                "max": 91,
                                "min": 0,
                                "avg": 4.942372881355932,
                                "total": 14580.0,
                                "count": 2950,
                                "nonzero": False,
                                "distribution-bars": [ ],
                                "sup": 0,
                            }
                        },
                        "std.code.lines": {
                            "code": { }
                        },
                    },
                    "file-data": {},
                    "subdirs": [ ],
                    "subfiles": [ ],
                }
            }
        ]
    }
    """

    d = ast.literal_eval(data)
    newDictionary = d["view"]

    return (newDictionary[0]['data']['aggregated-data']['std.code.complexity']['cyclomatic']['avg'])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--summary', required=True, help='Path of the complexity summary file in the json format')
    parser.add_argument('-o', '--outfile', help='Path of the file to write analysis output to')
    parser.add_argument('-d', '--data_file', help='Code complexity data file in the csv format')

    args = parser.parse_args()
    global dataFrame
    global topNRegions

    dataFrame = pd.read_csv(args.data_file)
    topNRegions = 5
    get_atlas_compatible_code_statistics(args.summary, args.data_file, args.outfile)

if __name__ == '__main__':
    main()
