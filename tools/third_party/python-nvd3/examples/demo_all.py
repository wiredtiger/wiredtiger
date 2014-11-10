#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import cumulativeLineChart
from nvd3 import discreteBarChart
from nvd3 import lineChart
from nvd3 import linePlusBarChart
from nvd3 import lineWithFocusChart
from nvd3 import linePlusBarWithFocusChart
from nvd3 import multiBarChart
from nvd3 import multiBarHorizontalChart
from nvd3 import stackedAreaChart
from nvd3 import scatterChart
from nvd3 import pieChart
import random
import datetime
import time


start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
nb_element = 100

#Open File for test
output_file = open('test_demo_all.html', 'w')
#---------------------------------------

html_open = """
<!DOCTYPE html>
<html lang="en">
<head>
<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
<link media="all" href="./bower_components/nvd3/src/nv.d3.css" type="text/css" rel="stylesheet" />
<script src="./bower_components/d3/d3.min.js" type="text/javascript"></script>
<script src="./bower_components/nvd3/nv.d3.min.js" type="text/javascript"></script>
</head>
"""

output_file.write(html_open)

type = "discreteBarChart"
chart = discreteBarChart(name='mygraphname', height=400, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
xdata = ["A", "B", "C", "D", "E", "F", "G"]
ydata = [3, 12, -10, 5, 25, -7, 2]

extra_serie = {"tooltip": {"y_start": "", "y_end": " cal"}}
chart.add_serie(y=ydata, x=xdata, extra=extra_serie)

chart.buildcontent()
output_file.write(chart.htmlcontent)
#---------------------------------------

type = "pieChart"
chart = pieChart(color_category='category20c', height=400,
                 width=400, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

color_list = ['orange', 'yellow', '#C5E946', '#95b43f', 'red', '#FF2259', '#F6A641']
extra_serie = {"tooltip": {"y_start": "", "y_end": " cal"}, "color_list": color_list}
xdata = ["Orange", "Banana", "Pear", "Kiwi", "Apple", "Strawberry", "Pineapple"]
ydata = [3, 4, 2, 1, 5, 7, 3]

chart.add_serie(y=ydata, x=xdata, extra=extra_serie)
chart.buildcontent()
output_file.write(chart.htmlcontent)
#---------------------------------------

type = "lineChart"
chart = lineChart(height=350, x_is_date=True, x_axis_format="%d %b %Y %H",
                  jquery_on_ready=True)

chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = [x * 2 for x in ydata]

#Configure a color for a specific serie
kwargs1 = {'color': 'green'}
kwargs2 = {'color': 'red'}

extra_serie = {"tooltip": {"y_start": "There is ", "y_end": " calls"},
               "date_format": "%d %b %Y %I:%M:%S %p"}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie, **kwargs1)
extra_serie = {"tooltip": {"y_start": "", "y_end": " min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie, **kwargs2)

chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "lineWithFocusChart"
chart = lineWithFocusChart(color_category='category20b', x_is_date=True,
                           x_axis_format="%d %b %Y", jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(-10, 10) for i in list(range(nb_element))]

ydata2 = [x * 2 for x in ydata]
ydata3 = [x * 3 for x in ydata]
ydata4 = [x * 4 for x in ydata]

extra_serie = {"tooltip": {"y_start": "There is ", "y_end": " calls"},
               "date_format": "%d %b %Y %I:%M:%S"}
#extra_serie = None
chart.add_serie(name="serie 1", y=ydata, x=xdata, extra=extra_serie)
chart.add_serie(name="serie 2", y=ydata2, x=xdata, extra=extra_serie)
chart.add_serie(name="serie 3", y=ydata3, x=xdata, extra=extra_serie)
chart.add_serie(name="serie 4", y=ydata4, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)

#---------------------------------------

type = "stackedAreaChart"
chart = stackedAreaChart(height=350, x_is_date=True,
                         x_axis_format="%d %b %Y %I", jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in list(range(nb_element))]
ydata2 = [x * 2 for x in ydata]

extra_serie = {"tooltip": {"y_start": "There is ", "y_end": " calls"},
               "date_format": "%d %b %Y %I:%M:%S %p"}
chart.add_serie(name="serie 1", y=ydata, x=xdata, extra=extra_serie)
chart.add_serie(name="serie 2", y=ydata2, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "linePlusBarChart"
chart = linePlusBarChart(height=350, x_is_date=True,
                         x_axis_format="%d %b %Y", jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = [i + random.randint(1, 10) for i in reversed(list(range(nb_element)))]
kwargs = {}
kwargs['bar'] = True
extra_serie = {"tooltip": {"y_start": "$ ", "y_end": ""}}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie, **kwargs)
extra_serie = {"tooltip": {"y_start": "", "y_end": " min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "cumulativeLineChart"
chart = cumulativeLineChart(height=350, x_is_date=True,
                            jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = [x * 2 for x in ydata]

extra_serie = {"tooltip": {"y_start": "", "y_end": " Calls"}}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie)
extra_serie = {"tooltip": {"y_start": "", "y_end": " Min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "multiBarHorizontalChart"
chart = multiBarHorizontalChart(height=350, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

nb_element = 10
xdata = list(range(nb_element))
ydata = [random.randint(-10, 10) for i in range(nb_element)]
ydata2 = [x * 2 for x in ydata]
extra_serie = {"tooltip": {"y_start": "", "y_end": " Calls"}}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie)
extra_serie = {"tooltip": {"y_start": "", "y_end": " Min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "multiBarChart"
chart = multiBarChart(height=350, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
nb_element = 10
xdata = list(range(nb_element))
ydata = [random.randint(1, 10) for i in range(nb_element)]
ydata2 = [x * 2 for x in ydata]

extra_serie = {"tooltip": {"y_start": "", "y_end": " call"}}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie)
extra_serie = {"tooltip": {"y_start": "", "y_end": " min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)
chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "multiBarChartDate"
chart = multiBarChart(name=type, height=350, x_is_date=True, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
nb_element = 100
start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
xdata = range(nb_element)
xdata = map(lambda x: start_time + x * 1000000000, xdata)
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = map(lambda x: x * 2, ydata)

tooltip_date = "%d %b %Y %H:%M:%S %p"
extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " calls"},
               "date_format": tooltip_date}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie)
extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " duration"},
               "date_format": tooltip_date}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)
chart.buildcontent()

output_file.write(chart.htmlcontent)
#---------------------------------------

type = "scatterChart"
chart = scatterChart(height=350, date=False, jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
nb_element = 50
xdata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata = [i * random.randint(1, 10) for i in range(nb_element)]
ydata2 = [x * 2 for x in ydata]
ydata3 = [x * 5 for x in ydata]

kwargs1 = {'shape': 'circle', 'size': '1'}
kwargs2 = {'shape': 'cross', 'size': '10'}
kwargs3 = {'shape': 'triangle-up', 'size': '100'}

extra_serie = {"tooltip": {"y_start": "", "y_end": " calls"}}
chart.add_serie(name="serie 1", y=ydata, x=xdata, extra=extra_serie, **kwargs1)
chart.add_serie(name="serie 2", y=ydata2, x=xdata, extra=extra_serie, **kwargs2)
chart.add_serie(name="serie 3", y=ydata3, x=xdata, extra=extra_serie, **kwargs3)

chart.buildcontent()

output_file.write(chart.htmlcontent)

#--------------------------------------------
nb_element = 200

type = "linePlusBarWithFocusChart"
chart = linePlusBarWithFocusChart(height=350, x_is_date=True,
                                  x_axis_format="%d %b %Y", jquery_on_ready=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = [i + random.randint(10, 20) for i in reversed(list(range(nb_element)))]
kwargs = {}
kwargs['bar'] = True
extra_serie = {"tooltip": {"y_start": "$ ", "y_end": ""}}
chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie, **kwargs)
extra_serie = {"tooltip": {"y_start": "", "y_end": " min"}}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)

chart.buildcontent()

output_file.write(chart.htmlcontent)

#---------------------------------------

html_close = """</body></html>"""
output_file.write(html_close)

#close Html file
output_file.close()
