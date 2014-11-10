#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import discreteBarChart
import random
import datetime
import time


#Open File for test
output_file = open('test_discreteBarChart_with_date.html', 'w')

type = "discreteBarChart"

start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
nb_element = 10

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(1, 10) for i in range(nb_element)]

chart = discreteBarChart(name=type, height=400, width=600, x_is_date=True, x_axis_format="%d-%b")
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

extra_serie = {"tooltip": {"y_start": "", "y_end": " cal"}}
chart.add_serie(y=ydata, x=xdata, extra=extra_serie)

chart.buildhtml()
output_file.write(chart.htmlcontent)
#---------------------------------------

#close Html file
output_file.close()
