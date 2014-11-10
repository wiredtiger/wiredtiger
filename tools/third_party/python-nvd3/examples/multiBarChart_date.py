#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import multiBarChart
import random
import time
import datetime

#Open File for test
output_file = open('test_multiBarChart_date.html', 'w')

type = "multiBarChart"

chart = multiBarChart(name=type, height=350, x_is_date=True)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

nb_element = 100
start_time = int(time.mktime(datetime.datetime(2013, 6, 1).timetuple()) * 1000)
xdata = range(nb_element)
xdata = map(lambda x: start_time + x * 100000000, xdata)
ydata = [i + random.randint(1, 10) for i in range(nb_element)]
ydata2 = map(lambda x: x * 2, ydata)

tooltip_date = "%d %b %Y %H:%M:%S %p"
extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " calls"},
               "date_format": tooltip_date}

chart.add_serie(name="Count", y=ydata, x=xdata, extra=extra_serie)

extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " duration"},
               "date_format": tooltip_date}
chart.add_serie(name="Duration", y=ydata2, x=xdata, extra=extra_serie)
chart.buildhtml()

output_file.write(chart.htmlcontent)
#---------------------------------------

#close Html file
output_file.close()
