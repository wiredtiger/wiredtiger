#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import linePlusBarWithFocusChart
import random
import datetime
import time


start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
nb_element = 100

#Open File for test
output_file = open('test_linePlusBarWithFocusChart.html', 'w')
#---------------------------------------
type = "linePlusBarWithFocusChart"
chart = linePlusBarWithFocusChart(name=type, color_category='category20b', x_is_date=True, x_axis_format="%d %b %Y")
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

xdata = list(range(nb_element))
xdata = [start_time + x * 1000000000 for x in xdata]
ydata = [i + random.randint(-10, 10) for i in range(nb_element)]

ydata2 = [200 - i + random.randint(-10, 10) for i in range(nb_element)]

extra_serie_1 = {
    "tooltip": {"y_start": "$ ", "y_end": ""},
    "date_format": "%d %b %Y",
}
kwargs = {"bar": "true"}

chart.add_serie(name="serie 1", y=ydata, x=xdata, extra=extra_serie_1, **kwargs)

extra_serie_2 = {
    "tooltip": {"y_start": "$ ", "y_end": ""},
    "date_format": "%d %b %Y",
}
chart.add_serie(name="serie 2", y=ydata2, x=xdata, extra=extra_serie_2)

chart.buildhtml()

output_file.write(chart.htmlcontent)

#close Html file
output_file.close()
