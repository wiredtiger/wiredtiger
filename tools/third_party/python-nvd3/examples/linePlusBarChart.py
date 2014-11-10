#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import linePlusBarChart
import random
import datetime
import time


start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
nb_element = 100


output_file = open('test_linePlusBarChart.html', 'w')
type = "linePlusBarChart"
chart = linePlusBarChart(name=type, height=350, x_is_date=True, x_axis_format="%d %b %Y")
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

chart.buildhtml()
output_file.write(chart.htmlcontent)
#---------------------------------------

#close Html file
output_file.close()
