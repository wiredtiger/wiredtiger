#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import discreteBarChart


#Open File for test
output_file = open('test_discreteBarChart.html', 'w')

type = "discreteBarChart"
chart = discreteBarChart(name='mygraphname', height=400, width=600)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")
xdata = ["A", "B", "C", "D", "E", "F", "G"]
ydata = [3, 12, -10, 5, 25, -7, 2]

extra_serie = {"tooltip": {"y_start": "", "y_end": " cal"}}
chart.add_serie(y=ydata, x=xdata, extra=extra_serie)

chart.buildhtml()
output_file.write(chart.htmlcontent)
#---------------------------------------

#close Html file
output_file.close()
