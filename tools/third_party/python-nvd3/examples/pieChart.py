#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Examples for Python-nvd3 is a Python wrapper for NVD3 graph library.
NVD3 is an attempt to build re-usable charts and chart components
for d3.js without taking away the power that d3.js gives you.

Project location : https://github.com/areski/python-nvd3
"""

from nvd3 import pieChart


#Open File for test
output_file = open('test_pieChart.html', 'w')

type = "pieChart"
chart = pieChart(name=type, color_category='category20c', height=400, width=400)
chart.set_containerheader("\n\n<h2>" + type + "</h2>\n\n")

extra_serie = {"tooltip": {"y_start": "", "y_end": " cal"}}
xdata = ["Orange", "Banana", "Pear", "Kiwi", "Apple", "Strawberry", "Pineapple"]
ydata = [3, 4, 0, 1, 5, 7, 3]

chart.add_serie(y=ydata, x=xdata, extra=extra_serie)
chart.buildhtml()
output_file.write(chart.htmlcontent)
#---------------------------------------

#close Html file
output_file.close()
