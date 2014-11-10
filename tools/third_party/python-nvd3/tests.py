#!/usr/bin/python
# -*- coding: utf-8 -*-

from nvd3 import lineChart
from nvd3 import lineWithFocusChart
from nvd3 import stackedAreaChart
from nvd3 import multiBarHorizontalChart
from nvd3 import linePlusBarChart
from nvd3 import cumulativeLineChart
from nvd3 import scatterChart
from nvd3 import discreteBarChart
from nvd3 import pieChart
from nvd3 import multiBarChart
from nvd3.translator import Function, AnonymousFunction, Assignment
import random
import unittest
import datetime
import time


class ChartTest(unittest.TestCase):

    def test_lineWithFocusChart(self):
        """Test Line With Focus Chart"""
        type = "lineWithFocusChart"
        chart = lineWithFocusChart(name=type, date=True, height=350)
        nb_element = 100
        xdata = list(range(nb_element))
        xdata = [1365026400000 + x * 100000 for x in xdata]
        ydata = [i + random.randint(-10, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        chart.add_serie(y=ydata, x=xdata)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_lineChart(self):
        """Test Line Chart"""
        type = "lineChart"
        chart = lineChart(name=type, date=True, height=350)
        nb_element = 100
        xdata = list(range(nb_element))
        xdata = [1365026400000 + x * 100000 for x in xdata]
        ydata = [i + random.randint(1, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        chart.add_serie(y=ydata, x=xdata)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_linePlusBarChart(self):
        """Test line Plus Bar Chart"""
        type = "linePlusBarChart"
        chart = linePlusBarChart(name=type, date=True, height=350)
        start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
        nb_element = 100
        xdata = list(range(nb_element))
        xdata = [start_time + x * 1000000000 for x in xdata]
        ydata = [i + random.randint(1, 10) for i in range(nb_element)]
        ydata2 = [i + random.randint(1, 10) for i in reversed(list(range(nb_element)))]
        kwargs = {}
        kwargs['bar'] = True
        chart.add_serie(y=ydata, x=xdata, **kwargs)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_stackedAreaChart(self):
        """Test Stacked Area Chart"""
        type = "stackedAreaChart"
        chart = stackedAreaChart(name=type, height=400)
        nb_element = 100
        xdata = list(range(nb_element))
        xdata = [100 + x for x in xdata]
        ydata = [i + random.randint(1, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        chart.add_serie(y=ydata, x=xdata)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_MultiBarChart(self):
        """Test Multi Bar Chart"""
        type = "MultiBarChart"
        chart = multiBarChart(name=type, height=400)
        nb_element = 10
        xdata = list(range(nb_element))
        ydata = [random.randint(1, 10) for i in range(nb_element)]
        chart.add_serie(y=ydata, x=xdata)
        chart.buildhtml()

    def test_multiBarHorizontalChart(self):
        """Test multi Bar Horizontal Chart"""
        type = "multiBarHorizontalChart"
        chart = multiBarHorizontalChart(name=type, height=350)
        nb_element = 10
        xdata = list(range(nb_element))
        ydata = [random.randint(-10, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        chart.add_serie(y=ydata, x=xdata)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_cumulativeLineChart(self):
        """Test Cumulative Line Chart"""
        type = "cumulativeLineChart"
        chart = cumulativeLineChart(name=type, height=400)
        start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)
        nb_element = 100
        xdata = list(range(nb_element))
        xdata = [start_time + x * 1000000000 for x in xdata]
        ydata = [i + random.randint(1, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        chart.add_serie(y=ydata, x=xdata)
        chart.add_serie(y=ydata2, x=xdata)
        chart.buildhtml()

    def test_scatterChart(self):
        """Test Scatter Chart"""
        type = "scatterChart"
        chart = scatterChart(name=type, date=True, height=350)
        nb_element = 100
        xdata = [i + random.randint(1, 10) for i in range(nb_element)]
        ydata = [i * random.randint(1, 10) for i in range(nb_element)]
        ydata2 = [x * 2 for x in ydata]
        ydata3 = [x * 5 for x in ydata]

        kwargs1 = {'shape': 'circle', 'size': '1'}
        kwargs2 = {'shape': 'cross', 'size': '10'}
        kwargs3 = {'shape': 'triangle-up', 'size': '100'}
        chart.add_serie(y=ydata, x=xdata, **kwargs1)
        chart.add_serie(y=ydata2, x=xdata, **kwargs2)
        chart.add_serie(y=ydata3, x=xdata, **kwargs3)
        chart.buildhtml()

    def test_discreteBarChart(self):
        """Test discrete Bar Chart"""
        type = "discreteBarChart"
        chart = discreteBarChart(name=type, date=True, height=350)
        xdata = ["A", "B", "C", "D", "E", "F", "G"]
        ydata = [3, 12, -10, 5, 35, -7, 2]

        chart.add_serie(y=ydata, x=xdata)
        chart.buildhtml()

    def test_pieChart(self):
        """Test Pie Chart"""
        type = "pieChart"
        chart = pieChart(name=type, height=400, width=400)
        xdata = ["Orange", "Banana", "Pear", "Kiwi", "Apple", "Strawberry", "Pineapple"]
        ydata = [3, 4, 0, 1, 5, 7, 3]
        chart.add_serie(y=ydata, x=xdata)
        chart.buildhtml()


class TranslatorTest(unittest.TestCase):

    def test_pieChart(self):
        func = Function('nv').addGraph(
            AnonymousFunction('', Assignment('chart',
                Function('nv').models.pieChart(
                    ).x(
                        AnonymousFunction('d', 'return d.label;')
                    ).y(
                        AnonymousFunction('d', 'return d.value;')
                    ).showLabels('true')
                )
            )
        )
        self.assertEqual(str(func),
                         'nv.addGraph(function() { var chart = '
                         'nv.models.pieChart().x(function(d) { return d.label; '
                         '}).y(function(d) { return d.value; }).showLabels(true); })')


if __name__ == '__main__':
    unittest.main()

# Usage
# python tests.py -v
