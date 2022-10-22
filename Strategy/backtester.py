import numpy as np
import pandas as pd
from scipy.optimize import brute
from data_loader import *

class SAMVectorBactester:
    def __init__(self, symbol, SMA1, SMA2, start, end):
        self.symbol = symbol
        self.SMA1 = SMA1
        self.SMA2 = SMA2
        self.start = start
        self.end = end
        self.results = None
        self.get_data()

    def get_data(self):
        
        pass
    def set_parameters(self, SMA1 = None, SMA2 = None):
        pass
    def run_strategy(self):
        pass
    def plot_results(self):
        pass
    def update_and_run(self, SMA):
        pass
    def optimize_parameters(self):
        pass
