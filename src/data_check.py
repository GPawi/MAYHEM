#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Gregor Pfalz
github: GPawi
"""

import numpy as np
import pandas as pd


class data_check(object):
    def __init__(self, core):
        self.core = core

    def check_completeness(self, individual_check = False):
        self.__individual_check = individual_check
        core = self.core
        self.__completeness_check = {}
        
        self.__completeness_check['Scienist Input'] = core._data_preparation__scientist
        self.__completeness_check['Expedition Input'] =  core._data_preparation__expedition
        self.__completeness_check['Lake Input'] =  core._data_preparation__lake
        self.__completeness_check['Drilling Input'] =  core._data_preparation__drilling
        self.__completeness_check['Source Input'] =  core._data_preparation__source
        self.__completeness_check['Publication Input'] =  core._data_preparation__publication
        try: self.__completeness_check['Organic Input'] =  core._data_preparation__input_organic
        except AttributeError: pass
        try: self.__completeness_check['Grain Size Input'] =  core._data_preparation__input_grainsize
        except AttributeError: pass
        try: self.__completeness_check['Element Input'] =  core._data_preparation__input_element
        except AttributeError: pass
        try: self.__completeness_check['Mineral Input'] =  core._data_preparation__input_mineral
        except AttributeError: pass
        try: self.__completeness_check['Diatom Input'] =  core._data_preparation__input_diatom
        except AttributeError: pass
        try: self.__completeness_check['Chironomid Input'] =  core._data_preparation__input_chironomid
        except AttributeError: pass
        try: self.__completeness_check['Pollen Input'] =  core._data_preparation__input_pollen
        except AttributeError: pass
        self.__completeness_check['Age Input'] =  core._data_preparation__input_age
        
        if self.__individual_check == True:
            for name, element in self.__completeness_check.items():
                print (f'{name} has a completeness of {(1 - element.isna().stack().mean()) * 100:.2f}%')
        
        else:
            self.__overall_completeness = []
            for element in self.__completeness_check.values():
                self.__completeness_value = (1 - element.isna().stack().mean())
                self.__overall_completeness.append(self.__completeness_value)
            
            self.__overall_completeness = (sum(self.__overall_completeness)/len(self.__completeness_check)) * 100
            print (f'The dataset has an overall completeness of {self.__overall_completeness:.2f}')
