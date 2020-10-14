#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Gregor Pfalz
github: GPawi

Plans for v2: combine diatom, chironomid and pollen data prep in one function
"""

import numpy as np
import pandas as pd
import sqlalchemy
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from psycopg2.extras import NumericRange


class data_preparation(object):
    def __init__(self, filename = None, suppress_message = False):
        self.__filename = filename
        self.__suppress_message = suppress_message
                
    def __select_data__(self):
        __filename = self.__filename
        #sheets = ['DatasetDescription',
        #          'Organic',
        #          'GrainSize',
        #          'Element',
        #          'Mineral',
        #          'Diatom',
        #          'Chironomid',
        #          'Pollen',
        #          'Age']
        
        if __filename == None: 
            root = Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)
            __filename = askopenfilename()
            get_ipython().run_line_magic('gui', 'tk')
            xl = pd.ExcelFile(__filename)
            self.__input_dictionary = {}
            for sheet in xl.sheet_names:
                self.__input_dictionary[f'{sheet}']= pd.read_excel(xl,sheet_name=sheet)
        else:
            xl = pd.ExcelFile(__filename)
            self.__input_dictionary = {}
            for sheet in xl.sheet_names:
                self.__input_dictionary[f'{sheet}']= pd.read_excel(xl,sheet_name=sheet)
                
    def __main_input__(self):
        __input_dictionary = self.__input_dictionary
        self.__input_main = __input_dictionary['DatasetDescription']
        
    def __coreid__(self):
        __input_main = self.__input_main
        ##
        try:
            self.__coreid = __input_main[(__input_main == 'Unique CoreID').any(1)].dropna(axis = 1).iloc[0,1]
        except IndexError:
            raise Exception('Please add a unique CoreID!')
          
    def __scientist_input__(self):
        __input_main = self.__input_main
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        ##
        ### PI ###
        self.__scientist_columns = ['firstname','lastname','email','orcid']
        self.__scientist = pd.DataFrame(columns = self.__scientist_columns)
        try:
            self.__firstname = __input_main[(__input_main == 'First Name Contributer').any(1)].dropna(axis = 1).iloc[0,1]
            self.__scientist.at[0,'firstname'] = self.__firstname
            self.__lastname = __input_main[(__input_main == 'Last Name Contributer').any(1)].dropna(axis = 1).iloc[0,1]
            self.__scientist.at[0,'lastname'] = self.__lastname
        except IndexError:
            raise Exception('Please add the first and last name of the main contributer!')
        try:
            self.__email = __input_main[(__input_main == 'Email').any(1)].dropna(axis = 1).iloc[0,1]
        except IndexError:
            if __suppress_message == False:
                print(f'No Email found for {self.__firstname} {self.__lastname} - will set NULL')
                self.__email = None
            else:
                self.__email = None
        self.__scientist.at[0,'email'] = self.__email
        try:
            self.__orcid = __input_main[(__input_main == 'ORCID').any(1)].dropna(axis = 1).iloc[0,1]
        except IndexError:
            if __suppress_message == False:
                print(f'No ORCID found for {self.__firstname} {self.__lastname} - will set NULL')
                self.__orcid = None
            else:
                self.__orcid = None
        self.__scientist.at[0,'orcid'] = self.__orcid
        ### Additional Scientists ###
        self.__start_index_addsci = __input_main[(__input_main == 'Additional involved scientist').any(1)].dropna(axis = 1).index[0]
        self.__start_index_addsci = self.__start_index_addsci + 1
        self.__stop_index_addsci = __input_main[(__input_main == 'Additional involved scientist')].index.stop
        
        self.__start_column_addsci = __input_main[(__input_main == 'Additional involved scientist').any(1)].dropna(axis = 1).columns[0]
        self.__start_column_addsci = (__input_main.columns.get_loc(self.__start_column_addsci))
        self.__stop_column_addsci = self.__start_column_addsci + 4
        
        self.__addsci = __input_main.iloc[self.__start_index_addsci:self.__stop_index_addsci,self.__start_column_addsci:self.__stop_column_addsci]
        self.__addsci.rename(columns=self.__addsci.iloc[0], inplace = True)
        if len(self.__addsci) > 1:
            self.__addsci = self.__addsci.drop(self.__addsci.index[0])
            self.__addsci = self.__addsci.rename(columns = {'First Name':'firstname',
                                                            'Last Name':'lastname',
                                                            'Email':'email',
                                                            'ORCID':'orcid'})
            self.__scientist = self.__scientist.append(self.__addsci, ignore_index = True)
        else:
            if __suppress_message == False:
                print(f'No additional scientist found for {__coreid}')
            else:
                pass
        
    def __expedition_input__(self):
        __input_main = self.__input_main
        ##
        self.__expedition_columns = ['expeditionname','expeditionyear']
        self.__expedition = pd.DataFrame(columns = self.__expedition_columns)
        try:
            self.__expeditionname = __input_main[(__input_main == 'Name of Field Campaign').any(1)].dropna(axis = 1).iloc[0,1]
            self.__expedition.at[0,'expeditionname'] = self.__expeditionname
            self.__expeditionyear = __input_main[(__input_main == 'Year of Field Campaign').any(1)].dropna(axis = 1).iloc[0,1]
            self.__expedition.at[0,'expeditionyear'] = self.__expeditionyear
        except IndexError:
            raise Exception('Please add information about the expedition!')
            
    def __lake_input__(self):
        __input_main = self.__input_main
        ##
        self.__lake_columns = ['sitename','country',
                             'lakedepth','lakeextent',
                             'catchmentarea','climatezone',
                             'vegetationzone','laketype']
        self.__lake = pd.DataFrame(columns = self.__lake_columns)
        try:
            self.__sitename = __input_main[(__input_main == 'Site Name').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'sitename'] = self.__sitename
            self.__country = __input_main[(__input_main == 'Country').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'country'] = self.__country
            self.__lakedepth = __input_main[(__input_main == 'Lake Depth (m)').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'lakedepth'] = self.__lakedepth
            self.__lakeextent = __input_main[(__input_main == 'Lake Extent (km²)').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'lakeextent'] = self.__lakeextent
            self.__catchmentarea = __input_main[(__input_main == 'Catchment Area (km²)').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'catchmentarea'] = self.__catchmentarea
            self.__climatezone = __input_main[(__input_main == 'Climate Zone').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'climatezone'] = self.__climatezone
            self.__vegetationzone = __input_main[(__input_main == 'Vegetation Zone').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'vegetationzone'] = self.__vegetationzone
            self.__laketype = __input_main[(__input_main == 'Lake Type').any(1)].dropna(axis = 1).iloc[0,1]
            self.__lake.at[0,'laketype'] = self.__laketype
        except IndexError:
            raise Exception('Please add more information about the lake!')
    
    def __drilling_input__(self):
        __input_main = self.__input_main
        __coreid = self.__coreid
        __sitename = self.__sitename
        __expeditionname = self.__expeditionname
        __expeditionyear = self.__expeditionyear
        ##
        self.__drilling_columns = ['coreid',
                                 'latitude','longitude',
                                 'waterdepth','corelength',
                                 'drillingdevice','sitename',
                                 'expeditionname','expeditionyear']
        self.__drilling = pd.DataFrame(columns = self.__drilling_columns)
        try:
            self.__drilling.at[0,'coreid'] = __coreid
            self.__latitude = __input_main[(__input_main == 'Latitude').any(1)].dropna(axis = 1).iloc[0,1]
            self.__drilling.at[0,'latitude'] = self.__latitude
            self.__longitude = __input_main[(__input_main == 'Longitude').any(1)].dropna(axis = 1).iloc[0,1]
            self.__drilling.at[0,'longitude'] = self.__longitude
            self.__waterdepth = __input_main[(__input_main == 'Water Depth (m)').any(1)].dropna(axis = 1).iloc[0,1]
            self.__drilling.at[0,'waterdepth'] = self.__waterdepth
            self.__corelength = __input_main[(__input_main == 'Core Length (m)').any(1)].dropna(axis = 1).iloc[0,1]
            self.__drilling.at[0,'corelength'] = self.__corelength
            self.__drillingdevice = __input_main[(__input_main == 'Drilling Device').any(1)].dropna(axis = 1).iloc[0,1]
            self.__drilling.at[0,'drillingdevice'] = self.__drillingdevice
            self.__drilling.at[0,'sitename'] = __sitename
            self.__drilling.at[0,'expeditionname'] = __expeditionname
            self.__drilling.at[0,'expeditionyear'] = __expeditionyear
        except IndexError:
            raise Exception('Please add more information about the lake!')
    
    def __source_input__(self):
        __input_main = self.__input_main
        ##
        self.__start_index_source = __input_main[(__input_main == 'Sources').any(1)].dropna(axis = 1).index[0]
        self.__start_index_source = self.__start_index_source + 1 
        self.__stop_index_source = __input_main[(__input_main == 'Age').any(1)].dropna(axis = 1).index[0]
        self.__stop_index_source = self.__stop_index_source + 1
        
        self.__start_column_source = __input_main[(__input_main == 'Sources').any(1)].dropna(axis = 1).columns[0]
        self.__start_column_source = (__input_main.columns.get_loc(self.__start_column_source))
        self.__stop_column_source = self.__start_column_source + 4
        
        self.__source = __input_main.iloc[self.__start_index_source:self.__stop_index_source, self.__start_column_source:self.__stop_column_source]
        self.__source.rename(columns= self.__source.iloc[0], inplace = True)
        self.__source = self.__source.drop(self.__source.index[0])
        self.__source = self.__source.rename(columns = {'Entity':'entity',
                                                    'Repository':'repository',
                                                    'File Name':'filename',
                                                    'Accessible':'accessible'})
        self.__source = self.__source.dropna(axis = 0)
        
    def __publication_input__(self):
        __input_main = self.__input_main
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        ##
        self.__publication_index_search = __input_main[(__input_main == 'Publication').any(1)].dropna(axis = 1)
        self.__start_index_publication = self.__publication_index_search[(self.__publication_index_search == 'Publication').any(1)].index[0]
        self.__start_index_publication = self.__start_index_publication + 1 
        
        self.__stop_index_publication = __input_main[(__input_main == 'Additional involved scientist').any(1)].dropna(axis = 1).index[0]
        self.__stop_index_publication = self.__stop_index_publication - 2
        
        self.__publication_column_search = __input_main[(__input_main == 'Publication').any(1)].dropna(axis = 1)
        self.__start_column_publication = self.__publication_column_search[(self.__publication_column_search == 'Publication').any(1)].columns[0]
        self.__start_column_publication = (__input_main.columns.get_loc(self.__start_column_publication))
        self.__stop_column_publication = self.__start_column_publication + 3
        
        
        
        self.__publication = __input_main.iloc[self.__start_index_publication:self.__stop_index_publication, self.__start_column_publication:self.__stop_column_publication]
        self.__publication.rename(columns= self.__publication.iloc[0], inplace = True)
        if len(self.__publication) > 1:
            self.__publication = self.__publication.drop(self.__publication.index[0])
            self.__publication = self.__publication.rename(columns = {'Type':'type',
                                                 'Short Version':'pubshort',
                                                 'Full Citation':'citation'})
            self.__publication = self.__publication.dropna(axis = 0)
        else:
            if __suppress_message == False:
                print(f'No __publication found for {__coreid}')
            else:
                pass
        
    def __organic_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__input_organic = __input_dictionary['Organic']
            self.__input_organic.columns = self.__input_organic.iloc[6]
            self.__input_organic = self.__input_organic.drop(self.__input_organic.index[6])
            self.__input_organic = self.__input_organic.iloc[6:, 1:7]
            self.__input_organic.rename(columns={'MeasurementID':'measurementid',
                                                 'Nitrogen \n(TN, %)':'tn',
                                                 'Total Carbon \n(TC, %)':'tc',
                                                 'Total Organic Carbon \n(TOC, %)':'toc',
                                                 'δ13C  \n(‰ vs. VPDB)':'d13c',
                                                 'water content (%)':'water_content'}, inplace=True)
            ### For detection limit
            self.__input_organic.reset_index(drop = True, inplace = True)
            for i in range(0, len(self.__input_organic)):
                for j in range(1,6):
                    if type(self.__input_organic.iloc[i,j]) is str and '<' in self.__input_organic.iloc[i,j]:
                        __organic_array = self.__input_organic.iloc[i,j].split('<')
                        __organic_indi = NumericRange(None, round(float(__organic_array[1]),3), bounds = '()', empty = False)
                        self.__input_organic.iloc[i,j] = __organic_indi
                    elif type(self.__input_organic.iloc[i,j]) is str and '>' in self.__input_organic.iloc[i,j]:
                        __organic_array = self.__input_organic.iloc[i,j].split('>')
                        __organic_indi = NumericRange(round(float(__organic_array[1]),3), None, bounds = '[)', empty = False)
                        self.__input_organic.iloc[i,j] = __organic_indi
                    elif np.isnan(self.__input_organic.iloc[i,j]) == True:
                        pass
                    else:
                        __organic_indi = NumericRange(round(self.__input_organic.iloc[i,j],3), round(self.__input_organic.iloc[i,j],3), bounds = '[]', empty = False)
                        self.__input_organic.iloc[i,j] = __organic_indi
            ###
        except KeyError:
            if __suppress_message == False:
                print(f'No organic data for {__coreid}')
            else:
                pass
    
    def __grainsize_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__input_grainsize = __input_dictionary['GrainSize']
            self.__input_grainsize.columns = self.__input_grainsize.iloc[6]
            self.__input_grainsize = self.__input_grainsize.drop(self.__input_grainsize.index[6])
            self.__input_grainsize = self.__input_grainsize.iloc[6:, 1:12]
            self.__input_grainsize.rename(columns={'MeasurementID':'measurementid',
                                                   'Total Clay (%)':'total_clay',
                                                   'Total Silt (%)':'total_silt',
                                                   'Fine \nSilt (%)':'fine_silt',
                                                   'Medium Silt (%)':'medium_silt',
                                                   'Coarse Silt (%)':'coarse_silt',
                                                   'Total Sand (%)':'total_sand',
                                                   'Fine \nSand (%)':'fine_sand',
                                                   'Medium Sand (%)':'medium_sand',
                                                   'Coarse Sand (%)':'coarse_sand',
                                                   'Total Gravel (%)':'total_gravel'}, inplace=True)
        except KeyError:
            if __suppress_message == False:
                print(f'No grain size data for {__coreid}')
            else:
                pass
    
    def __element_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__element_data = __input_dictionary['Element']
            self.__element_data.columns = self.__element_data.iloc[5]
            self.__element_data = self.__element_data.drop(self.__element_data.index[5])
            self.__element_data = self.__element_data.iloc[5:, 1:]
            self.__element_data = self.__element_data.dropna(axis = 1, how = 'all')
            self.__element_names = self.__element_data.columns[1:].values.tolist()
            self.__input_element= pd.DataFrame(columns = ['measurementid','element_name','element_value'])
            for j in range(1,len(self.__element_names)+1):
                for k in range(0,len(self.__element_data)):
                    self.__element_row = pd.DataFrame(np.array([[self.__element_data.iloc[k,0],self.__element_names[j-1],self.__element_data.iloc[k,j]]]), columns = ['measurementid','element_name','element_value'])
                    self.__input_element= self.__input_element.append(self.__element_row)
            ### For detection limit
            self.__input_element.reset_index(drop = True, inplace = True)
            for i in range(0, len(self.__input_element)):
                if type(self.__input_element.iloc[i,2]) is str and '<' in self.__input_element.iloc[i,2]:
                    __element_array = self.__input_element.iloc[i,2].split('<')
                    __element_indi = NumericRange(None, round(float(__element_array[1]),3), bounds = '()', empty = False)
                    self.__input_element.iloc[i,2] = __element_indi
                elif type(self.__input_element.iloc[i,2]) is str and '>' in self.__input_element.iloc[i,2]:
                    __element_array = self.__input_element.iloc[i,2].split('>')
                    __element_indi = NumericRange(round(float(__element_array[1]),3), None, bounds = '[)', empty = False)
                    self.__input_element.iloc[i,2] = __element_indi
                else:
                    __element_indi = NumericRange(round(float(self.__input_element.iloc[i,2]),3), round(float(self.__input_element.iloc[i,2]),3), bounds = '[]', empty = False)
                    self.__input_element.iloc[i,2] = __element_indi
            ###
        except KeyError:
            if __suppress_message == False:
                print(f'No elemental data for {__coreid}')
            else:
                pass
    
    def __mineral_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__mineral_data = __input_dictionary['Mineral']
            self.__mineral_data.columns = self.__mineral_data.iloc[5]
            self.__mineral_data = self.__mineral_data.drop(self.__mineral_data.index[5])
            self.__mineral_data = self.__mineral_data.drop(columns = 'Mineral')
            self.__mineral_data = self.__mineral_data.dropna(axis = 1, how = 'all')
            self.__angstrom_list = self.__mineral_data.iloc[5, 1:].values.tolist()
            self.__mineral_data = self.__mineral_data.iloc[6:, 0:]
            self.__mineral_names = self.__mineral_data.columns[1:].values.tolist()
            self.__input_mineral = pd.DataFrame(columns = ['measurementid','mineral_name','mineral_wavelength', 'mineral_intensity'])     
            for j in range(1,len(self.__mineral_names)+1):
                for k in range(0,len(self.__mineral_data)): 
                    self.__mineral_row = pd.DataFrame(np.array([[self.__mineral_data.iloc[k,0],self.__mineral_names[j-1],self.__angstrom_list[j-1],self.__mineral_data.iloc[k,j]]]), columns = ['measurementid','mineral_name','mineral_wavelength', 'mineral_intensity'])
                    self.__input_mineral = self.__input_mineral.append(self.__mineral_row)
        except KeyError:
            if __suppress_message == False:
                print(f'No mineral data for {__coreid}')
            else:
                pass
    
    def __diatom_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__diatom_data = __input_dictionary['Diatom']
            self.__diatom_data.columns = self.__diatom_data.iloc[5]
            self.__diatom_data = self.__diatom_data.drop(self.__diatom_data.index[5])
            self.__diatom_data = self.__diatom_data.iloc[5:, 1:]
            self.__diatom_data = self.__diatom_data.dropna(axis = 1, how = 'all')
            self.__diatom_names = self.__diatom_data.columns[1:].values.tolist()
            self.__input_diatom = pd.DataFrame(columns = ['measurementid','diatom_taxa','diatom_count'])
            for j in range(1,len(self.__diatom_names)+1):
                for k in range(0,len(self.__diatom_data)):
                    self.__diatom_row = pd.DataFrame(np.array([[self.__diatom_data.iloc[k,0],self.__diatom_names[j-1],self.__diatom_data.iloc[k,j]]]), columns = ['measurementid','diatom_taxa','diatom_count'])
                    self.__input_diatom = self.__input_diatom.append(self.__diatom_row)
        except KeyError:
            if __suppress_message == False:
                print(f'No diatom data for {__coreid}')
            else:
                pass
    
    def __chironomid_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__chironomid_data = __input_dictionary['Chironomid']
            self.__chironomid_data.columns = self.__chironomid_data.iloc[6]
            self.__chironomid_data = self.__chironomid_data.drop(self.__chironomid_data.index[6])
            self.__chironomid_data  = self.__chironomid_data.iloc[6:, 1:]
            self.__chironomid_data  = self.__chironomid_data.dropna(axis = 1, how = 'all')
            self.__chironomid_names = self.__chironomid_data.columns[1:].values.tolist()
            self.__input_chironomid = pd.DataFrame(columns = ['measurementid','chironomid_taxa','chironomid_count'])
            for j in range(1,len(self.__chironomid_names)+1):
                for k in range(0,len(self.__chironomid_data )):
                    self.__chironomid_row = pd.DataFrame(np.array([[self.__chironomid_data.iloc[k,0],self.__chironomid_names[j-1],self.__chironomid_data.iloc[k,j]]]), columns = ['measurementid','chironomid_taxa','chironomid_count'])
                    self.__input_chironomid = self.__input_chironomid.append(self.__chironomid_row)
        except KeyError:
            if __suppress_message == False:
                print(f'No chironomid data for {__coreid}')
            else:
                pass
    
    def __pollen_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        __suppress_message = self.__suppress_message
        try:
            self.__pollen_data = __input_dictionary['Pollen']
            self.__pollen_data.columns = self.__pollen_data.iloc[6]
            self.__pollen_data = self.__pollen_data.drop(self.__pollen_data.index[6])
            self.__pollen_data = self.__pollen_data.iloc[6:, 1:]
            self.__pollen_data = self.__pollen_data.dropna(axis = 1, how = 'all')
            self.__pollen_names = self.__pollen_data.columns[1:].values.tolist()
            self.__input_pollen = pd.DataFrame(columns = ['measurementid','pollen_taxa','pollen_count'])
            for j in range(1,len(self.__pollen_names)+1):
                for k in range(0,len(self.__pollen_data)):
                    self.__pollen_row = pd.DataFrame(np.array([[self.__pollen_data.iloc[k,0],self.__pollen_names[j-1],self.__pollen_data.iloc[k,j]]]), columns = ['measurementid','pollen_taxa','pollen_count'])
                    self.__input_pollen = self.__input_pollen.append(self.__pollen_row)
        except KeyError:
            if __suppress_message == False:
                print(f'No pollen data for {__coreid}')
            else:
                pass
    
    def __age_input__(self):
        __input_dictionary = self.__input_dictionary
        __coreid = self.__coreid
        try:
            self.__input_age = __input_dictionary['Age']
            self.__input_age.columns = self.__input_age.iloc[6]
            self.__input_age = self.__input_age.drop(self.__input_age.index[6])
            ## age
            self.__input_age = self.__input_age.iloc[6:, 1:13]
            self.__input_age.rename(columns={'MeasurementID':'measurementid',
                                            'Thickness \n(cm)':'thickness',
                                            'Lab-ID':'labid',
                                            'Lab-Location':'lab_location',
                                            'Category':'material_category',
                                            'Material':'material_description', 
                                            'Weight \n(µg C)':'material_weight', 
                                            'Age \n(yr BP)':'age', 
                                            'Age Error \n(+/- yr)':'age_error', 
                                            'Pretreatment':'pretreatment_dating', 
                                            'Reservoir Age \n(yr)':'reservoir_age', 
                                            'Reservoir Error \n(+/- yr)':'reservoir_error'}, inplace=True)
            ### For detection limit
            self.__input_age.reset_index(drop = True, inplace = True)
            for i in range(0, len(self.__input_age)):
                if type(self.__input_age.iloc[i,7]) is str and '>' in self.__input_age.iloc[i,7]:
                    __age_array= self.__input_age.iloc[i,7].split('>')
                    __age_indi = NumericRange(int(__age_array[1]), None, bounds = '[)', empty = False)
                    self.__input_age.iloc[i,7] = __age_indi
                elif type(self.__input_age.iloc[i,7]) is str and '<' in self.__input_age.iloc[i,7]:
                    __age_array= self.__input_age.iloc[i,7].split('<')
                    __age_indi = NumericRange(None, int(__age_array[1]), bounds = '()', empty = False)
                    self.__input_age.iloc[i,7] = __age_indi
                else:
                    __age_indi = NumericRange(int(self.__input_age.iloc[i,7]), int(self.__input_age.iloc[i,7]), bounds = '[]', empty = False)
                    self.__input_age.iloc[i,7] = __age_indi
            ###
        except KeyError:
            raise Exception(f'No age data for {__coreid} - need age information to be included in MAYHEM database')

    
    def run_data_prep(self):
        self.__select_data__()
        self.__main_input__()
        self.__coreid__()
        self.__scientist_input__()
        self.__expedition_input__()
        self.__lake_input__()
        self.__drilling_input__()
        self.__source_input__()
        self.__publication_input__()
        self.__organic_input__()
        self.__grainsize_input__()
        self.__element_input__()
        self.__mineral_input__()
        self.__diatom_input__()
        self.__chironomid_input__()
        self.__pollen_input__()
        self.__age_input__()
