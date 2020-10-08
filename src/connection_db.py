#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Gregor Pfalz
github: GPawi
"""

import numpy as np
import pandas as pd
import sqlalchemy
import getpass
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker


class connection_db(object):
    def __init__(self, core, db = None, password = None, force_upload = False, upload_stop = False):
        self.__core = core
        self.__force_upload = force_upload
        self.__coreid = core._data_preparation__coreid
        self.__upload_stop = upload_stop
        if db is not None and password is not None:
            self.__db = db
            self.__password = password
        elif db is None and password is None:
            self.__db = input(f'What is the database name in which {self.__coreid} should be inserted? ')
            self.__password = getpass.getpass(prompt='What is the password for that database? ')
        elif db is not None and password is None:
            self.__db = db
            self.__password = getpass.getpass(prompt='What is the password for that database? ')
        else:
            self.__db = input(f'What is the database name in which {self.__coreid} should be inserted? ')
            self.__password = password
            
        self.__engine = sqlalchemy.create_engine(f'postgresql://postgres:{self.__password}@localhost/{self.__db}',
                                                 executemany_mode='batch')
        
    
    def __upload_scientist__(self, con = []):
        __core = self.__core
        self.__scientist = __core._data_preparation__scientist
        self.__scientist_columns = __core._data_preparation__scientist_columns
        try:
            self.__scientist_duplicate_check = pd.merge(self.__scientist, pd.read_sql('scientist', con), how ='inner', on = self.__scientist_columns)
            if any(self.__scientist_duplicate_check.columns == 'scientistid') and (len(self.__scientist_duplicate_check) == len(self.__scientist)):
                self.__scientist = self.__scientist_duplicate_check
                print ('Scientist(s) already exist!')
            elif any(self.__scientist_duplicate_check.columns == 'scientistid') and (len(self.__scientist_duplicate_check) != len(self.__scientist)) and (len(self.__scientist_duplicate_check)>0):
                self.__new_scientist = self.__scientist[~self.__scientist.isin(self.__scientist_duplicate_check)].dropna()
                self.__new_scientist.to_sql('scientist', con, if_exists='append', index = False)
                self.__scientist_duplicate_check = pd.merge(self.__scientist, pd.read_sql('scientist', con), how ='inner', on = self.__scientist_columns)
                self.__scientist = self.__scientist_duplicate_check
                print ('Added new scientist(s)!')
            else:
                self.__scientist.to_sql('scientist', con, if_exists='append', index = False)
                self.__scientist_duplicate_check = pd.merge(self.__scientist, pd.read_sql('scientist', con), how ='inner', on = self.__scientist_columns)
                self.__scientist = self.__scientist_duplicate_check
                print ('All scientist(s) added!')
        except:
            print ('There was an issue - Please report to Gregor Pfalz (Gregor.Pfalz@awi.de)!')
                        
    def __upload_expedition__(self, con = []):
        __core = self.__core
        __scientist = self.__scientist
        self.__scientistid = __scientist['scientistid']
        self.__expedition = __core._data_preparation__expedition
        self.__expedition_columns = __core._data_preparation__expedition_columns
        try:
            self.__expedition_duplicate_check = pd.merge(self.__expedition, pd.read_sql('expedition', con), how ='inner', on = self.__expedition_columns)
            if len(self.__expedition_duplicate_check) > 0:
                self.__expedition = self.__expedition_duplicate_check
                print ('Expedition already exists!')
            else:
                self.__expedition['scientistid'] = self.__scientistid
                self.__expedition.to_sql('expedition', con, if_exists='append', index = False)
                print ('New expedition added!')
        except:
            print ('There was an issue - Please report to Gregor Pfalz (Gregor.Pfalz@awi.de)!')
                        
    def __upload_lake__(self, con = []):
        __core = self.__core
        self.__lake = __core._data_preparation__lake
        self.__lake_columns = __core._data_preparation__lake_columns
        try:
            self.__lake_duplicate_check = pd.merge(self.__lake, pd.read_sql('lake', con), how ='inner', on = self.__lake_columns)
            if len(self.__lake_duplicate_check) > 0:
                self.__lake = self.__lake_duplicate_check
                print ('Lake already exists!')
            else:
                self.__lake.to_sql('lake', con, if_exists='append', index = False)
                print ('New lake added!')
        except:
            print ('There was an issue - Please report to Gregor Pfalz (Gregor.Pfalz@awi.de)!')
    
    def __upload_drilling__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        __force_upload = self.__force_upload
        self.__drilling = __core._data_preparation__drilling
        self.__drilling_columns = __core._data_preparation__drilling_columns
        try:
            self.__drilling_duplicate_check = pd.merge(self.__drilling, pd.read_sql('drilling', con), how ='inner', on = self.__drilling_columns)
            if len(self.__drilling_duplicate_check) > 0:
                self.__drilling = self.__drilling_duplicate_check
                if __force_upload == False:
                    while True:
                        self.__upload_query = input(f'Core {__coreid} already exist - Do you still want to continue uploading the data? Y/N? ')
                        self.__upload_answer = self.__upload_query[0].lower() 
                        if self.__upload_query == '' or not self.__upload_answer in ['y','n']:
                            print('Please answer with yes or no!') 
                        else:
                            break
                    if self.__upload_answer == 'y':
                        self.__upload_stop = False
                        print ('Ok!')
                    if self.__upload_answer == 'n':
                        raise Exception('Manually stopped upload process.')
                else:
                    self.__upload_stop = False
                    print (f'Core {__coreid} already exists!')
                
            else:
                self.__drilling.to_sql('drilling', con, if_exists='append', index = False)
                self.__upload_stop = False
                print ('New core information added!')
        
        except Exception:
            self.__upload_stop = True
            print ('Manually stopped upload process.')
        
        except:
            print ('There was an issue - Please report to Gregor Pfalz (Gregor.Pfalz@awi.de)!')
    
    def __upload_participant__(self, con = []):
        self.__participant = self.__scientist
        __coreid = self.__coreid
        self.__participant['coreid'] = __coreid
        self.__participant = self.__participant[['scientistid','coreid']]
        try:
            self.__participant_duplicate_check = pd.merge(self.__participant, pd.read_sql('participant', con), how ='inner', on = ['scientistid','coreid'])
            if (len(self.__participant_duplicate_check) == len(self.__participant)):
                self.__participant = self.__participant_duplicate_check
                print ('Participant(s) already registered!')
            elif (len(self.__participant_duplicate_check) != len(self.__participant)):
                self.__new_participant = self.__participant[~self.__participant.isin(self.__participant_duplicate_check)].dropna()
                self.__new_participant.to_sql('participant', con, if_exists='append', index = False)
                self.__participant_duplicate_check = pd.merge(self.__participant, pd.read_sql('participant', con), how ='inner', on = ['scientistid','coreid'])
                self.__participant = self.__participant_duplicate_check
                print (f'Added new participant(s) to {__coreid}!')
            else:
                self.__participant.to_sql('participant', con, if_exists='append', index = False)
                self.__participant_duplicate_check = pd.merge(self.__participant, pd.read_sql('participant', con), how ='inner', on = ['scientistid','coreid'])
                self.__participant = self.__participant_duplicate_check
                print (f'Participant(s) added for {__coreid}!')
        except:
            print ('There was an issue - Please report to Gregor Pfalz (Gregor.Pfalz@awi.de)!')
    
    def __upload_organic__(self, con = []): 
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_organic  = __core._data_preparation__input_organic
            self.__measurementids_organic = __core._data_preparation__input_organic.copy()
            self.__measurementids_organic[['coreid','compositedepth']] = self.__measurementids_organic['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_organic = self.__measurementids_organic[['measurementid','coreid','compositedepth']]
            self.__measurementids_organic['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            try:
                self.__measurement_organic_duplicate_check = pd.merge(self.__measurementids_organic, pd.read_sql('measurement', con), how ='inner', on = ['measurementid', 'coreid'])
                self.__measurement_organic_duplicate_check = self.__measurement_organic_duplicate_check.drop(columns = 'compositedepth_y')
                self.__measurement_organic_duplicate_check = self.__measurement_organic_duplicate_check.rename(columns = {'compositedepth_x':'compositedepth'})
                self.__measurementids_organic = self.__measurementids_organic.append(self.__measurement_organic_duplicate_check).drop_duplicates(keep=False)
                self.__measurementids_organic.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for organic proxy')
            finally:
                self.__input_organic.to_sql('organic', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for organic proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that organic data were already uploaded.')
    
    def __upload_grainsize__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_grainsize  = __core._data_preparation__input_grainsize
            self.__measurementids_grainsize = __core._data_preparation__input_grainsize.copy()
            self.__measurementids_grainsize[['coreid','compositedepth']] = self.__measurementids_grainsize['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_grainsize = self.__measurementids_grainsize[['measurementid','coreid','compositedepth']]
            self.__measurementids_grainsize['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            try:
                self.__measurement_grainsize_duplicate_check = pd.merge(self.__measurementids_grainsize, pd.read_sql('measurement', con), how ='inner', on = ['measurementid', 'coreid'])
                self.__measurement_grainsize_duplicate_check = self.__measurement_grainsize_duplicate_check.drop(columns = 'compositedepth_y')
                self.__measurement_grainsize_duplicate_check = self.__measurement_grainsize_duplicate_check.rename(columns = {'compositedepth_x':'compositedepth'})
                self.__measurementids_grainsize = self.__measurementids_grainsize.append(self.__measurement_grainsize_duplicate_check).drop_duplicates(keep=False)
                self.__measurementids_grainsize.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for grain size proxy')
            finally:
                self.__input_grainsize.to_sql('grainsize', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for grain size proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that grain size data were already uploaded.')        
    
    def __upload_element__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_element  = __core._data_preparation__input_element
            self.__measurementids_element = __core._data_preparation__input_element.copy()
            self.__measurementids_element[['coreid','compositedepth']] = self.__measurementids_element['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_element = self.__measurementids_element[['measurementid','coreid','compositedepth']]
            self.__measurementids_element['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_element = self.__measurementids_element.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_ele = pd.read_sql('measurement', con)
                self.__down_measurement_ele = self.__down_measurement_ele.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_element_duplicate_check = pd.concat([self.__measurementids_element.reset_index(drop=True),self.__down_measurement_ele.reset_index(drop=True)])
                self.__measurement_element_duplicates = self.__measurement_element_duplicate_check[self.__measurement_element_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_element_duplicate_free = self.__measurementids_element.append(self.__measurement_element_duplicates)
                self.__measurement_element_duplicate_free = self.__measurement_element_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_element_duplicate_free.to_sql('measurement', con, if_exists='append', index = False) #, method = 'multi'), chunksize = 1000)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for element proxy')
            finally:
                self.__input_element.to_sql('element', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for element proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that element data were already uploaded.')
            
    
    def __upload_mineral__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_mineral  = __core._data_preparation__input_mineral
            self.__measurementids_mineral = __core._data_preparation__input_mineral.copy()
            self.__measurementids_mineral[['coreid','compositedepth']] = self.__measurementids_mineral['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_mineral = self.__measurementids_mineral[['measurementid','coreid','compositedepth']]
            self.__measurementids_mineral['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_mineral = self.__measurementids_mineral.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_mineral = pd.read_sql('measurement', con)
                self.__down_measurement_mineral = self.__down_measurement_mineral.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_mineral_duplicate_check = pd.concat([self.__measurementids_mineral.reset_index(drop=True),self.__down_measurement_mineral.reset_index(drop=True)])
                self.__measurement_mineral_duplicates = self.__measurement_mineral_duplicate_check[self.__measurement_mineral_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_mineral_duplicate_free = self.__measurementids_mineral.append(self.__measurement_mineral_duplicates)
                self.__measurement_mineral_duplicate_free = self.__measurement_mineral_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_mineral_duplicate_free.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for mineral proxy')
            finally:
                self.__input_mineral.to_sql('mineral', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for mineral proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that mineral data were already uploaded.')

    
    def __upload_diatom__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_diatom  = __core._data_preparation__input_diatom
            self.__measurementids_diatom = __core._data_preparation__input_diatom.copy()
            self.__measurementids_diatom[['coreid','compositedepth']] = self.__measurementids_diatom['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_diatom = self.__measurementids_diatom[['measurementid','coreid','compositedepth']]
            self.__measurementids_diatom['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_diatom = self.__measurementids_diatom.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_diatom = pd.read_sql('measurement', con)
                self.__down_measurement_diatom = self.__down_measurement_diatom.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_diatom_duplicate_check = pd.concat([self.__measurementids_diatom.reset_index(drop=True), self.__down_measurement_diatom.reset_index(drop=True)], ignore_index = True)
                self.__measurement_diatom_duplicates = self.__measurement_diatom_duplicate_check[self.__measurement_diatom_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_diatom_duplicate_free = self.__measurementids_diatom.append(self.__measurement_diatom_duplicates)
                self.__measurement_diatom_duplicate_free = self.__measurement_diatom_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_diatom_duplicate_free.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for diatom proxy')
            finally:
                self.__input_diatom.to_sql('diatom', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for diatom proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that diatom data were already uploaded.')

    
    def __upload_chironomid__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_chironomid  = __core._data_preparation__input_chironomid
            self.__measurementids_chironomid = __core._data_preparation__input_chironomid.copy()
            self.__measurementids_chironomid[['coreid','compositedepth']] = self.__measurementids_chironomid['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_chironomid = self.__measurementids_chironomid[['measurementid','coreid','compositedepth']]
            self.__measurementids_chironomid['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_chironomid = self.__measurementids_chironomid.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_chironomid = pd.read_sql('measurement', con)
                self.__down_measurement_chironomid = self.__down_measurement_chironomid.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_chironomid_duplicate_check = pd.concat([self.__measurementids_chironomid.reset_index(drop=True),self.__down_measurement_chironomid.reset_index(drop=True)])
                self.__measurement_chironomid_duplicates = self.__measurement_chironomid_duplicate_check[self.__measurement_chironomid_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_chironomid_duplicate_free = self.__measurementids_chironomid.append(self.__measurement_chironomid_duplicates)
                self.__measurement_chironomid_duplicate_free = self.__measurement_chironomid_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_chironomid_duplicate_free.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for chironomid proxy')
            finally:
                self.__input_chironomid.to_sql('chironomid', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for chironomid proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that chironomid data were already uploaded.')

    
    def __upload_pollen__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_pollen  = __core._data_preparation__input_pollen
            self.__measurementids_pollen = __core._data_preparation__input_pollen.copy()
            self.__measurementids_pollen[['coreid','compositedepth']] = self.__measurementids_pollen['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_pollen = self.__measurementids_pollen[['measurementid','coreid','compositedepth']]
            self.__measurementids_pollen['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_pollen = self.__measurementids_pollen.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_pollen = pd.read_sql('measurement', con)
                self.__down_measurement_pollen = self.__down_measurement_pollen.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_pollen_duplicate_check = pd.concat([self.__measurementids_pollen.reset_index(drop=True),self.__down_measurement_pollen.reset_index(drop=True)])
                self.__measurement_pollen_duplicates = self.__measurement_pollen_duplicate_check[self.__measurement_pollen_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_pollen_duplicate_free = self.__measurementids_pollen.append(self.__measurement_pollen_duplicates)
                self.__measurement_pollen_duplicate_free = self.__measurement_pollen_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_pollen_duplicate_free.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for pollen proxy')
            finally:
                self.__input_pollen.to_sql('pollen', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for pollen proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that pollen data were already uploaded.')

    
    def __upload_age__(self, con = []):
        __core = self.__core
        __coreid = self.__coreid
        try:
            self.__input_agedetermination  = __core._data_preparation__input_age
            self.__measurementids_agedetermination = __core._data_preparation__input_age.copy()
            self.__measurementids_agedetermination[['coreid','compositedepth']] = self.__measurementids_agedetermination['measurementid'].str.split(' ', n = 1, expand = True)
            self.__measurementids_agedetermination = self.__measurementids_agedetermination[['measurementid','coreid','compositedepth']]
            self.__measurementids_agedetermination['compositedepth'].replace(regex=True,inplace=True,to_replace=(r'_duplicate'+r'\d'),value=r'')
            self.__measurementids_agedetermination = self.__measurementids_agedetermination.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float}).drop_duplicates()
            try:
                self.__down_measurement_agedetermination = pd.read_sql('measurement', con)
                self.__down_measurement_agedetermination = self.__down_measurement_agedetermination.astype(dtype = {'measurementid':str, 'coreid': str, 'compositedepth': float})
                self.__measurement_agedetermination_duplicate_check = pd.concat([self.__measurementids_agedetermination.reset_index(drop=True),self.__down_measurement_agedetermination.reset_index(drop=True)])
                self.__measurement_agedetermination_duplicates = self.__measurement_agedetermination_duplicate_check[self.__measurement_agedetermination_duplicate_check.duplicated() == True].reset_index(drop=True)
                self.__measurement_agedetermination_duplicate_free = self.__measurementids_agedetermination.append(self.__measurement_agedetermination_duplicates)
                self.__measurement_agedetermination_duplicate_free = self.__measurement_agedetermination_duplicate_free.drop_duplicates(keep = False)
                self.__measurement_agedetermination_duplicate_free.to_sql('measurement', con, if_exists='append', index = False)
            except IntegrityError:
                raise Exception(f'There is a problem with core {__coreid} for age determination proxy')
            finally:
                self.__input_agedetermination.to_sql('agedetermination', con, if_exists='append', index = False)
                print (f'I am done with core {__coreid} for age determination proxy')
        except IntegrityError:   
            print (f'I had an integrity error for {__coreid} - It seemes that age determination data were already uploaded.')
            
    
    def run_data_upload(self):
        engine = self.__engine
        Session = sessionmaker(bind = engine)
        self.__method_list = {1 : self.__upload_scientist__,
                              2 : self.__upload_expedition__,
                              3 : self.__upload_lake__,
                              4 : self.__upload_drilling__,
                              5 : self.__upload_participant__,
                              6 : self.__upload_organic__,
                              7 : self.__upload_grainsize__,
                              8 : self.__upload_element__,
                              9 : self.__upload_mineral__,
                              10 : self.__upload_diatom__,
                              11 : self.__upload_chironomid__,
                              12 : self.__upload_pollen__,
                              13 : self.__upload_age__}
        
        for method in range(1,14):
            session = Session()
            con = session.get_bind()
            if self.__upload_stop == False:
                try: 
                    self.__method_list[method](con)
                    session.commit()
                except:
                    session.rollback()
                finally:
                    session.close()
            else:
                session.close()
