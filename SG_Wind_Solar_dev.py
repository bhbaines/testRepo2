#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
from sqlalchemy import create_engine  
import arcpy
import os
import datetime
import arcgis
from arcgis import *

# User Parameters
prj_num     = 1124#4064
#prj_layout  = 101
debug = True
email_requestor = True
turbine_source = "Apex_Layouts" #"Techdash"


# In[2]:


# Other Script constants
engine = create_engine('mssql+pyodbc://@ace-ra-sql1/Apex_ProjectData?driver=ODBC+Driver+11+for+SQL+Server')  
srid_out = 3857
aprx_file = r"\\ace-ra-fs1\data\GIS\_Dev\python\_working\Stagegate_dev\Stagegate_Automation_devCopy_new_4.aprx"
aprx = arcpy.mp.ArcGISProject(aprx_file)


sde_cnx_dir = r'\\ace-ra-fs1\data\GIS\_Dev\software\esri\_Latest\SDE_Cnxns'
working_gdb = os.path.join(sde_cnx_dir, 'Apex_ProjectData.sde')

        



# In[3]:


#Hardcoded replacement for row read from SQL requests table
request_row ={'ProjectCode' : prj_num,
          'Layout' : 'NoLyt',
          'WindSolar' : 'Solar'}


# # Preprocessing and Configuration

# ## Solar/Wind Conditional Variable

# Fucntionalize below

# In[4]:


#project_type='Solar'


# In[5]:


project_type='Wind'


# In[6]:


Resource = '{} Resource'.format(project_type)


# # Map Configuration

# In[7]:




##Change universal project layers to be solar relavent
##project_layers = [ 'Apex MET Tower', 'Turbine', 'Interconnection', 'Gentie Line', 'Linear Facilities', 'Project Boundary']
##^^project_layers generalized below to allow for solar or wind conditional.  If wind, include MET towers and Turbines.
project_layers = ['Interconnection', 'Point of Interconnect', 'Project Substation', 'Gentie Line', 'Linear Facilities', 
                  'Project Boundary', 'World Topographic Map']





# ## Solar-specific Configuration

# In[8]:


if project_type == 'Solar':
     
    template_params =   {'BizDev'         : '' 
                        , Resource        : '' 
                        ,'Project Overview' : ''
                        ,'Lease Status'   : '' 
                        ,'Title Status'   : ''
                        ,'Constraints'    : '' 
                        ,'TROW'           : '' 
                        ,'Slope Aspect'   : ''
                        
                        } 
    
    layerViz_params =   {'BizDev'     : ['Project Location', 'Oceans', 'State', 'Major Highways', 'Cities', 'Interstate'] 
                         , Resource        : ['State Boundaries', 'Project', 'State Boundaries', 'Cities', 
                                         'Solar Resource GHI (kWh/m2)'] 
                        
                         ,'Project Overview' : ['Interconnection', 'Point of Interconnect', 'Project Substation', 'Gentie Line', 
                                               'Project Boundary', 'World Topographic Map']                        
                                          
                         ,'Lease Status'   : project_layers +
                                         [ 'Ekho', 'Parcels', 'Signed Lease Agreements', 'Signed Solar Agreements'
                                          ,'Signed Other Agreements', 'Unsigned Agreements', 'Agreement Type'
                                          ,'Agreement Status']  
                        
                         ,'Title Status'   : project_layers + [ 'Ekho', 'Parcels', 'Title Status']
                        
                         ,'Constraints'    : project_layers + ['Buildable Area', 'Construction Feature','Transmission',
                                                              'Transmission (Ventyx)', 'Existing Transmission', 
                                                              'Existing Transmission (Low Voltage)', 
                                                              'Existing Transmission (High Voltage)', 'Setback Polygons']
                         ,'TROW'           : project_layers + 
                                         [# 'Transportation', 'Roads', 'Railroads',
                                             'Ekho', 'Signed Lease Agreements', 'Unsigned Agreements', 'Agreement Type'
                                          ,'Signed Solar Agreements', 'Signed Other Agreements']
                         
                         ,'Slope Aspect'   : project_layers + ['Panel Racks', 'Construction Feature', 'Slope/Aspect Suitability']
                        } 
    
    scales_params =     {'BizDev'         : 2800000
                        , Resource        : '' 
                        ,'Project Overview' : ''
                        ,'Lease Status'   : '' 
                        ,'Title Status'   : ''
                        ,'Constraints'    : '' 
                        ,'TROW'           : '' 
                        ,'Slope Aspect'   : ''
                        } 
               
              
    layout_params =     {'BizDev'         : 'BizDev' 
                        , Resource        : '_Main_Map'
                        ,'Project Overview' : '_Main_Map'
                        ,'Lease Status'   : '_Main_Map' 
                        ,'Title Status'   : '_Main_Map'
                        ,'Constraints'    : '_Main_Map' 
                        ,'TROW'           : '_Main_Map' 
                        ,'Slope Aspect'   : '_Main_Map'
                        }


    # Configure settings for where the extent is derived
    extent_params =     {'BizDev'         : 'state' 
                        , Resource        : 'state' 
                        ,'Project Overview' : 'project'
                        ,'Lease Status'   : 'project' 
                        ,'Title Status'   : 'project'
                        ,'Constraints'    : 'project' 
                        ,'TROW'           : 'gentie_line'
                        ,'Slope Aspect'   : 'project'
                        }

    # Configure settings for the name of the mapframe to set
    mapframeScale_params =      {'BizDev'         : 'Overview Map Frame' 
                                , Resource        : 'Layers Map Frame' 
                                ,'Project Overview' : 'Layers Map Frame'
                                ,'Lease Status'   : 'Layers Map Frame' 
                                ,'Title Status'   : 'Layers Map Frame'
                                ,'Constraints'    : 'Layers Map Frame' 
                                ,'TROW'           : 'Layers Map Frame'
                                ,'Slope Aspect'   : 'Layers Map Frame'
                                }

    # Configure settings for the factor that is used to adjust the scale
    scaleAdj_params =       {'BizDev'         : 1.25
                            , Resource        : 1.15
                            ,'Project Overview' : 1.25
                            ,'Lease Status'   : 1.25
                            ,'Title Status'   : 1.25
                            ,'Constraints'    : 1.25
                            ,'TROW'           : 1.5 
                            ,'Slope Aspect'   : 1.25
                            }

    # Merge all parameter dictionaries into single dataframe
    param_dict_cols = { 'scale_param'  : scales_params
                       ,'layout_param' : layout_params
                       ,'layer_viz'    : layerViz_params
                       ,'extent_obj'   : extent_params
                       ,'mapframeScalesToChange' : mapframeScale_params
                       ,'Scale_Adj'    : scaleAdj_params
                      }


# ## Wind-specific Configuration

# In[9]:


if project_type == 'Wind':
    
    template_params =   {'BizDev'         : '' 
                        , Resource        : '' 
                        ,'Lease Status'   : '' 
                        ,'Title Status'   : ''
                        ,'Constraints'    : '' 
                        ,'TROW'           : '' 
                        } 
    
    layerViz_params =   {'BizDev'     : ['Project Location', 'Oceans', 'State', 'Major Highways', 'Cities', 'Interstate'] 
                        , Resource        : project_layers + ['Apex MET Tower', 'Turbine', 'Wind Speed', 
                                                         'AWS Mean Wind Speed (100m)'] 
                        ,'Lease Status'   : project_layers +
                                         [ 'Ekho', 'Parcels', 'Signed Lease Agreements', 'Signed Solar Agreements'
                                          ,'Signed Other Agreements', 'Unsigned Agreements', 'Agreement Type'
                                          ,'Agreement Status']  
                        ,'Title Status'   : project_layers + [ 'Ekho', 'Parcels', 'Title Status']
                        ,'Constraints'    : project_layers + ['Buildable Area']
                        ,'TROW'           : project_layers + 
                                         [# 'Transportation', 'Roads', 'Railroads',
                                             'Ekho', 'Signed Lease Agreements', 'Unsigned Agreements', 'Agreement Type'
                                          ,'Signed Solar Agreements', 'Signed Other Agreements']
                        } 
    
    scales_params =     {'BizDev'         : 2800000
                        , Resource        : '' 
                        ,'Lease Status'   : '' 
                        ,'Title Status'   : ''
                        ,'Constraints'    : '' 
                        ,'TROW'           : '' 
                        } 
               
              
    layout_params =     {'BizDev'         : 'BizDev' 
                        , Resource        : '_Main_Map' 
                        ,'Lease Status'   : '_Main_Map' 
                        ,'Title Status'   : '_Main_Map'
                        ,'Constraints'    : '_Main_Map' 
                        ,'TROW'           : '_Main_Map' 
                        }


    # Configure settings for where the extent is derived
    extent_params =     {'BizDev'         : 'state' 
                        , Resource        : 'project' 
                        ,'Lease Status'   : 'project' 
                        ,'Title Status'   : 'project'
                        ,'Constraints'    : 'project' 
                        ,'TROW'           : 'gentie_line' 
                        }

    # Configure settings for the name of the mapframe to set
    mapframeScale_params =      {'BizDev'         : 'Overview Map Frame' 
                                , Resource        : 'Layers Map Frame' 
                                ,'Lease Status'   : 'Layers Map Frame' 
                                ,'Title Status'   : 'Layers Map Frame'
                                ,'Constraints'    : 'Layers Map Frame' 
                                ,'TROW'           : 'Layers Map Frame' 
                                }

    # Configure settings for the factor that is used to adjust the scale
    scaleAdj_params =       {'BizDev'         : 1.15
                            , Resource        : 1.15
                            ,'Lease Status'   : 1.15
                            ,'Title Status'   : 1.15
                            ,'Constraints'    : 1.15
                            ,'TROW'           : 1.5 
                            }

    # Merge all parameter dictionaries into single dataframe
    param_dict_cols = { 'scale_param'  : scales_params
                       ,'layout_param' : layout_params
                       ,'layer_viz'    : layerViz_params
                       ,'extent_obj'   : extent_params
                       ,'mapframeScalesToChange' : mapframeScale_params
                       ,'Scale_Adj'    : scaleAdj_params
                      }


# In[10]:


# Merge raw configuration dictionaries into single dataframe
def Merge_Dicts(param_dict_cols):
    dfs = []
    first = True
    for col_name, param_dict in param_dict_cols.items():
        if first:
            first = False
            df = pd.DataFrame(param_dict.values(), columns = [ col_name], index = param_dict.keys())
            continue
        
        df[col_name] = None
        for name, val in param_dict.items():
            df.at[name, col_name] = val

    #df  = pd.concat(dfs, axis=1)
    df['map'] = df.index
    
    return df

params_df_raw = Merge_Dicts(param_dict_cols)
params_df_raw['layout_param'].unique()

 


# In[11]:


# Get layout objects
def Match_Layouts(aprx, params_df):    
    layout_aprx_dict = {}
    params_df['layout_aprx'] = None
    
    for name in params_df['layout_param'].unique():
        layout_aprx_dict[name] = aprx.listLayouts(name)[0]
        
    for map_name, row in params_df.iterrows():
        layout = row['layout_param']
        params_df.loc[map_name, 'layout_aprx'] = layout_aprx_dict[layout]
        
        
    #return layout_aprx_dict

Match_Layouts(aprx, params_df_raw)
params_df_raw   


# In[12]:


# # # Automated check for any requested projects Tool_Requests
def Get_Requests():
    # Create SQL Engine
    engine = create_engine('mssql+pyodbc://@ace-ra-sql1/Apex_ProjectData?driver=ODBC+Driver+11+for+SQL+Server')
    
    dupe_fields = "[Project], [ProjectCode], [PRJ], [Tool_Requested], [Complete], [Layout], [WindSolar]"
    
    Clear_Dupes_SQL(engine, dupe_fields, "Tool_Requests", 'last_edited_date')
    
    # Define SQL command as a python string
    requests_sql_tbl = "Tool_Requests"
    sql_cmd = """SELECT Project, PRJ, ProjectCode, WindSolar, Layout, created_user, created_date 
                    from """ + requests_sql_tbl + " WHERE Tool_Requested = 'StageGate' AND Complete = 0" 

    # Execute command to return list of unprocessed requests
    with engine.begin() as connection:
        result = connection.execute(sql_cmd).fetchall()

    #print(input_list)
    
    input_list = [dict(row) for row in result] 
    
    for row in input_list:   
        if row['Layout'] is None:
            row['Layout'] = 'NoLyt'
        
    print(len(input_list), "need to be completed")
    
    return input_list


# In[13]:


# # # Function clears out duplicates from request table

def Clear_Dupes_SQL(engine, fields_str, sql_tbl, sort_field):
    fields = fields_str + ", RN = ROW_NUMBER()OVER(PARTITION BY " + fields_str + """
                                        ORDER BY """ + sort_field + " desc)"
    sql_cmd = """  WITH CTE AS (
                   SELECT """ + fields + "," + sort_field +  """                       
                   FROM """ + sql_tbl + """
                )
                DELETE FROM CTE WHERE RN > 1"""
    #print(sql_cmd)
    
    with engine.begin() as connection:
        result = connection.execute(sql_cmd)


# # Gather Project info

# In[14]:


# Get Turbine info
def Get_Turbine_Info(prj_num, prj_layout):
    sql_cmd = """SELECT [capacity_mw], [wtg_model], [hub_height_m] FROM [GIS_TechDash].[dbo].[ENERGY_DATA_MASTER]
                WHERE [project_id] = """ + str(prj_num) + "AND [layout_id] like '%" + str(prj_layout) + "'"
    with engine.begin() as connection:
        prj_cap, prj_wtgModel, prj_hh = connection.execute(sql_cmd).fetchall()[0]
            

    return prj_cap, prj_wtgModel, prj_hh


# In[15]:



def Gather_Project_info(request_row):
    prj_num    = request_row['ProjectCode']
    prj_layout = request_row['Layout']
    
    
    sql_cmd = "SELECT Project, PRJ, County, ST_Abbrv from Apex_Project_Boundary WHERE PROJECTCODE = " + str(prj_num) 
    with engine.begin() as connection:
            prj_name, prj_alpha, prj_co, prj_st = connection.execute(sql_cmd).fetchall()[0]
            
    prj_cap = prj_hh = 0   
    prj_wtgModel = ''
    if request_row['WindSolar'] == 'Wind':
        
        if prj_layout == 'NoLyt':
            prj_wtgModel  = ''
            prj_cap = prj_hh = 0
        else:
            prj_cap, prj_wtgModel, prj_hh = Get_Turbine_Info(prj_num, prj_layout)
    


    prj_info = {
         'prj_num'     : str(prj_num)
        ,'prj_layout'  : str(prj_layout)
        ,'prj_name'    : prj_name
        ,'prj_alpha'   : prj_alpha
        ,'prj_co'      : prj_co
        ,'prj_st'      : prj_st
        ,'prj_cap'     : str(float(prj_cap))
        ,'prj_wtgModel': prj_wtgModel
        ,'prj_hh'      : str(prj_hh)
    }

    display(prj_info)
    
    return prj_info



# In[16]:


#Most likely not used for solar
# Return matching layout from BA or most recent if not found
def Get_BA_Layout(prj_info):
    sql_cmd = """SELECT        [OBJECTID]
                              ,[Project]
                              ,[ProjectCode]
                              ,[Layout]
                              ,[Author]
                              ,[RunDate]
                              ,[Notes]
                              ,[sb_WTG]
                              ,[created_user]
                              ,[created_date]
                              ,[last_edited_user]
                              ,[last_edited_date]
                              ,[LandStatus]
                            FROM [Apex_ProjectData].[dbo].[APEX_BUILDABLEAREAS]
                            WHERE ProjectCode = """ + str(prj_num) + """
                                ORDER BY Layout desc"""
    df = pd.read_sql_query(sql_cmd, engine)
    
    display(df)
    
    ba_lyt = max(df['Layout'].unique())
    
    ba_defQ = "ProjectCode = '" + str(df['ProjectCode'][0]) + "' AND LAYOUT = '" + str(ba_lyt) + "'"
    
    if ba_lyt is None:
        ba_defQ = "ProjectCode = '" + str(df['ProjectCode'][0]) + "' AND LAYOUT IS NULL"
    
        
    prj_info['ba_defQ'] = ba_defQ
    #print(ba_lyt)
    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Preprocessing, Project-specific

# In[17]:


def Get_DefQs(prj_info):
    # Definition queries for layers
    defQ_prj_num   = "Project_Code = " + prj_info['prj_num']
    defQ_prjnum    = "ProjectCode = '" + prj_info['prj_num'] + "'"
    defQ_prjnum_int= "ProjectCode =  " + prj_info['prj_num'] 
    if prj_info['prj_num'] == '1065':
        defQ_prj_num = "Project_Code in (" + prj_info['prj_num'] + ", 1004)"
        defQ_prjnum = "ProjectCode in (" + prj_info['prj_num'] + ", 1004)"
        defQ_prjnum_int = defQ_prjnum

    
    if prj_info['prj_layout'] is None or prj_info['prj_layout'] == 'NoLyt':
        defQ_Layout_num = "Layout_Number is NULL"
        defQ_Layout     = "Layout is NULL" 
    else:        
        defQ_Layout_num = "Layout_Number like '%" + prj_info['prj_layout'] + "'"
        defQ_Layout     = "Layout like '%"        + prj_info['prj_layout'] + "'"

    defQs = {
         'prj_num'     : defQ_prj_num
        ,'prjnum'      : defQ_prjnum
        ,'prjnum_int'  : defQ_prjnum_int
        ,'layout_num'  : defQ_Layout_num
        ,'layout'      : defQ_Layout

        ,'Project Location': defQ_prjnum_int
        ,'Apex MET Tower'  : "APEXID = " + prj_info['prj_num']
        ,'Turbine'         : defQ_prjnum_int #+ " AND " + defQ_Layout
        ,'Interconnection' : defQ_prjnum_int
        ,'Gentie Line'     : defQ_prjnum_int
        ,'Linear Facilities' : defQ_prjnum_int #+ " AND " + defQ_Layout
        ,'Buildable Area'    : defQ_prjnum_int#prj_info['ba_defQ']
        ,'Project Boundary'  : """Type = 'Primary' And Status IN ('Active', 'Built and Operating', 'Operating', 'Under Contract') 
                                    AND """ + defQ_prjnum
        ,'Lease Restrictions' : ""
        ,'Competitor Leases'  : ""
        ,'Parcels'            : defQ_prj_num 
        ,'Signed Lease Agreements' : defQ_prj_num + """ AND Agreement_Status IN ('Agreement Signed', 'Agreement Signed - Amended') 
                                                        And Agreement_Type IN ('Lease', 'Lease Option')"""

        ,'Signed Solar Agreements' : defQ_prj_num + """ AND Agreement_Status IN ('Agreement Signed', 'Agreement Signed - Amended') 
                                                        And Agreement_Type IN ('Solar Lease', 'Solar Lease Option')"""

        ,'Signed Other Agreements' : defQ_prj_num + """ AND Agreement_Status IN ( 'Agreement Signed' , 'Agreement Signed - Amended' ) 
                                                        AND ( NOT ( Agreement_Type IN 
                                                                ('Lease' , 'Lease Option' , 'Solar Lease' , 'Solar Lease Option')) 
                                                            OR Agreement_Type IS NULL )"""

        ,'Agreement Type' : defQ_prj_num + """ AND NOT( Agreement_Status IN ( 'Agreement Signed' , 'Agreement Signed - Amended' ) ) 
                                               AND Agreement_Type IN ( 'Good Neighbor' , 'Setback Waiver' , 'TROW' 
                                                   , 'TROW Option' , 'UCE', 'UCE Option', 'OCE' , 'OCE Option' )"""

        ,'Agreement Status' : defQ_prj_num + """ AND Agreement_Status IN ('Agreement Signed', 'Agreement Signed - Amended') 
                                                 And Agreement_Type IN ('Lease', 'Lease Option')"""

        ,'Contact Status'   : defQ_prj_num + """ AND ( NOT( Agreement_Status IN 
                                                    ( 'Agreement Signed' , 'Agreement Signed - Amended' ) ) 
                                              OR Agreement_Status IS NULL) AND Contact_Status IN 
                                                  ( 'Contacted - Negative' , 'Contacted - Neutral' 
                                                  , 'Contacted - No' , 'Contacted - Positive' )"""
        ,'Title Status'     : defQ_prj_num + """ AND Title_Policy_Parcel = 1 
                                                And Title_Commitment_Status <> 'No Title Status' 
                                                And Title_Commitment_Status IS NOT NULL"""
        ,'County' :  "COUNTY = '" + prj_info['prj_co'] + "' And ST_ABBR = '" + prj_info['prj_st'] + "'"
        ,'State'  :  "ABBREV = '" + prj_info['prj_st'] + "'" 
        ,'cursor' : defQ_prjnum + " AND Complete in (0, 9) AND Tool_Requested = 'StageGate' AND " + defQ_Layout
        #added for Solar Maps:
        ,'Project' : """Type = 'Primary' And Status IN ('Active', 'Built and Operating', 'Operating', 'Under Contract') 
                                    AND """ + defQ_prjnum
        #,'Construction Feature' : defQ_prjnum_int
        ,'Setback Polygons' : defQ_prjnum
    }
    
    prj_info['defQs'] = defQs
    #return defQs


# In[18]:


# Get extent of project 

def Get_Project_Extent(prj_num, params_df, prj_info):
    import arcpy
    # Get SQL geometry as string and the SRID of the project polygon
    sql_cmd = """SELECT SHAPE.STEnvelope().ToString(), SHAPE.STSrid FROM Apex_ProjectData.dbo.Apex_Project_Boundary
                    WHERE TYPE = 'Primary' AND Project_NUM = """ + str(prj_num)

    with engine.begin() as connection:
        poly, srid = connection.execute(sql_cmd).fetchall() [0]
    
    # Extract the corners of the bounding envelope from the polygon string
    coor_strings = poly[poly.find('-'):-2].split(', ')
    eastings = list(set([float(x.split(' ')[0]) for x in coor_strings]))
    northings = list(set([float(x.split(' ')[1]) for x in coor_strings]))
    
    # Get min and max coordinates
    x_max = max(eastings)
    x_min = min(eastings)
    y_max = max(northings)
    y_min = min(northings)
    
    # Store and return values in dict formatted for the arcpy extent object function
        # Not used currently but might need coordinates and SR object in future
    extent_dict = { 'XMin' : x_min
                   ,'YMin' : y_min
                   ,'XMax' : x_max
                   ,'YMax' : y_max
                   ,'ZMin' : None
                   ,'ZMax' : None
                   ,'MMin' : None
                   ,'MMax' : None
                   ,'spatial_reference' : srid}  
    
    # Generate arcpy extent object
    proj_extent = arcpy.Extent(x_min, y_min, x_max, y_max) #, spatial_reference=arcpy.SpatialReference(srid))
    
    # Add extent to parameters dataframe
    prj_info['proj_extent'] = proj_extent
    for index, row in params_df.loc[(params_df['extent_obj'].isin(['project']))].iterrows():
        params_df.loc[index, 'extent_obj'] = proj_extent 

    #return extent_dict
    #return proj_extent


# In[19]:


# Get extent of TROW leases and/or gentie line
def Get_Gentie_Extent(prj_info, params_df):
    gentie_fc = r'\\ace-ra-fs1\data\GIS\_Dev\software\esri\_Latest\SDE_Cnxns\Apex_ProjectData.sde\Apex_ProjectData.dbo.APEX_INTERCONNECT_GENTIE'

    sql_cl = (None, 'ORDER BY ProjectCode, Line_Priority')

    try:
        extent_gentie = [r[0].extent for r in arcpy.da.SearchCursor(gentie_fc
                                                                    , 'SHAPE@'
                                                                    , "ProjectCode ='" + prj_info['prj_num'] + "'" 
                                                                    ,sql_clause=sql_cl)][0]
        
    except IndexError:
        print('No Gentie line found. Project extent will be used')
        extent_gentie = prj_info['proj_extent']

    # Add extent to parameters dataframe
    for index, row in params_df.loc[(params_df['extent_obj'] == 'gentie_line')].iterrows():
        params_df.loc[index, 'extent_obj'] = extent_gentie
        
    #print('Gentie Extent:', extent_gentie.JSON)
    
    



# In[20]:


# Get extent of State in which the project is located
def Get_State_Extent(prj_info, params_df):
    # Get extent of project state from queried state featureclass
    state_fc = r'\\ace-ra-fs1\data\GIS\_Dev\software\esri\_Latest\SDE_Cnxns\National_Static.sde\GIS_National_StaticData.DBO.AdminBoundaries_States_ventyx'
    state_extent = [r[0].extent for r in arcpy.da.SearchCursor(state_fc, 'SHAPE@', "ABBREV = '" + prj_info['prj_st'] + "'" )][0]
    
    # Add extent to parameters dataframe
    for index, row in params_df.loc[(params_df['extent_obj'] == 'state')].iterrows():
        params_df.loc[index, 'extent_obj'] = state_extent


# In[21]:


# Determines map scale
def Determine_Map_Scale(params_df, prj_extent):
    print("Determining Scales")
    
    # Get arcpy layout object from layout params and layout_aprx dictionaries
    lyt = params_df.loc['Lease Status']['layout_aprx']   
    
    # Find Mapframe
    mapframe = lyt.listElements("MAPFRAME_ELEMENT", "Layers Map Frame")[0]  
    
    print('\tExisting Extent', mapframe.camera.getExtent().JSON)
    
    # Apply extent to map frame
    mapframe.camera.setExtent(prj_extent)
    print('\Project Extent', prj_extent.JSON)

    # Increase scale by 10% to increase context around boundary
    scale_in = mapframe.camera.scale
    scale_out = round(scale_in / 10000.0) * 1.1 * 10000.0
    print('\tExisting Scale', mapframe.camera.scale)
    
    # Add scale to parameters dataframe
    for index, row in params_df.loc[(params_df['scale_param']=='')].iterrows():
        print ('\t', index, scale_out)
        params_df.loc[index, 'scale_param'] = scale_out
    
    
    
# Determines map scale
def Determine_Map_Scales(params_df): 
    for index, row in params_df[params_df['extent_obj'].isin(params_df['extent_obj'].unique())].iterrows():
        extent = row['extent_obj']
        layout = row['layout_aprx'] 
        mapframe_name = row['mapframeScalesToChange']
        scale_Adj = row['Scale_Adj']
        
        # Find Mapframe
        mapframe = layout.listElements("MAPFRAME_ELEMENT", mapframe_name)[0]  
        
        # Apply extent to map frame
        mapframe.camera.setExtent(extent)
        
        # Increase scale by pre-configured adjustment value to include context around the map
        scale_in = mapframe.camera.scale
        scale_out = round(scale_in / 10000.0) * scale_Adj * 10000.0
        if scale_out == 0:
            scale_out = 10000
        
        # Add scale to parameters dataframe
        for index, row in params_df.loc[(params_df['extent_obj']==extent)].iterrows():
            params_df.loc[index, 'scale_param'] = scale_out
        
        

    


# In[ ]:





# In[22]:


#Set_Scale(params_df, prj_info)


# In[23]:


# Set layer definition queries

def Set_DefQs(params_df, prj_info, aprx):
    defQs = prj_info['defQs']
    # Get maps in template whose names are in the parameter dictionary
    maps = [m for m in aprx.listMaps("*") if m.name in params_df['layout_param'].unique()]

    # Iterate through maps
    for m in maps:
        
        # Iterate through layers in map
        for lyr in m.listLayers():            
        
            # Set definition queries if one is included in the
            if lyr.name in defQs.keys():
                defQ_in = lyr.definitionQuery  # for debugging
                lyr.definitionQuery = defQs[lyr.name]
#                if debug:
#                    print(lyr.name)
#                    print('\t', 'existing defQ', '\t', defQ_in)
#                    print('\t', 'new defQ', '\t', lyr.definitionQuery, '\n', )
#            else:
#                if debug:
#                    print(lyr.name)
#                    print('\tno defQ assigned', '\n')
        


# In[24]:


# Set Overview map properties
def Config_Overview(params_df, prj_info, aprx): 
    map_obj = [m for m in aprx.listMaps("Overview")][0]
    layout = params_df.loc['BizDev']['layout_aprx']
    
    # Set definition queries
    for lyr in map_obj.listLayers():
        if lyr.name == 'County':
            lyr.definitionQuery = "COUNTY = '" + prj_info['prj_co'] + "' And ST_ABBR = '" + prj_info['prj_st'] + "'"
        if lyr.name == 'State':
            lyr.definitionQuery = "ABBREV = '" + prj_info['prj_st'] + "'" 
    
    # Set Mapframe extent
        # Get extent of project state from queried state featureclass
    state_fc = r'\\ace-ra-fs1\data\GIS\_Dev\software\esri\_Latest\SDE_Cnxns\National_Static.sde\GIS_National_StaticData.DBO.AdminBoundaries_States_ventyx'
    state_extent = [r[0].extent for r in arcpy.da.SearchCursor(state_fc, 'SHAPE@', "ABBREV = '" + prj_info['prj_st'] + "'" )][0]
    
    # Find Mapframe
    mapframe = layout.listElements("MAPFRAME_ELEMENT", "Overview Map Frame")[0]
    # Apply extent to map frame
    mapframe.camera.setExtent(state_extent)
    
        


# In[ ]:





# In[ ]:





# In[25]:


# # # Create directory to hold outputs
def Cr8_Output_Dir(prj_info):
    base = r'\\ace-ra-fs1\data\GIS\_Projects'
    
    prj_alpha = prj_info['prj_alpha']
    prj_name  = prj_info['prj_name']
    
    if debug:
        base = r'\\ace-ra-fs1\data\GIS\_Dev\ScratchDirectoryForDevTools\Stagegate\Outputs'
     
    # Find Project Folder
    try:
        prj_dir_name = [f for f in os.listdir(base) if f[:3] == prj_alpha][0]
    except IndexError:
        prj_dir_name = prj_alpha + '_'+ prj_name.replace(' ', '')

    # Create full path with all intermediary directories
    output_dir = os.path.join(base, prj_dir_name, 'Outputs', 'Project', 'Stagegate')

    # Create output and any intermediary dirs if they don't already exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Created output directory")

    # Return full path of directory
    return output_dir


# In[26]:


# # # Create paths for master and intermediate PDF files:
def Generate_PDF_Paths(prj_info, params_df):
    prj_layout = prj_info['prj_layout']
    prj_num    = prj_info['prj_num']
    prj_alpha  = prj_info['prj_alpha']
    output_dir = prj_info['output_dir']

    date_str = datetime.datetime.today().strftime('%Y%m%d_%H%M')

    # Determine name that all PDF files will be based
    pdf_name_base = prj_alpha + "_" + prj_num + "_" + prj_layout + "_Stagegate_" + date_str + "_Mapbook.pdf"
    

    # Determine full file path of master pdf from which all temp pdfs will be derived
    pdf_master = os.path.join(output_dir, pdf_name_base)
    prj_info['master_pdf_path'] = pdf_master   
    
    # Determine name of temp pdfs and add them to the parameters dataframe
    params_df['map_path'] = ''
    for map_name_raw in params_df['map']:
        map_name = map_name_raw.replace(' ', '_')
        pdf_temp = pdf_master.replace("Mapbook.pdf", map_name + '.pdf')
        params_df.loc[map_name_raw, 'map_path'] = pdf_temp
        

    # Remove temp pdf file if it already exists
    for pdf_path in list(params_df['map']) + [pdf_master]:
        if os.path.exists(pdf_path):
            print(pdf_path, 'exists so it is being deleted')
            os.path.remove(pdf_path)
            
    # Create arcpy PDF document object for master pdf
    pdf_master_arcpy = arcpy.mp.PDFDocumentCreate(prj_info['master_pdf_path'])
    prj_info['pdf_master_arcpy'] = pdf_master_arcpy

    print(datetime.datetime.today().strftime('%H:%M:%S'), "Created intermediate PDF paths")


# In[ ]:





# In[ ]:





# In[ ]:





# In[27]:


# Main project preproccesing function

def Preproccess_Project(prj_info, params_df):
    Get_DefQs(prj_info)
    
    # Determine Various Extents
    Get_Project_Extent(prj_info['prj_num'], params_df, prj_info)
    Get_State_Extent(prj_info, params_df)
    Get_Gentie_Extent(prj_info, params_df)   
    
    # Determine map scales based on project extent
    Determine_Map_Scales(params_df) 
    
    # Set definition queries of relevent layers in aprx maps
    Set_DefQs(params_df, prj_info, aprx)
    
    # Set scales of relevent mapframes in aprx layouts
    #Set_Scale(params_df, prj_info)
    
    # Configure overview map in Bizdev layout
    #Config_Overview(params_df, prj_info, aprx)
    
    # Determine and create output directory
    prj_info['output_dir'] = Cr8_Output_Dir(prj_info)

    # Determine PDF Paths
    Generate_PDF_Paths(prj_info, params_df)    
    
    display(params_df)
    display(prj_info)
    
    return(params_df, prj_info)


# # Automate Maps
#    

# In[28]:


# # # Create master PDF
def Create_Master_PDF(pdf_out_path):

    pdfDoc = arcpy.mp.PDFDocumentCreate(pdf_out_path)
    
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Created output PDF arcpy object")
    
    return pdfDoc


# In[29]:


# Sets scale of all mapframes
def Set_Scale(param_row):
    print('Setting scales')
    
    layout = param_row['layout_aprx']
    extent = param_row['extent_obj']
    scale_out = param_row['scale_param']
    mapframe_name = param_row['mapframeScalesToChange']

    # Find Mapframe
    mapframe = layout.listElements("MAPFRAME_ELEMENT", mapframe_name)[0]   

    # Apply extent to map frame
    mapframe.camera.setExtent(extent)

    # Set mapframe Extent
    mapframe.camera.scale = scale_out

    #aprx.save()

        
        


# In[30]:


# Set layer visibilities
def Toggle_Layers(param_row):
    map_obj = [m for m in aprx.listMaps("*") if m.name == param_row['layout_param']][0]
    
    # Iterate through layers in map
    for lyr in map_obj.listLayers():            
        
        # Set definition queries if one is included in the
        if lyr.name in param_row['layer_viz']:
            lyr.visible = True
            
        else:
            lyr.visible = False

    
    


# In[31]:


# Set map layout elements
def Config_Elements(params_row):
    title_text = params_row['map']    
    


# In[32]:


# Open directory of given file in windows explorer
def Open_Dir(file_path):
    import subprocess
    
    subprocess.Popen(r'explorer /select,' + file_path)
    


# In[33]:


# Export PDF map page
def Export_PDF_Map_Page(params_row, prj_info):
    map_path         = params_row['map_path'] 
    layout_aprx      = params_row['layout_aprx']
    pdf_master_arcpy = prj_info['pdf_master_arcpy']
    
    print('exporting', map_path)
    
    # Export map layout to temporary pdf
    layout_aprx.exportToPDF(map_path)
    
    print('finished export')
    
    # Append temporary PDF (an individual page) into master PDF document
    pdf_master_arcpy.appendPages(map_path)
    
    # Delete individual page pdf file
    if not debug:
        os.remove(map_path)
    # Else open folder
    else:
        #Open_Dir(map_path)
        pass
    
    
    
    
    
    
    


# In[34]:


# Set Title Text
def Set_Title(params_row, prj_info):
    if params_row['map'] == 'BizDev':
        return
    
    map_name   = params_row['map']     
    layout_aprx= params_row['layout_aprx']
    layout_num = prj_info['prj_layout']
    prj_name   = prj_info['prj_name']
    wtg_model  = prj_info['prj_wtgModel']
    prj_hh     = prj_info['prj_hh']
    cap_MW     = prj_info['prj_cap']
    
    title_Text  = prj_name[:20] + '\n' + map_name 
    print(title_Text)
    
    title_obj = layout_aprx.listElements("TEXT_ELEMENT", "title")[0]
    title_obj.text = title_Text
    
 


# In[ ]:





# In[35]:


# Set Layout info table values
def Set_Summary_Text(params_row, prj_info):
    if params_row['map'] == 'BizDev':
        return
    
    layout_aprx = params_row['layout_aprx']
    
    match_dict = {'cap_MW' : prj_info['prj_cap'] + ' MW'
                 ,'layout' : prj_info['prj_layout']
                 ,'wtg'    : prj_info['prj_wtgModel']
                 ,'hh'     : prj_info['prj_hh'] + ' m'}
    for text_obj_name, value in match_dict.items():   
        #print(text_obj_name, value)
        text_obj = layout_aprx.listElements("TEXT_ELEMENT", text_obj_name)[0]
        text_obj.text = value
        
        


# In[36]:


def Map_Automation(params_df, prj_info, aprx):
    for map_name, params_row in params_df.iterrows():
        print('\nworking on ', map_name)
        if map_name in ('BizDev'):#, 'TROW'):
            #Export_PDF_Map_Page(params_row, prj_info)
            continue
            
        # Set map camera extent and scale
        Set_Scale(params_row)
            
        # Toggle layer visibility as defined in the parameters dataframe
        Toggle_Layers(params_row)
        
        # Set Title Text
        Set_Title(params_row, prj_info)
        
        # Set Project summary table values
        Set_Summary_Text(params_row, prj_info)  
        
        # Export map pdf
        Export_PDF_Map_Page(params_row, prj_info)
        
        aprx.save()

    
    prj_info['pdf_master_arcpy'].saveAndClose()
    Open_Dir(prj_info['master_pdf_path']) 
    
    # Save and close arcGIS Pro project file
    aprx.save()
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Performed script cleanup")
    #del aprx    
    
    return prj_info['master_pdf_path']
    
    
        


# In[ ]:





# In[ ]:





# # Post Processing
#     
#     
#     
#     
#     
#     
#     
#     
#     

# In[37]:


# # # Update Tool Requests SQL table
def Update_Tracker(dict_defQs, complete_val, output_pdf_path):
    req_tbl = r'\\ace-ra-fs1\data\GIS\_Dev\software\esri\_Latest\SDE_CnxnsApex_ProjectData.sde'

    whereCl = dict_defQs['cursor']
    fields_cursor = ['Complete', 'Output_Path']
    
    print (whereCl)
    with arcpy.da.UpdateCursor(req_tbl, fields_cursor, whereCl) as uCur:
        for row in uCur:
            #print(row)
            row = [complete_val, output_pdf_path]
            #print(row)
            uCur.updateRow(row)

    print(datetime.datetime.today().strftime('%H:%M:%S'), "Updated Tool Requests table")


# In[38]:


# # # Update Tool Requests SQL table
def Update_Tracker(dict_defQs, complete_val, output_pdf_path):
    whereCl = dict_defQs['cursor']
    
    sql_cmd = "UPDATE [Apex_ProjectData].[dbo].[TOOL_REQUESTS] SET COMPLETE = " + str(complete_val) + """
                    , Output_Path = '""" + output_pdf_path + """'
                WHERE """ + whereCl 
    
    #print(sql_cmd)

    with engine.begin() as connection:
        result = connection.execute(sql_cmd)
    
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Updated Tool Requests table")


# In[39]:


# # # Function returns maximum objectID from log table
def Max_OID():

    engine = create_engine('mssql+pyodbc://@ace-ra-sql1/Apex_ProjectData?driver=ODBC+Driver+11+for+SQL+Server')

    sql_cmd = "SELECT max(objectid) from [GIS_Logging].[dbo].[ApexGIS_Script_Log]"

    # Execute command to return list of unprocessed requests
    with engine.begin() as connection:
        result = connection.execute(sql_cmd).first()

    return result[0]


# In[40]:


# Function logs script completion status to SQL
def Log_to_GIS_Table(status='1', params=[]):
    from sqlalchemy import create_engine  
    import sys
    
    oid = "'" + str(Max_OID() +1) + "'"
    
    status = str(status)
    try:
        process = __file__
    except NameError:
        process = 'Stagegate'
    try:
        if debug:
            debug_log = '1'
        else:
            debug_log = '0'
    except NameError:
        debug_log = "'None'"
    
    #if len(params) == 0 :
    #    params = str(sys.argv[1:])[:500].replace("'", '')
    params = str(params).replace("'", '')
    
    version = sys.version   
    notes = "'None'"
    log_time = 'CURRENT_TIMESTAMP'
    exec_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    print(type(exec_time), exec_time)
    print(type(debug_log), debug_log)
    print(type(version), version)
    print(type(params), params)
    print(type(process), process)
    print(type(oid), oid)

    

    engine = create_engine('mssql+pyodbc://@ace-ra-sql1/Apex_ProjectData?driver=ODBC+Driver+11+for+SQL+Server')
    
    sql_cmd = """INSERT INTO [GIS_Logging].[dbo].[ApexGIS_Script_Log]
                (  [Process]
                  ,[Debug]
                  ,[Status]
                  ,[Parameters]
                  ,[Notes]
                  ,[Version]
                  ,[Execution_Time]
                  ,[Log_Time]
                  ,ObjectID
                  )
                VALUES
                    ('""" + process + """'
                    ,""" + str(debug_log) + """                
                    ,'""" + status + """'
                    ,'""" + params + """'
                    ,""" + str(notes) + """
                    ,'""" + version + """'
                    ,'""" + exec_time + """'
                    ,CURRENT_TIMESTAMP
                    ,""" + str(oid) + ");"
    
    print(sql_cmd)
    with engine.begin() as connection:
        result = connection.execute(sql_cmd)
        
    #print(result)


# In[41]:


# # # Send email to user once process is complete

# function replaces spaces with underscores
    # # # NOTE: You likely want to set the other script up to reverse this
def Replace_Spaces(vals):
    out = []
    for val in vals:
        if val == None:
            out.append(val)
            continue
            
        out.append(val.replace(' ', '_'))
    
    return out


# In[42]:


def Email_Requestor(username, portal_url, prj_name):
    from email.message import EmailMessage
    from email.utils import make_msgid
    import mimetypes
    from smtplib import SMTP
    
    # Set email attributes (change to parameters in future?)
    mail_server = 'apexcleanenergy-com.mail.protection.outlook.com'
    sender = 'Alerts.ApexGIS@apexcleanenergy.com'
    receivers = [username + '@apexcleanenergy.com' ]#, 'john.foster@apexcleanenergy.com'] 
    
    # Configure email body elements
    msg = EmailMessage()
    msg['Subject'] = "Stagegate Mapset: " + prj_name
    msg['From'] = sender
    msg['To'] = ", ".join(receivers)
    
    msg.set_content("Find the completed Stagegate mapset at the following link: " + portal_url)
    
    # Create and connect to email server object to send email
    mailserver = SMTP(mail_server, 25)

    # Shot in the dark
    mailserver.ehlo()
    mailserver.starttls()

    ##mailserver.login(sender, sender_pw)
    mailserver.sendmail(sender, 'daniel.germroth@apexcleanenergy.com', msg.as_string())
    mailserver.quit()


# In[43]:


# # # Upload pdf mapset to portal
def UploadtoPortal(prj_info, request_row):
    import Generate_Portal_Token
    
    
    pdf_master_path = prj_info['master_pdf_path']
    prj_name        = prj_info['prj_name']
    wind_or_solar   = request_row['WindSolar']
    prj_layout      = prj_info['prj_layout']
    prj_num         = prj_info['prj_num']
    prj_alpha       = prj_info['prj_alpha']    
    
    
    gis = GIS(r'https://gis.apexcleanenergy.com/portal', token=Generate_Portal_Token.main())
    
    pdf_properties = {
        'title': prj_name + ' Stagegate Mapset'
        ,'description' : "Stagegate mapset for " + prj_name + " depicting layout: " + prj_layout 
        ,'tags': ('stagegate', 'mapset', prj_name, prj_alpha, prj_num, prj_layout)
        ,'type': 'PDF'
    }
    
    # Get Apex Employees portal group
    grp_emply = gis.groups.search('title:Apex Employees', max_groups=15)[0]
    print(grp_emply.id)
    
    # Get Stagegate portal group
    grp_sg = gis.groups.search('title:Stagegate Maps', max_groups=15)[0]
    print(grp_sg.id)
    
    pdf_portal_item = gis.content.add(pdf_properties, data=pdf_master_path, folder="Stagegate_PDFs")
    
    # Share PDF item with employees group
    print(pdf_portal_item.shared_with) 
    pdf_portal_item.share(groups=[grp_emply, grp_sg])
    print(pdf_portal_item.shared_with) 
    
    # Categorize item
    # get the CategoryManager for this GIS
    cs=gis.admin.category_schema
    cs.categorize_item(pdf_portal_item,['/Categories/Apex Project Data (Apex)/Stagegate'])
    
    portal_id = pdf_portal_item.id
    
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Uploaded PDF to Portal:", portal_id)
    
    return portal_id


# In[44]:


# # # Update project featureclass with portal ID
def Update_Prj_SDE(portal_id, dict_defQs):
    prj_tbl = os.path.join(working_gdb, 'Apex_ProjectData.DBO.Apex_Project_Boundary')
    whereCl = dict_defQs['Project Boundary']
    fields_cursor = ['Stagegate_Map']
    
    out_val = "https://gis.apexcleanenergy.com/portal/home/item.html?id=" + portal_id
    
    # Open an edit session
    edit = arcpy.da.Editor( working_gdb )

    edit.startEditing(False, True)
    edit.startOperation()
    
    with arcpy.da.UpdateCursor(prj_tbl, fields_cursor, whereCl) as uCur:
        for row in uCur:
            #print(row)
            row = [portal_id]
            uCur.updateRow(row)
            
    # Stop the edit operation.
    edit.stopOperation()

    # Stop the edit session and save the changes
    edit.stopEditing(True)

            
    print(datetime.datetime.today().strftime('%H:%M:%S'), "Added pdf mapset URL to master Projects featureclass")
    
    return out_val


# In[45]:


def main(request_row): 
    # Get requests from Tool Requests SQL table
    #request_rows_raw = Get_Requests()   
    
    ##Add Wind/Solar Conditional here...as function that assigns map configs based on project type
    
    #request_rows = [r for r in request_rows_raw if r['WindSolar'] == 'Wind']
    #display(request_rows)
    request_rows = [request_row]
    
    #return
    
    # Iterate through all requests
    for request_row in request_rows:
        #if request_row['Project'] != 'Emerson Creek':
        #    continue
        print("#################### Starting ", request_row)
    
        # Gather Project info    
        prj_info = Gather_Project_info(request_row)
        #Get_BA_Layout(prj_info)

        # Preprocess mapping objects
        params_df, prj_info = Preproccess_Project(prj_info, params_df_raw)

        # Automate Maps
        Map_Automation(params_df, prj_info, aprx)
        
        # Update tool requests table
        #Update_Tracker(prj_info['defQs'], 1, prj_info['master_pdf_path'])
        
        if not debug:
            # Upload pdf mapset to portal
            portal_id = UploadtoPortal(prj_info, request_row)

            # Add hyperlink to portal item to project featureclass
            portal_url = Update_Prj_SDE(portal_id, prj_info['defQs'])

            # Email initial requestor of tool
            if email_requestor == True:
                Email_Requestor(request_row['created_user'], portal_url, prj_info['prj_name'])
        
        #break
        
    


# In[46]:


main(request_row)


# In[ ]:





# In[47]:


params_df_raw['layer_viz'][1]


# In[ ]:





# In[ ]:





# In[ ]:





# In[48]:


#Get_Requests()


# In[ ]:




