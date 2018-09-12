import os
import arcpy
import smtplib
import string
import traceback
import datetime
import StringIO




arcpy.env.workspace = r"Database Connections\SDE@Planning_CLUSTER.sde"
arcpy.env.overwriteOutput = True
db_conn = r"Database Connections\SDE@Planning_CLUSTER.sde"


#   data
ComPlus_BusiLic = r"Database Connections\COMMPLUS.sde\COMPLUS.WPB_ALL_BUSINESSLICENSES"
Planning_AlcoLic = r"Planning.SDE.WPB_GIS_ALCOHOL_LICENSES"
Fields = [Field.baseName.encode('ascii') for Field in arcpy.ListFields(Planning_AlcoLic)]
Fields_lessObjID = Fields[1:]
query_layer = "ALCOLICENCE_QL"
alco_licence_poly = "Alco_licence_poly"
alco_license_points = "Alco_license_points"
alco_license ="AlcoholLicense_complus"
spatialref = arcpy.Describe(r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.LandUsePlanning").spatialReference.exportToString()
TempTable = r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.TempTable"
Sql_copytable = "CATEGORY IN  ('AAM','445310','424810','312130','424820','445310','722410','312120','312140') AND STAT IN ('ACTIVE','PRINTED','HOLD')"


try:
    arcpy.AcceptConnections(db_conn,True)
    arcpy.DisconnectUser(db_conn,'ALL')
    arcpy.AcceptConnections(db_conn,False)

    # create lists of license numbers from Community Plus and corresponding table in GIS Cluster
    ComplusLicenses = []
    PlanningLicenses = []


    with arcpy.da.SearchCursor(ComPlus_BusiLic,'LICENSE',Sql_copytable) as ComplusUC:
        for record in ComplusUC:
            ComplusLicenses.append(record)

    with arcpy.da.SearchCursor(Planning_AlcoLic,'LICENSE') as PlanAlcUC:
        for record in PlanAlcUC:
            PlanningLicenses.append(record)

    #   Compare the two lists, write to Delta which licenses are in complus but not in planning

    delta = ['0',]

    for record in ComplusLicenses:
        if not record in PlanningLicenses:
            delta.append(record)

    #   If there is at least 2 record, then proceed to append record to GIS_ALCOHOL_LICENSES, and create a point fc of reccord and append to AlocholLicense_complus

    if len(delta) == 1:
        with open(r"C:\Users\jsawyer\Desktop\Tickets\alcohol permits\logfile.txt","a") as log:
            now = datetime.datetime.now().strftime("%Y-%d-%m")
            log.write("\n-----------------\n")
            log.write(now + " no new alcohol licenses found\n\n")

    else:
        #   change list to a tuple (in prepartation of creating a text string for the query). Delta is in unicode, need it in plain ascii text for query

        delta = [x[0].encode('ascii') for x in delta] # list comprehension to reformat to ascii
        delta_tup = tuple(delta)


        #   save the query as a string

        sqlquery = "LICENSE IN {}".format(delta_tup)

        #   it doesnt like insert cursor, so make a temp table and append to that, then append to GIS_ALCOHOL_LICENSES

        arcpy.CreateTable_management(db_conn,"TempTable",Planning_AlcoLic)

        with arcpy.da.SearchCursor(ComPlus_BusiLic,Fields_lessObjID,sqlquery) as sc:
            with arcpy.da.InsertCursor(TempTable,Fields_lessObjID) as ic:
                for record in sc:
                    ic.insertRow(record)

        arcpy.Append_management(TempTable,Planning_AlcoLic)

        #   THe following block creates a Query Layer from a join between the new licenses identified earlier and the parcels in which they reside, saves the Query layer as a polygon fc...
        #   changes that to point fc, then appends the points to AlcoholLicense_complus

        sql = "SELECT PARCELS.[OBJECTID],[OWNPARCELID] AS PARCELS_PCN,[SRCREF],[OWNTYPE],[GISdata_GISADMIN_OwnerParcel_AR],[LASTUPDATE],[LASTEDITOR],[Shape],[PARCEL_ID] AS COMPLUS_PCN,[BUSINESS_ID],[LICENSE],[CATEGORY],[CATEGORY_DESC],[STAT],[ISSUE],[EXPIRATION],[BUS_ENTITY_ID],[BUS_NAME],[BUS_PROD],[SERVICE],[ADRS1],[BUS_PHONE],[BUS_EMAIL] FROM [Planning].[sde].[PLANNINGPARCELS] PARCELS,[Planning].[sde].[WPB_GIS_ALCOHOL_LICENSES] ALCOLIC WHERE PARCELS.OWNPARCELID = ALCOLIC.PARCEL_ID AND {}".format(sqlquery)

        arcpy.MakeQueryLayer_management(input_database=db_conn, out_layer_name=query_layer, query=sql, oid_fields="OBJECTID", shape_type="POLYGON", srid="2881", spatial_reference=spatialref)
        arcpy.management.CopyFeatures(query_layer, alco_licence_poly, None, None, None, None)
        arcpy.FeatureToPoint_management(alco_licence_poly,alco_license_points,"INSIDE")
        arcpy.Append_management(alco_license_points,alco_license)


        #   Create the alert email text. Uses StringIO to create a string treated as a file for formatting purposes

        TT_fieldnames =['PARCEL_ID','LICENSE','BUS_NAME','ADRS1']
        string_obj = StringIO.StringIO()
        with arcpy.da.SearchCursor(TempTable,TT_fieldnames) as TTSC:
            for row in TTSC:
                string_obj.write(''.join(row))
                string_obj.write('\n')

        report = string_obj.getvalue()

        today =  datetime.datetime.now().strftime("%d-%m-%Y")
        subject = 'Alcohol License report ' +  today
        sendto = ["cdglass@wpb.org","jssawyer@wpb.org"]
        sender = 'scriptmonitorwpb@gmail.com'
        sender_pw = "Bibby1997"
        server = 'smtp.gmail.com'
        body_text = "From: {0}\r\nTo: {1}\r\nSubject: {2}\r\nHere is a list of the new licenses.\nThese have been added to AlcoholLicense_complus:\n\nPCN\t\tLicense Number\tBusiness Name\tAddress\n\n{3}".format(sender, sendto, subject,report)


        gmail = smtplib.SMTP(server, 587)
        gmail.starttls()
        gmail.login(sender,sender_pw)
        gmail.sendmail(sender,sendto,body_text)
        gmail.quit()

        with open(r"C:\Users\jsawyer\Desktop\Tickets\alcohol permits\logfile.txt","a") as log:
            now = datetime.datetime.now().strftime("%Y-%d-%m")
            log.write("\n------------------------------------------\n\n")
            log.write(now)
            log.write('\n')
            log.write(report)
            log.write("\n")



        del_list = (TempTable,alco_licence_poly,alco_license_points)
        for fc in del_list:
            arcpy.Delete_management(fc)


    arcpy.AcceptConnections(db_conn,True)

except Exception as E:
    arcpy.AcceptConnections(db_conn,True)

    today =  datetime.datetime.now().strftime("%Y-%d-%m")
    subject = 'Alcohol License script failure report ' +  today
    sendto = "jssawyer@wpb.org" # ,'JJudge@wpb.org','NKerr@wpb.org'
    sender = 'scriptmonitorwpb@gmail.com'
    sender_pw = "Bibby1997"
    server = 'smtp.gmail.com'
    log = traceback.format_exc()
    body_text = "From: {0}\r\nTo: {1}\r\nSubject: {2}\r\nAn error occured. Here are the Type, arguements, and log of the error\n\n{3}\n{4}\n{5}".format(sender, sendto, subject,type(E).__name__, E.args, log)

    gmail = smtplib.SMTP(server, 587)
    gmail.starttls()
    gmail.login(sender,sender_pw)
    gmail.sendmail(sender,sendto,body_text)
    gmail.quit()

    print body_text
